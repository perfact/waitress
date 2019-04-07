##############################################################################
#
# Copyright (c) 2001, 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
from collections import deque
import socket
import threading
import time
import traceback

from waitress.buffers import (
    BytesIOBasedBuffer,
    OverflowableBuffer,
    ReadOnlyFileBasedBuffer,
)

from waitress.parser import HTTPRequestParser

from waitress.task import (
    ErrorTask,
    WSGITask,
)

from waitress.utilities import InternalServerError

from . import wasyncore

class ClientDisconnected(Exception):
    """ Raised when attempting to write to a closed socket."""

class HTTPChannel(wasyncore.dispatcher, object):
    """
    Setting self.requests = [somerequest] prevents more requests from being
    received until the out buffers have been flushed.

    Setting self.requests = [] allows more requests to be received.
    """

    task_class = WSGITask
    error_task_class = ErrorTask
    parser_class = HTTPRequestParser

    request = None               # A request parser instance
    last_activity = 0            # Time of last activity
    will_close = False           # set to True to close the socket.
    close_when_flushed = False   # set to True to close the socket when flushed
    requests = ()                # currently pending requests
    sent_continue = False        # used as a latch after sending 100 continue
    known_outbufs_len = 0        # total known bytes ready to send
    has_unseekable_outbufs = False # any unseekable data to send
    has_outbuf_data = False      # any data to write including unseekable
    current_outbuf_count = 0     # total bytes written to current outbuf

    #
    # ASYNCHRONOUS METHODS (including __init__)
    #

    def __init__(
            self,
            server,
            sock,
            addr,
            adj,
            map=None,
            ):
        self.server = server
        self.adj = adj
        self.outbufs = deque()
        self.outbufs.append(OverflowableBuffer(adj.outbuf_overflow))
        self.creation_time = self.last_activity = time.time()
        self.sendbuf_len = sock.getsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF)

        # task_lock used to push/pop requests
        self.task_lock = threading.Lock()
        # outbuf_lock used to access any outbuf (expected to use an RLock)
        self.outbuf_lock = threading.Condition()

        wasyncore.dispatcher.__init__(self, sock, map=map)

        # Don't let wasyncore.dispatcher throttle self.addr on us.
        self.addr = addr

    def writable(self):
        # if there's data in the out buffer or we've been instructed to close
        # the channel (possibly by our server maintenance logic), run
        # handle_write
        return self.has_outbuf_data or self.will_close

    def handle_write(self):
        # Precondition: there's data in the out buffer to be sent, or
        # there's a pending will_close request
        if not self.connected:
            # we dont want to close the channel twice
            return

        # try to flush any pending output
        if not self.requests:
            # 1. There are no running tasks, so we don't need to try to lock
            #    the outbuf before sending
            # 2. The data in the out buffer should be sent as soon as possible
            #    because it's either data left over from task output
            #    or a 100 Continue line sent within "received".
            flush = self._flush_some
        elif (
            self.known_outbufs_len >= self.adj.send_bytes
            or self.has_unseekable_outbufs
        ):
            # 1. There's a running task, so we need to try to lock
            #    the outbuf before sending
            # 2. Only try to send if the data in the out buffer is larger
            #    than self.adj_bytes to avoid TCP fragmentation
            flush = self._flush_some_if_lockable
        else:
            # 1. There's not enough data in the out buffer to bother to send
            #    right now.
            flush = None

        if flush:
            try:
                flush()
            except socket.error:
                if self.adj.log_socket_errors:
                    self.logger.exception('Socket error')
                self.will_close = True
            except Exception:
                self.logger.exception('Unexpected exception when flushing')
                self.will_close = True

        if self.close_when_flushed and not self.has_outbuf_data:
            self.close_when_flushed = False
            self.will_close = True

        if self.will_close:
            self.handle_close()

    def readable(self):
        # We might want to create a new task.  We can only do this if:
        # 1. We're not already about to close the connection.
        # 2. There's no already currently running task(s).
        # 3. There's no data in the output buffer that needs to be sent
        #    before we potentially create a new task.
        return not (self.will_close or self.requests or self.has_outbuf_data)

    def handle_read(self):
        try:
            data = self.recv(self.adj.recv_bytes)
        except socket.error:
            if self.adj.log_socket_errors:
                self.logger.exception('Socket error')
            self.handle_close()
            return
        if data:
            self.last_activity = time.time()
            self.received(data)

    def received(self, data):
        """
        Receives input asynchronously and assigns one or more requests to the
        channel.
        """
        # Preconditions: there's no task(s) already running
        request = self.request
        requests = []

        if not data:
            return False

        while data:
            if request is None:
                request = self.parser_class(self.adj)
            n = request.received(data)
            if request.expect_continue and request.headers_finished:
                # guaranteed by parser to be a 1.1 request
                request.expect_continue = False
                if not self.sent_continue:
                    # there's no current task, so we don't need to try to
                    # lock the outbuf to append to it.
                    outbuf_payload = b'HTTP/1.1 100 Continue\r\n\r\n'
                    self.outbufs[-1].append(outbuf_payload)
                    self.current_outbuf_count += len(outbuf_payload)
                    self.known_outbufs_len += len(outbuf_payload)
                    self.has_outbuf_data = True
                    self.sent_continue = True
                    self._flush_some()
                    request.completed = False
            if request.completed:
                # The request (with the body) is ready to use.
                self.request = None
                if not request.empty:
                    requests.append(request)
                request = None
            else:
                self.request = request
            if n >= len(data):
                break
            data = data[n:]

        if requests:
            self.requests = requests
            self.server.add_task(self)

        return True

    def _flush_some_if_lockable(self):
        # Since our task may be appending to the outbuf, we try to acquire
        # the lock, but we don't block if we can't.
        if self.outbuf_lock.acquire(False):
            try:
                self._flush_some()

                if self.known_outbufs_len < self.adj.outbuf_high_watermark:
                    self.outbuf_lock.notify()
            finally:
                self.outbuf_lock.release()

    def _flush_some(self):
        # Send as much data as possible to our client

        sent = 0
        dobreak = False
        outbufs = self.outbufs

        while True:
            outbuf = outbufs[0]
            # remaining might be -1 for an unseekable ROFBB
            # so we perform a read and assume that the ROFBB will update
            # remaining when it knows it's empty
            while outbuf.remaining != 0:
                chunk = outbuf.read(self.sendbuf_len)
                num_tosend = len(chunk)
                num_sent = self.send(chunk)
                # handle_close may have been called by send() so be careful
                # about mutating state below if num_sent is 0
                sent += num_sent
                if num_sent < num_tosend and self.connected:
                    # failed to write all of the data, so either put the
                    # remaining amount into a new buffer to be used on the
                    # next write or rollback the pointer to only skip what was
                    # successfully written
                    if outbuf.seekable:
                        outbuf.rollback(num_tosend - num_sent)
                    else:
                        outbuf = BytesIOBasedBuffer(chunk[num_sent:])
                        outbufs.appendleft(outbuf)
                if not num_sent:
                    # failed to write anything, break out entirely
                    dobreak = True
                    break
            else:
                # self.outbufs[-1] must always be a writable outbuf
                if len(outbufs) > 1:
                    toclose = outbufs.popleft()
                    try:
                        toclose.close()
                    except Exception:
                        self.logger.exception(
                            'Unexpected error when closing an outbuf')
                else:
                    # caught up, done flushing for now
                    dobreak = True

            if dobreak:
                break

        # refresh the outbuf statistics after a write
        self._scan_outbufs()

        if sent:
            self.last_activity = time.time()
            return True

        return False

    def _scan_outbufs(self):
        self.has_unseekable_outbufs = False
        self.known_outbufs_len = 0
        for o in self.outbufs:
            if o.seekable:
                self.known_outbufs_len += o.remaining
            else:
                self.has_unseekable_outbufs = True
        self.has_outbuf_data = (
            self.known_outbufs_len or self.has_unseekable_outbufs
        )

    def handle_close(self):
        with self.outbuf_lock:
            outbufs = self.outbufs
            while outbufs:
                toclose = outbufs.popleft()
                try:
                    toclose.close()
                except Exception:
                    self.logger.exception(
                        'Unknown exception while trying to close outbuf')
            self.known_outbufs_len = 0
            self.has_outbuf_data = False
            self.has_unseekable_outbufs = False
            self.current_outbuf_count = 0
            self.connected = False
            self.outbuf_lock.notify()
        self.close()

    def add_channel(self, map=None):
        """See wasyncore.dispatcher

        This hook keeps track of opened channels.
        """
        wasyncore.dispatcher.add_channel(self, map)
        self.server.active_channels[self._fileno] = self

    def del_channel(self, map=None):
        """See wasyncore.dispatcher

        This hook keeps track of closed channels.
        """
        fd = self._fileno # next line sets this to None
        wasyncore.dispatcher.del_channel(self, map)
        ac = self.server.active_channels
        if fd in ac:
            del ac[fd]

    #
    # SYNCHRONOUS METHODS
    #

    def write_soon(self, data):
        if not self.connected:
            # if the socket is closed then interrupt the task so that it
            # can cleanup possibly before the app_iter is exhausted
            raise ClientDisconnected
        if data:
            # the async mainloop might be popping data off outbuf; we can
            # block here waiting for it because we're in a task thread
            with self.outbuf_lock:
                self._flush_outbufs_below_high_watermark()
                if not self.connected:
                    raise ClientDisconnected
                if data.__class__ is ReadOnlyFileBasedBuffer:
                    # they used wsgi.file_wrapper
                    self.outbufs.append(data)
                    nextbuf = OverflowableBuffer(self.adj.outbuf_overflow)
                    self.outbufs.append(nextbuf)
                    self.current_outbuf_count = 0
                    num_bytes = data.remaining
                    if num_bytes == -1:
                        self.has_unseekable_outbufs = True
                    else:
                        self.known_outbufs_len += num_bytes
                else:
                    if self.current_outbuf_count > self.adj.outbuf_high_watermark:
                        # rotate to a new buffer if the current buffer has hit
                        # the watermark to avoid it growing unbounded
                        nextbuf = OverflowableBuffer(self.adj.outbuf_overflow)
                        self.outbufs.append(nextbuf)
                        self.current_outbuf_count = 0
                    self.outbufs[-1].append(data)
                    num_bytes = len(data)
                    self.current_outbuf_count += num_bytes
                    self.known_outbufs_len += num_bytes
                self.has_outbuf_data = True
            # XXX We might eventually need to pull the trigger here (to
            # instruct select to stop blocking), but it slows things down so
            # much that I'll hold off for now; "server push" on otherwise
            # unbusy systems may suffer.
            return num_bytes
        return 0

    def _flush_outbufs_below_high_watermark(self):
        # check first to avoid locking if possible
        if self.known_outbufs_len > self.adj.outbuf_high_watermark:
            with self.outbuf_lock:
                while (
                    self.connected and
                    self.known_outbufs_len > self.adj.outbuf_high_watermark
                ):
                    self.outbuf_lock.wait()

    def service(self):
        """Execute all pending requests """
        with self.task_lock:
            while self.requests:
                request = self.requests[0]
                if request.error:
                    task = self.error_task_class(self, request)
                else:
                    task = self.task_class(self, request)
                try:
                    task.service()
                except ClientDisconnected:
                    self.logger.info('Client disconnected while serving %s' %
                                     task.request.path)
                    task.close_on_finish = True
                except Exception:
                    self.logger.exception('Exception while serving %s' %
                                          task.request.path)
                    if not task.wrote_header:
                        if self.adj.expose_tracebacks:
                            body = traceback.format_exc()
                        else:
                            body = ('The server encountered an unexpected '
                                    'internal server error')
                        req_version = request.version
                        req_headers = request.headers
                        request = self.parser_class(self.adj)
                        request.error = InternalServerError(body)
                        # copy some original request attributes to fulfill
                        # HTTP 1.1 requirements
                        request.version = req_version
                        try:
                            request.headers['CONNECTION'] = req_headers[
                                'CONNECTION']
                        except KeyError:
                            pass
                        task = self.error_task_class(self, request)
                        try:
                            task.service() # must not fail
                        except ClientDisconnected:
                            task.close_on_finish = True
                    else:
                        task.close_on_finish = True
                # we cannot allow self.requests to drop to empty til
                # here; otherwise the mainloop gets confused
                if task.close_on_finish:
                    self.close_when_flushed = True
                    for request in self.requests:
                        request.close()
                    self.requests = []
                else:
                    # before processing a new request, ensure there is not too
                    # much data in the outbufs waiting to be flushed
                    # NB: currently readable() returns False while we are
                    # flushing data so we know no new requests will come in
                    # that we need to account for, otherwise it'd be better
                    # to do this check at the start of the request instead of
                    # at the end to account for consecutive service() calls
                    if len(self.requests) > 1:
                        self._flush_outbufs_below_high_watermark()
                    request = self.requests.pop(0)
                    request.close()

        if self.connected:
            self.server.pull_trigger()
        self.last_activity = time.time()

    def cancel(self):
        """ Cancels all pending / active requests """
        self.will_close = True
        self.connected = False
        self.last_activity = time.time()
        self.requests = []
