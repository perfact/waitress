import unittest
import io

class FileBasedBufferTests(object):

    def test_seekable(self):
        inst = self._makeOneFromBytes()
        self.assertTrue(inst.seekable)
        self.assertEqual(inst.remaining, 0)

    def test___bool__(self):
        inst = self._makeOneFromBytes()
        inst.remaining = 10
        self.assertEqual(bool(inst), True)
        inst.remaining = 0
        self.assertEqual(bool(inst), False)
        inst.remaining = -1
        self.assertEqual(bool(inst), True)

    def test_append(self):
        inst = self._makeOneFromBytes(b'data')
        inst.append(b'data2')
        self.assertEqual(inst.remaining, 9)
        self.assertEqual(inst.read(), b'datadata2')
        self.assertEqual(inst.remaining, 0)

    def test_read_zero(self):
        inst = self._makeOneFromBytes(b'data')
        result = inst.read(0)
        self.assertEqual(result, b'')
        self.assertEqual(inst.remaining, 4)

    def test_read_all(self):
        inst = self._makeOneFromBytes(b'data')
        result = inst.read()
        self.assertEqual(result, b'data')
        self.assertEqual(inst.remaining, 0)

    def test_read_not_enough(self):
        inst = self._makeOneFromBytes(b'data')
        result = inst.read(3)
        self.assertEqual(result, b'dat')
        self.assertEqual(inst.remaining, 1)

    def test_read_exact(self):
        inst = self._makeOneFromBytes(b'data')
        result = inst.read(4)
        self.assertEqual(result, b'data')
        self.assertEqual(inst.remaining, 0)

    def test_read_too_much(self):
        inst = self._makeOneFromBytes(b'data')
        result = inst.read(100)
        self.assertEqual(result, b'data')
        self.assertEqual(inst.remaining, 0)

    def test_rollback(self):
        inst = self._makeOneFromBytes(b'data')
        self.assertEqual(inst.remaining, 4)
        result = inst.read(3)
        self.assertEqual(inst.remaining, 1)
        self.assertEqual(result, b'dat')
        inst.rollback(len(result))
        self.assertEqual(inst.remaining, 4)
        result = inst.read()
        self.assertEqual(inst.remaining, 0)
        self.assertEqual(result, b'data')

    def test_close(self):
        inst = self._makeOneFromBytes()
        inst.close()
        self.assertEqual(inst.remaining, 0)

class TestTempfileBasedBuffer(FileBasedBufferTests, unittest.TestCase):

    def _makeOne(self, from_buffer=None):
        from waitress.buffers import TempfileBasedBuffer
        buffer = TempfileBasedBuffer(from_buffer=from_buffer)
        self.buffers.append(buffer)
        return buffer

    def _makeOneFromBytes(self, from_bytes=None):
        return self._makeOne(from_buffer=io.BytesIO(from_bytes))

    def setUp(self):
        self.buffers = []

    def tearDown(self):
        for b in self.buffers:
            b.close()

class TestBytesIOBasedBuffer(FileBasedBufferTests, unittest.TestCase):

    def _makeOne(self, from_bytes=None):
        from waitress.buffers import BytesIOBasedBuffer
        return BytesIOBasedBuffer(from_bytes)

    _makeOneFromBytes = _makeOne

class TestReadOnlyFileBasedBuffer(FileBasedBufferTests, unittest.TestCase):

    def _makeOne(self, file, block_size=32768):
        from waitress.buffers import ReadOnlyFileBasedBuffer
        buffer = ReadOnlyFileBasedBuffer(file, block_size)
        self.buffers.append(buffer)
        return buffer

    def _makeOneFromBytes(self, from_bytes=None):
        buffer = self._makeOne(io.BytesIO(from_bytes))
        buffer.prepare()
        return buffer

    def setUp(self):
        self.buffers = []

    def tearDown(self):
        for b in self.buffers:
            b.close()

    def test_append(self):  # overrides FileBasedBufferTests.test_append
        inst = self._makeOneFromBytes()
        self.assertRaises(NotImplementedError, inst.append, 'a')

    def test_prepare_unseekable(self):
        f = KindaFilelike(b'abc')
        inst = self._makeOne(f)
        result = inst.prepare()
        self.assertEqual(result, -1)
        self.assertFalse(inst.seekable)
        self.assertEqual(inst.remaining, -1)

    def test_prepare_seekable(self):
        f = Filelike(b'abc', tellresults=[0, 10])
        inst = self._makeOne(f)
        result = inst.prepare()
        self.assertEqual(result, 10)
        self.assertTrue(inst.seekable)
        self.assertEqual(inst.remaining, 10)
        self.assertEqual(inst.file.seeked, 0)

    def test_prepare_maxsize_lt_len(self):
        f = Filelike(b'abc', tellresults=[0, 10])
        inst = self._makeOne(f)
        result = inst.prepare(3)
        self.assertEqual(result, 3)
        self.assertEqual(inst.remaining, 3)
        self.assertTrue(inst.seekable)

    def test_prepare_maxsize_gt_len(self):
        f = Filelike(b'abc', tellresults=[3, 10])
        inst = self._makeOne(f)
        result = inst.prepare(15)
        self.assertEqual(result, 7)
        self.assertEqual(inst.remaining, 7)
        self.assertTrue(inst.seekable)

    def test_read_numbytes_neg_one(self):
        f = io.BytesIO(b'abcdef')
        f.seek(4)
        inst = self._makeOne(f)
        inst.prepare()
        self.assertEqual(inst.remaining, 2)
        result = inst.read(-1)
        self.assertEqual(result, b'ef')
        self.assertEqual(inst.remaining, 0)
        self.assertEqual(f.tell(), 6)

    def test_get_numbytes_gt_remain(self):
        f = io.BytesIO(b'abcdef')
        inst = self._makeOne(f)
        inst.remaining = 2
        result = inst.read(3)
        self.assertEqual(result, b'ab')
        self.assertEqual(inst.remaining, 0)
        self.assertEqual(f.tell(), 2)

    def test_get_numbytes_lt_remain(self):
        f = io.BytesIO(b'abcdef')
        inst = self._makeOne(f)
        inst.remaining = 2
        result = inst.read(1)
        self.assertEqual(result, b'a')
        self.assertEqual(inst.remaining, 1)
        self.assertEqual(f.tell(), 1)

    def test___iter__(self):
        data = b'a' * 10000
        f = io.BytesIO(data)
        inst = self._makeOne(f)
        r = b''
        for val in inst:
            r += val
        self.assertEqual(r, data)

    def test_unseekable_updates_remaining_at_eof(self):
        f = io.BytesIO(b'abcdef')
        inst = self._makeOne(f)
        inst.remaining = -1
        result1 = inst.read()
        result2 = inst.read()
        self.assertEqual(result1, b'abcdef')
        self.assertEqual(result2, b'')
        self.assertEqual(inst.remaining, 0)


class TestOverflowableBuffer(FileBasedBufferTests, unittest.TestCase):

    def _makeOne(self, overflow=10):
        from waitress.buffers import OverflowableBuffer
        buffer = OverflowableBuffer(overflow)
        self.buffers.append(buffer)
        return buffer

    def _makeOneFromBytes(self, from_bytes=None):
        buffer = self._makeOne()
        if from_bytes:
            buffer.append(from_bytes)
        return buffer

    def setUp(self):
        self.buffers = []

    def tearDown(self):
        for b in self.buffers:
            b.close()

    def test_append_with_len_more_than_max_int(self):
        from waitress.compat import MAXINT
        inst = self._makeOne()
        inst.overflowed = True
        buf = DummyBuffer(length=MAXINT)
        inst.buf = buf
        inst.remaining = MAXINT
        result = inst.append(b'x')
        # we don't want this to throw an OverflowError on Python 2 (see
        # https://github.com/Pylons/waitress/issues/47)
        self.assertEqual(result, None)
        
    def test_append_buf_None_not_longer_than_strbuf_limit(self):
        inst = self._makeOne()
        inst.strbuf = b'x' * 5
        inst.remaining = len(inst.strbuf)
        inst.append(b'hello')
        self.assertEqual(inst.strbuf, b'xxxxxhello')
        self.assertEqual(inst.remaining, 10)

    def test_append_buf_None_longer_than_strbuf_limit(self):
        inst = self._makeOne(10000)
        inst.strbuf = b'x' * 8192
        inst.remaining = len(inst.strbuf)
        inst.append(b'hello')
        self.assertEqual(inst.strbuf, b'')
        self.assertEqual(inst.buf.remaining, 8197)

    def test_append_overflow(self):
        inst = self._makeOne(10)
        inst.strbuf = b'x' * 8192
        inst.remaining = len(inst.strbuf)
        inst.append(b'hello')
        self.assertEqual(inst.strbuf, b'')
        self.assertEqual(inst.buf.remaining, 8197)

    def test_append_sz_gt_overflow(self):
        from waitress.buffers import BytesIOBasedBuffer
        inst = self._makeOne()
        buf = BytesIOBasedBuffer()
        inst.buf = buf
        inst.overflow = 2
        inst.append(b'data2')
        self.assertTrue(inst.overflowed)
        self.assertNotEqual(inst.buf, buf)

    def test_read_strbuf(self):
        inst = self._makeOne(10)
        inst.strbuf = b'x'
        inst.remaining = len(inst.strbuf)
        result = inst.read()
        self.assertEqual(result, b'x')
        self.assertEqual(inst.remaining, 0)

    def test_rollback_strbuf(self):
        inst = self._makeOne(10)
        inst.strbuf = b'x'
        inst.remaining = len(inst.strbuf)
        result = inst.read()
        self.assertEqual(result, b'x')
        self.assertEqual(inst.remaining, 0)
        inst.rollback(1)
        self.assertEqual(inst.remaining, 1)
        result = inst.read()
        self.assertEqual(result, b'x')
        self.assertEqual(inst.remaining, 0)

    def test_close_nobuf(self):
        inst = self._makeOne()
        inst.buf = None
        self.assertEqual(inst.close(), None) # doesnt raise

    def test_close_withbuf(self):
        class Buffer(object):
            def close(self):
                self.closed = True
        buf = Buffer()
        inst = self._makeOne()
        inst.buf = buf
        inst.close()
        self.assertTrue(buf.closed)

class KindaFilelike(object):

    def __init__(self, bytes, close=None, tellresults=None):
        self.bytes = bytes
        self.tellresults = tellresults
        if close is not None:
            self.close = lambda: close

class Filelike(KindaFilelike):

    def seek(self, v, whence=0):
        self.seeked = v

    def tell(self):
        v = self.tellresults.pop(0)
        return v

class DummyBuffer(object):
    def __init__(self, length=0):
        self.remaining = length

    def append(self, s):
        self.remaining = self.remaining + len(s)

    def close(self):
        self.closed = True
