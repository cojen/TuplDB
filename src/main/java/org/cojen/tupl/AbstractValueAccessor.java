/*
 *  Copyright (C) 2011-2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author Brian S O'Neill
 */
abstract class AbstractValueAccessor implements ValueAccessor {
    @Override
    public final int valueRead(long pos, byte[] buf, int off, int len) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException();
        }
        boundsCheck(buf, off, len);
        return doValueRead(pos, buf, off, len);
    }

    @Override
    public final void valueWrite(long pos, byte[] buf, int off, int len) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException();
        }
        boundsCheck(buf, off, len);
        doValueWrite(pos, buf, off, len);
    }

    @Override
    public final InputStream newValueInputStream(long pos) throws IOException {
        return newValueInputStream(pos, -1);
    }

    @Override
    public final InputStream newValueInputStream(long pos, int bufferSize) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException();
        }
        valueCheckOpen();
        return new In(pos, new byte[valueStreamBufferSize(bufferSize)]);
    }

    @Override
    public final OutputStream newValueOutputStream(long pos) throws IOException {
        return newValueOutputStream(pos, -1);
    }

    @Override
    public final OutputStream newValueOutputStream(long pos, int bufferSize) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException();
        }
        valueCheckOpen();
        return new Out(pos, new byte[valueStreamBufferSize(bufferSize)]);
    }

    abstract int doValueRead(long pos, byte[] buf, int off, int len) throws IOException;

    abstract void doValueWrite(long pos, byte[] buf, int off, int len) throws IOException;

    /**
     * Return an appropriate buffer size, using the given size suggestion.
     *
     * @param bufferSize buffer size hint; -1 if a default size should be used
     * @return actual size; must be greater than zero
     */
    abstract int valueStreamBufferSize(int bufferSize);

    /**
     * @throws IllegalStateException if closed
     */
    abstract void valueCheckOpen();

    /**
     * @throws NullPointerException if buf is null
     * @throws IndexOutOfBoundsException if off or len are out of bound
     */
    static void boundsCheck(byte[] buf, int off, int len) {
        if ((off | len | (off + len) | (buf.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }
    }

    final class In extends InputStream {
        private long mPos;
        private byte[] mBuffer;
        private int mStart;
        private int mEnd;

        In(long pos, byte[] buffer) {
            mPos = pos;
            mBuffer = buffer;
        }

        @Override
        public int read() throws IOException {
            byte[] buf = checkStreamOpen();
            int start = mStart;
            if (start < mEnd) {
                mPos++;
                int b = buf[start] & 0xff;
                mStart = start + 1;
                return b;
            }

            long pos = mPos;
            int amt = AbstractValueAccessor.this.doValueRead(pos, buf, 0, buf.length);

            if (amt <= 0) {
                if (amt < 0) {
                    throw new NoSuchValueException();
                }
                return -1;
            }

            mEnd = amt;
            mPos = pos + 1;
            mStart = 1;
            return buf[0] & 0xff;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            boundsCheck(b, off, len);

            byte[] buf = checkStreamOpen();
            int start = mStart;
            int amt = mEnd - start;

            if (amt >= len) {
                // Enough available in the buffer.
                System.arraycopy(buf, start, b, off, len);
                mStart = start + len;
                mPos += len;
                return len;
            }

            final int initialOff = off;

            if (amt > 0) {
                // Drain everything available from the buffer.
                System.arraycopy(buf, start, b, off, amt);
                mEnd = start;
                off += amt;
                len -= amt;
                mPos += amt;
            }

            doRead: {
                // Bypass buffer if parameter is large enough.
                while (len >= buf.length) {
                    amt = AbstractValueAccessor.this.doValueRead(mPos, b, off, len);
                    if (amt <= 0) {
                        break doRead;
                    }
                    off += amt;
                    len -= amt;
                    mPos += amt;
                    if (len <= 0) {
                        break doRead;
                    }
                }

                // Read into buffer and copy to parameter.
                while (true) {
                    amt = AbstractValueAccessor.this.doValueRead(mPos, buf, 0, buf.length);
                    if (amt <= 0) {
                        break doRead;
                    }
                    if (amt >= len) {
                        System.arraycopy(buf, 0, b, off, len);
                        off += len;
                        mPos += len;
                        mStart = len;
                        mEnd = amt;
                        break doRead;
                    }
                    // Drain everything available from the buffer.
                    System.arraycopy(buf, 0, b, off, amt);
                    off += amt;
                    len -= amt;
                    mPos += amt;
                }
            }

            int actual = off - initialOff;

            if (actual <= 0) {
                if (amt < 0) {
                    throw new NoSuchValueException();
                }
                return -1;
            }

            return actual;
        }

        @Override
        public long skip(long n) throws IOException {
            checkStreamOpen();

            if (n <= 0) {
                return 0;
            }

            int start = mStart;
            int amt = mEnd - start;

            if (amt > 0) {
                if (n >= amt) {
                    // Skip the entire buffer.
                    mEnd = start;
                } else {
                    amt = (int) n;
                    mStart = start + amt;
                }
                mPos += amt;
                return amt;
            }

            long pos = mPos;
            long newPos = Math.min(pos + n, valueLength());

            if (newPos > pos) {
                mPos = newPos;
                return newPos - pos;
            } else {
                return 0;
            }
        }

        @Override
        public int available() {
            return mBuffer == null ? 0 : (mEnd - mStart);
        }

        @Override
        public void close() throws IOException {
            mBuffer = null;
            AbstractValueAccessor.this.close();
        }

        private byte[] checkStreamOpen() {
            byte[] buf = mBuffer;
            if (buf == null) {
                throw new IllegalStateException("Stream closed");
            }
            return buf;
        }
    }

    final class Out extends OutputStream {
        private long mPos;
        private byte[] mBuffer;
        private int mEnd;

        Out(long pos, byte[] buffer) {
            mPos = pos;
            mBuffer = buffer;
        }

        @Override
        public void write(int b) throws IOException {
            byte[] buf = checkStreamOpen();
            int end = mEnd;

            if (end >= buf.length) {
                flush();
                end = 0;
            }

            buf[end++] = (byte) b;

            try {
                if (end >= buf.length) {
                    AbstractValueAccessor.this.doValueWrite(mPos, buf, 0, end);
                    mPos += end;
                    end = 0;
                }
            } finally {
                mEnd = end;
            }
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            boundsCheck(b, off, len);

            byte[] buf = checkStreamOpen();
            int end = mEnd;
            int avail = buf.length - end;

            if (len < avail) {
                System.arraycopy(b, off, buf, end, len);
                mEnd = end + len;
                return;
            }

            if (end != 0) {
                System.arraycopy(b, off, buf, end, avail);
                off += avail;
                len -= avail;
                avail = buf.length;
                try {
                    AbstractValueAccessor.this.doValueWrite(mPos, buf, 0, avail);
                } catch (Throwable e) {
                    mEnd = avail;
                    throw e;
                }
                mPos += avail;
                if (len < avail) {
                    System.arraycopy(b, off, buf, 0, len);
                    mEnd = len;
                    return;
                }
                mEnd = 0;
            }

            AbstractValueAccessor.this.doValueWrite(mPos, b, off, len);
            mPos += len;
        }

        @Override
        public void flush() throws IOException {
            doFlush(checkStreamOpen());
        }

        @Override
        public void close() throws IOException {
            byte[] buf = mBuffer;
            if (buf != null) {
                doFlush(buf);
                mBuffer = null;
            }
            AbstractValueAccessor.this.close();
        }

        private void doFlush(byte[] buf) throws IOException {
            int end = mEnd;
            if (end > 0) {
                AbstractValueAccessor.this.doValueWrite(mPos, buf, 0, end);
                mPos += end;
                mEnd = 0;
            }
        }

        private byte[] checkStreamOpen() {
            byte[] buf = mBuffer;
            if (buf == null) {
                throw new IllegalStateException("Stream closed");
            }
            return buf;
        }
    }
}
