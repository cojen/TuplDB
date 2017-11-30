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

import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;

import static java.lang.System.arraycopy;

/**
 * Simple buffered input stream.
 *
 * @author Brian S O'Neill
 */
abstract class DataIn extends InputStream {
    static final class Stream extends DataIn {
        private final InputStream mIn;

        Stream(long pos, InputStream in) {
            this(pos, in, 64 << 10);
        }

        Stream(long pos, InputStream in, int bufferSize) {
            super(pos, bufferSize);
            mIn = in;
        }

        @Override
        int doRead(byte[] buf, int off, int len) throws IOException {
            return mIn.read(buf, off, len);
        }

        @Override
        public void close() throws IOException {
            mIn.close();
        }
    }

    private final byte[] mBuffer;

    private int mStart;
    private int mEnd;

    long mPos;

    DataIn(long pos, int bufferSize) {
        if (pos < 0) {
            throw new IllegalArgumentException("Negative position: " + pos);
        }
        mPos = pos;
        mBuffer = new byte[bufferSize];
    }

    /**
     * @return -1 if EOF
     */
    abstract int doRead(byte[] buf, int off, int len) throws IOException;

    @Override
    public int read() throws IOException {
        int start = mStart;
        if (mEnd - start > 0) {
            mStart = start + 1;
            mPos++;
            return mBuffer[start] & 0xff;
        } else {
            int amt = doRead(mBuffer, 0, mBuffer.length);
            if (amt <= 0) {
                return -1;
            } else {
                mStart = 1;
                mEnd = amt;
                mPos++;
                return mBuffer[0] & 0xff;
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int start = mStart;
        int avail = mEnd - start;
        if (avail >= len) {
            arraycopy(mBuffer, start, b, off, len);
            mStart = start + len;
            mPos += len;
            return len;
        } else {
            arraycopy(mBuffer, start, b, off, avail);
            mStart = 0;
            mEnd = 0;
            off += avail;
            len -= avail;

            if (avail > 0) {
                mPos += avail;
                return avail;
            } else if (len >= mBuffer.length) {
                int amt = doRead(b, off, len);
                if (amt > 0) {
                    mPos += amt;
                }
                return amt;
            } else {
                int amt = doRead(mBuffer, 0, mBuffer.length);
                if (amt <= 0) {
                    return amt;
                } else {
                    int fill = Math.min(amt, len);
                    arraycopy(mBuffer, 0, b, off, fill);
                    mStart = fill;
                    mEnd = amt;
                    mPos += fill;
                    return fill;
                }
            }
        }
    }

    public int readIntBE() throws IOException {
        int start = require(4);
        int v = Utils.decodeIntBE(mBuffer, start);
        mStart = start + 4;
        mPos += 4;
        return v;
    }

    public int readIntLE() throws IOException {
        int start = require(4);
        int v = Utils.decodeIntLE(mBuffer, start);
        mStart = start + 4;
        mPos += 4;
        return v;
    }

    public long readLongBE() throws IOException {
        int start = require(8);
        long v = Utils.decodeLongBE(mBuffer, start);
        mStart = start + 8;
        mPos += 8;
        return v;
    }

    public long readLongLE() throws IOException {
        int start = require(8);
        long v = Utils.decodeLongLE(mBuffer, start);
        mStart = start + 8;
        mPos += 8;
        return v;
    }

    public int readUnsignedVarInt() throws IOException {
        int start = require(1);
        byte[] b = mBuffer;
        int v = b[start++];
        int amt = 1;

        if (v < 0) {
            switch ((v >> 4) & 0x07) {
            case 0x00: case 0x01: case 0x02: case 0x03:
                start = require(start, 1);
                v = (1 << 7)
                    + (((v & 0x3f) << 8)
                       | (b[start++] & 0xff));
                amt = 2;
                break;
            case 0x04: case 0x05:
                start = require(start, 2);
                v = ((1 << 14) + (1 << 7))
                    + (((v & 0x1f) << 16)
                       | ((b[start++] & 0xff) << 8)
                       | (b[start++] & 0xff));
                amt = 3;
                break;
            case 0x06:
                start = require(start, 3);
                v = ((1 << 21) + (1 << 14) + (1 << 7))
                    + (((v & 0x0f) << 24)
                       | ((b[start++] & 0xff) << 16)
                       | ((b[start++] & 0xff) << 8)
                       | (b[start++] & 0xff));
                amt = 4;
                break;
            default:
                start = require(start, 4);
                v = ((1 << 28) + (1 << 21) + (1 << 14) + (1 << 7)) 
                    + ((b[start++] << 24)
                       | ((b[start++] & 0xff) << 16)
                       | ((b[start++] & 0xff) << 8)
                       | (b[start++] & 0xff));
                amt = 5;
                break;
            }
        }

        mStart = start;
        mPos += amt;
        return v;
    }

    public long readUnsignedVarLong() throws IOException {
        int start = require(1);
        byte[] b = mBuffer;
        int d = b[start++];
        int amt;

        long v;
        if (d >= 0) {
            v = (long) d;
            amt = 1;
        } else {
            switch ((d >> 4) & 0x07) {
            case 0x00: case 0x01: case 0x02: case 0x03:
                start = require(start, 1);
                v = (1L << 7) +
                    (((d & 0x3f) << 8)
                     | (b[start++] & 0xff));
                amt = 2;
                break;
            case 0x04: case 0x05:
                start = require(start, 2);
                v = ((1L << 14) + (1L << 7))
                    + (((d & 0x1f) << 16)
                       | ((b[start++] & 0xff) << 8)
                       | (b[start++] & 0xff));
                amt = 3;
                break;
            case 0x06:
                start = require(start, 3);
                v = ((1L << 21) + (1L << 14) + (1L << 7))
                    + (((d & 0x0f) << 24)
                       | ((b[start++] & 0xff) << 16)
                       | ((b[start++] & 0xff) << 8)
                       | (b[start++] & 0xff));
                amt = 4;
                break;
            default:
                switch (d & 0x0f) {
                default:
                    start = require(start, 4);
                    v = ((1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                        + (((d & 0x07L) << 32)
                           | (((long) (b[start++] & 0xff)) << 24)
                           | (((long) (b[start++] & 0xff)) << 16)
                           | (((long) (b[start++] & 0xff)) << 8)
                           | ((long) (b[start++] & 0xff)));
                    amt = 5;
                    break;
                case 0x08: case 0x09: case 0x0a: case 0x0b:
                    start = require(start, 5);
                    v = ((1L << 35)
                         + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                        + (((d & 0x03L) << 40)
                           | (((long) (b[start++] & 0xff)) << 32)
                           | (((long) (b[start++] & 0xff)) << 24)
                           | (((long) (b[start++] & 0xff)) << 16)
                           | (((long) (b[start++] & 0xff)) << 8)
                           | ((long) (b[start++] & 0xff)));
                    amt = 6;
                    break;
                case 0x0c: case 0x0d:
                    start = require(start, 6);
                    v = ((1L << 42) + (1L << 35)
                         + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                        + (((d & 0x01L) << 48)
                           | (((long) (b[start++] & 0xff)) << 40)
                           | (((long) (b[start++] & 0xff)) << 32)
                           | (((long) (b[start++] & 0xff)) << 24)
                           | (((long) (b[start++] & 0xff)) << 16)
                           | (((long) (b[start++] & 0xff)) << 8)
                           | ((long) (b[start++] & 0xff)));
                    amt = 7;
                    break;
                case 0x0e:
                    start = require(start, 7);
                    v = ((1L << 49) + (1L << 42) + (1L << 35)
                         + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                        + ((((long) (b[start++] & 0xff)) << 48)
                           | (((long) (b[start++] & 0xff)) << 40)
                           | (((long) (b[start++] & 0xff)) << 32)
                           | (((long) (b[start++] & 0xff)) << 24)
                           | (((long) (b[start++] & 0xff)) << 16)
                           | (((long) (b[start++] & 0xff)) << 8)
                           | ((long) (b[start++] & 0xff)));
                    amt = 8;
                    break;
                case 0x0f:
                    start = require(start, 8);
                    v = ((1L << 56) + (1L << 49) + (1L << 42) + (1L << 35)
                         + (1L << 28) + (1L << 21) + (1L << 14) + (1L << 7))
                        + ((((long) b[start++]) << 56)
                           | (((long) (b[start++] & 0xff)) << 48)
                           | (((long) (b[start++] & 0xff)) << 40)
                           | (((long) (b[start++] & 0xff)) << 32)
                           | (((long) (b[start++] & 0xff)) << 24)
                           | (((long) (b[start++] & 0xff)) << 16)
                           | (((long) (b[start++] & 0xff)) << 8L)
                           | ((long) (b[start++] & 0xff)));
                    amt = 9;
                    break;
                }
                break;
            }
        }

        mStart = start;
        mPos += amt;
        return v;
    }

    public long readSignedVarLong() throws IOException {
        long v = readUnsignedVarLong();
        return ((v & 1) != 0) ? ((~(v >> 1)) | (1L << 63)) : (v >>> 1);
    }

    public void readFully(byte[] b) throws IOException {
        Utils.readFully(this, b, 0, b.length);
    }

    /**
     * Reads a byte string prefixed with a variable length.
     */
    public byte[] readBytes() throws IOException {
        byte[] bytes = new byte[readUnsignedVarInt()];
        readFully(bytes);
        return bytes;
    }

    /**
     * Transfers data to the given visitor, in the form of cursorValueWrite calls.
     */
    public void cursorValueWrite(RedoVisitor visitor, long cursorId, long txnId,
                                 long pos, int amount)
        throws IOException
    {
        int avail = mEnd - mStart;

        while (true) {
            if (amount <= avail) {
                visitor.cursorValueWrite(cursorId, txnId, pos, mBuffer, mStart, amount);
                mStart += amount;
                mPos += amount;
                return;
            }

            visitor.cursorValueWrite(cursorId, txnId, pos, mBuffer, mStart, avail);

            mStart = mEnd = 0;
            mPos += avail;
            pos += avail;
            amount -= avail;

            avail = doRead(mBuffer, 0, mBuffer.length);

            if (avail < 0) {
                throw new EOFException();
            }

            mEnd = avail;
        }
    }

    /**
     * @return start
     */
    private int require(int amount) throws IOException {
        return require(mStart, amount);
    }

    /**
     * @return start
     */
    private int require(int start, int amount) throws IOException {
        int avail = mEnd - start;
        if ((amount -= avail) <= 0) {
            return start;
        }

        if (mBuffer.length - mEnd < amount) {
            arraycopy(mBuffer, start, mBuffer, 0, avail);
            mStart = start = 0;
            mEnd = avail;
        }

        while (true) {
            int amt = doRead(mBuffer, mEnd, mBuffer.length - mEnd);
            if (amt <= 0) {
                throw new EOFException();
            }
            mEnd += amt;
            if ((amount -= amt) <= 0) {
                return start;
            }
        }
    }
}
