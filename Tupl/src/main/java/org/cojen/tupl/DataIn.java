/*
 *  Copyright 2011-2012 Brian S O'Neill
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;

/**
 * Simple buffered input stream.
 *
 * @author Brian S O'Neill
 */
class DataIn extends InputStream {
    private final InputStream mIn;
    private final byte[] mBuffer;

    private int mStart;
    private int mEnd;

    DataIn(InputStream in) {
        this(in, 4096);
    }

    DataIn(InputStream in, int bufferSize) {
        mIn = in;
        mBuffer = new byte[bufferSize];
    }

    @Override
    public int read() throws IOException {
        int start = mStart;
        if (mEnd - start > 0) {
            mStart = start + 1;
            return mBuffer[start] & 0xff;
        } else {
            int amt = mIn.read(mBuffer);
            if (amt <= 0) {
                return -1;
            } else {
                mStart = 1;
                mEnd = amt;
                return mBuffer[0] & 0xff;
            }
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int start = mStart;
        int avail = mEnd - start;
        if (avail >= len) {
            System.arraycopy(mBuffer, start, b, off, len);
            mStart = start + len;
            return len;
        } else {
            System.arraycopy(mBuffer, start, b, off, avail);
            mStart = 0;
            mEnd = 0;
            off += avail;
            len -= avail;

            if (avail > 0) {
                return avail;
            } else if (len >= mBuffer.length) {
                return mIn.read(b, off, len);
            } else {
                int amt = mIn.read(mBuffer, 0, mBuffer.length);
                if (amt <= 0) {
                    return amt;
                } else {
                    int fill = Math.min(amt, len);
                    System.arraycopy(mBuffer, 0, b, off, fill);
                    mStart = fill;
                    mEnd = amt;
                    return fill;
                }
            }
        }
    }

    @Override
    public void close() throws IOException {
        mIn.close();
    }

    public int readIntBE() throws IOException {
        require(4);
        int v = Utils.readIntBE(mBuffer, mStart);
        mStart += 4;
        return v;
    }

    public int readIntLE() throws IOException {
        require(4);
        int v = Utils.readIntLE(mBuffer, mStart);
        mStart += 4;
        return v;
    }

    public long readLongBE() throws IOException {
        require(8);
        long v = Utils.readLongBE(mBuffer, mStart);
        mStart += 8;
        return v;
    }

    public long readLongLE() throws IOException {
        require(8);
        long v = Utils.readLongLE(mBuffer, mStart);
        mStart += 8;
        return v;
    }

    public int readUnsignedVarInt() throws IOException {
        tryRequire(5);
        int v = Utils.readUnsignedVarInt(mBuffer, mStart, mEnd);
        mStart += Utils.calcUnsignedVarIntLength(v);
        return v;
    }

    public void readFully(byte[] b) throws IOException {
        int len = b.length;
        if (len > 0) {
            int off = 0;
            while (true) {
                int amt = read(b, off, len);
                if (amt <= 0) {
                    throw new EOFException();
                }
                if ((len -= amt) <= 0) {
                    break;
                }
                off += amt;
            }
        }
    }

    /**
     * Reads a byte string prefixed with a variable length.
     */
    public byte[] readBytes() throws IOException {
        byte[] bytes = new byte[readUnsignedVarInt()];
        readFully(bytes);
        return bytes;
    }

    public void require(int amount) throws IOException {
        if (!tryRequire(amount)) {
            throw new EOFException();
        }
    }

    public boolean tryRequire(int amount) throws IOException {
        int avail = mEnd - mStart;
        if ((amount -= avail) <= 0) {
            return true;
        }

        if (mBuffer.length - mEnd < amount) {
            System.arraycopy(mBuffer, mStart, mBuffer, 0, avail);
            mStart = 0;
            mEnd = avail;
        }

        while (true) {
            int amt = mIn.read(mBuffer, mEnd, mBuffer.length - mEnd);
            if (amt <= 0) {
                return false;
            }
            mEnd += amt;
            if ((amount -= amt) <= 0) {
                return true;
            }
        }
    }
}
