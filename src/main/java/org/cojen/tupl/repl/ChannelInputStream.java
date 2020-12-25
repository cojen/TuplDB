/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.nio.charset.StandardCharsets;

import java.util.zip.CRC32C;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ChannelInputStream extends InputStream {
    private final InputStream mSource;
    private final CRC32C mCRC;
    byte[] mBuffer;
    int mPos;
    private int mEnd;

    // Amount of bytes remaining to feed into the checksum.
    private int mChecksumLength;
    private int mChecksumValue;

    ChannelInputStream(InputStream source, int size, boolean checkCRCs) {
        mSource = source;
        mBuffer = new byte[size];
        mCRC = checkCRCs ? new CRC32C() : null;
    }

    /**
     * @param length amount of input to be checked
     * @param value expected CRC value
     */
    void prepareChecksum(int length, int value) throws IOException {
        CRC32C crc = mCRC;

        if (crc != null) {
            crc.reset();
            int avail = Math.min(length, mEnd - mPos);
            crc.update(mBuffer, mPos, avail);

            length -= avail;
            mChecksumLength = length;
            mChecksumValue = value;

            if (length <= 0 && value != (int) crc.getValue()) {
                throw checksumMismatch();
            }
        }
    }

    byte readByte() throws IOException {
        int b = read();
        if (b < 0) {
            throw new EOFException();
        }
        return (byte) b;
    }

    int readIntLE() throws IOException {
        fillBuffer(4);
        int pos = mPos;
        int value = Utils.decodeIntLE(mBuffer, pos);
        mPos = pos + 4;
        return value;
    }

    long readLongLE() throws IOException {
        fillBuffer(8);
        int pos = mPos;
        long value = Utils.decodeLongLE(mBuffer, pos);
        mPos = pos + 8;
        return value;
    }

    String readStr(int len) throws IOException {
        if (len <= mBuffer.length) {
            fillBuffer(len);
            var str = new String(mBuffer, mPos, len, StandardCharsets.UTF_8);
            mPos += len;
            return str;
        } else {
            var b = new byte[len];
            readFully(b, 0, b.length);
            return new String(b, StandardCharsets.UTF_8);
        }
    }

    void readFully(byte[] b, int off, int len) throws IOException {
        while (len > 0) {
            int amt = read(b, off, len);
            if (amt <= 0) {
                throw new EOFException();
            }
            off += amt;
            len -= amt;
        }
    }

    /**
     * Fully read and expand the buffer if necessary. The buffer instance might be replaced
     * as a result of making this call.
     */
    void readFully(int length) throws IOException {
        if (mBuffer.length < length) {
            var newBuffer = new byte[Math.max(length, (int) (mBuffer.length * 1.5))];
            int avail = mEnd - mPos;
            System.arraycopy(mBuffer, mPos, newBuffer, 0, avail);
            mBuffer = newBuffer;
            mPos = 0;
            mEnd = avail;
        }

        fillBuffer(length);
    }

    void skipFully(long n) throws IOException {
        while (n > 0) {
            long amt = skip(n);
            if (amt <= 0) {
                throw new EOFException();
            }
            n -= amt;
        }
    }

    @Override
    public int read() throws IOException {
        int pos = mPos;
        int avail = mEnd - pos;
        byte[] buf = mBuffer;

        if (avail <= 0) {
            avail = doRead(buf, 0, buf.length);
            if (avail <= 0) {
                return -1;
            }
            pos = 0;
            mEnd = avail;
        }

        int b = buf[pos++] & 0xff;
        mPos = pos;
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int avail = mEnd - mPos;
        byte[] buf = mBuffer;

        if (avail <= 0) {
            if (len >= buf.length) {
                return doRead(b, off, len);
            }
            avail = doRead(buf, 0, buf.length);
            if (avail <= 0) {
                return -1;
            }
            mPos = 0;
            mEnd = avail;
        }

        len = Math.min(avail, len);
        System.arraycopy(buf, mPos, b, off, len);
        mPos += len;
        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        // Stop checking the checksum.
        mChecksumLength = 0;

        int avail = mEnd - mPos;

        if (avail > 0) {
            if (n >= avail) {
                mPos = 0;
                mEnd = 0;
                return avail;
            }
            mPos += n;
            return n;
        }

        return mSource.skip(n);
    }

    @Override
    public int available() throws IOException {
        return mEnd - mPos + mSource.available();
    }

    @Override
    public void close() throws IOException {
        mPos = 0;
        mEnd = 0;
        mSource.close();
    }

    /**
     * Fully transfers all data until EOF is read.
     */
    public void drainTo(OutputStream out) throws IOException {
        byte[] buf = mBuffer;

        int avail = mEnd - mPos;
        if (avail > 0) {
            out.write(buf, mPos, avail);
            mPos = 0;
            mEnd = 0;
        }

        int amt;
        while ((amt = doRead(buf, 0, buf.length)) > 0) {
            out.write(buf, 0, amt);
        }
    }

    private void fillBuffer(int required) throws IOException {
        int avail = mEnd - mPos;
        if ((required -= avail) > 0) {
            fillBuffer(required, avail);
        }
    }

    private void fillBuffer(int required, int avail) throws IOException {
        byte[] buf = mBuffer;
        int end = mEnd;
        int tail = buf.length - end;
        if (tail < required) {
            // Shift buffer contents to make room.
            System.arraycopy(buf, mPos, buf, 0, avail);
            mPos = 0;
            mEnd = end = avail;
            tail = buf.length - end;
        }

        while (true) {
            avail = doRead(buf, end, tail);
            if (avail <= 0) {
                throw new EOFException();
            }
            end += avail;
            mEnd = end;
            required -= avail;
            if (required <= 0) {
                break;
            }
            tail -= avail;
        }
    }

    private int doRead(byte[] buf, int offset, int length) throws IOException {
        int amt = mSource.read(buf, offset, length);

        if (amt > 0 && mChecksumLength > 0) {
            int avail = Math.min(mChecksumLength, amt);
            CRC32C crc = mCRC;
            crc.update(buf, offset, avail);
            if ((mChecksumLength -= avail) <= 0 && mChecksumValue != (int) crc.getValue()) {
                throw checksumMismatch();
            }
        }

        return amt;
    }

    private IOException checksumMismatch() {
        Utils.closeQuietly(this);
        return new IOException("Checksum mismatch");
    }
}
