/*
 *  Copyright (C) 2023 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.remote;

import java.io.InputStream;
import java.io.IOException;

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.io.Utils;

/**
 * @author Brian S O'Neill
 * @see ClientCursor#newValueInputStream
 * @see ServerCursor#readTransfer
 */
final class ValueInputStream extends InputStream {
    private final ClientCursor mCursor;
    private final Pipe mPipe;

    private int mChunkSize;
    private byte[] mChunkBuffer;
    private int mChunkPos;
    private int mChunkEnd;

    private Throwable mException;

    ValueInputStream(ClientCursor cursor, Pipe pipe) throws IOException {
        mCursor = cursor;
        mPipe = pipe;
        // Flush to finish the request.
        pipe.flush();
    }

    @Override
    public int read() throws IOException {
        int pos = mChunkPos;
        int avail = mChunkEnd - pos;

        if (avail <= 0) {
            avail = readChunk();
            if (avail <= 0) {
                return -1;
            }
            pos = 0;
        }

        int b = mChunkBuffer[pos++] & 0xff;
        mChunkPos = pos;
        return b;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        int pos = mChunkPos;
        int avail = mChunkEnd - pos;

        if (avail <= 0) {
            avail = readChunk();
            if (avail <= 0) {
                return -1;
            }
            pos = 0;
        }

        len = Math.min(avail, len);
        System.arraycopy(mChunkBuffer, pos, b, off, len);
        mChunkPos += len;
        return len;
    }

    @Override
    public long skip(long n) throws IOException {
        if (n <= 0) {
            return 0;
        }

        int avail = available();

        if (avail <= 0) {
            avail = readChunk();
            if (avail <= 0) {
                return 0;
            }
        }

        if (n >= avail) {
            mChunkPos = mChunkEnd;
            return avail;
        } else {
            mChunkPos += (int) n;
            return n;
        }
    }

    @Override
    public int available() {
        return mChunkEnd - mChunkPos;
    }

    @Override
    public void close() throws IOException {
        int size = mChunkSize;
        if (size != -1) {
            mChunkSize = -1;
            mChunkBuffer = null;
            mChunkPos = mChunkEnd;
            mCursor.reset();
            if ((size & 0x8000) == 0) {
                mPipe.close();
            }
        }
    }

    /**
     * @return amount which can be read; is -1 if the end is reached
     */
    private int readChunk() throws IOException {
        int size = mChunkSize;
        if ((size & 0x8000) != 0) {
            if (size == -1) {
                throw new IllegalStateException("Stream closed");
            }
            Throwable ex = mException;
            if (ex != null) {
                Utils.rethrow(ex);
            }
            return -1;
        }

        try {
            mChunkSize = size = mPipe.readUnsignedShort();
        } catch (Throwable e) {
            failed();
            throw e;
        }

        if (size >= 0xffff) {
            Throwable ex;
            try {
                ex = (Throwable) mPipe.readThrowable();
            } catch (Throwable e) {
                failed();
                throw e;
            }
            mException = ex;
            finished();
            throw Utils.rethrow(ex);
        }

        int avail = size & 0x7fff;

        byte[] buf = mChunkBuffer;
        if (buf == null || buf.length < avail) {
            mChunkBuffer = buf = new byte[avail];
        }

        try {
            mPipe.readFully(buf, 0, avail);
        } catch (Throwable e) {
            failed();
            throw e;
        }

        mChunkPos = 0;
        mChunkEnd = avail;

        if ((size & 0x8000) != 0) {
            finished();
        }

        return avail;
    }

    private void failed() {
        mChunkSize = 0xffff;
        Utils.closeQuietly(mPipe);
    }

    private void finished() {
        Pipe pipe = mPipe;
        try {
            pipe.write(1); // write ack
            pipe.flush();
            pipe.recycle();
        } catch (Throwable e) {
            Utils.closeQuietly(pipe);
        }
    }
}
