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

import java.io.IOException;
import java.io.OutputStream;

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.io.Utils;

/**
 * @author Brian S O'Neill
 * @see ClientCursor#newValueOutputStream
 * @see ServerCursor
 */
final class ValueOutputStream extends OutputStream implements RemoteOutputControl {
    private final ClientCursor mCursor;
    private Pipe mPipe;

    private final byte[] mChunkBuffer;
    private int mChunkEnd;

    private volatile Throwable mException;

    ValueOutputStream(ClientCursor cursor, Pipe pipe, int bufferSize) throws IOException {
        mCursor = cursor;
        mPipe = pipe;

        if (bufferSize <= 0) {
            bufferSize = bufferSize == 0 ? 1 : 4096;
        } else {
            bufferSize = Math.min(bufferSize, 0x7ffe);
        }

        mChunkBuffer = new byte[2 + bufferSize];
        mChunkEnd = 2;

        pipe.writeObject(this); // provide RemoteOutputControl instance
        pipe.flush();
    }

    @Override
    public void write(int b) throws IOException {
        Pipe pipe = checkClosed();

        byte[] buf = mChunkBuffer;
        int end = mChunkEnd;
        if (end >= buf.length) {
            doFlush(pipe, end, 0);
            end = 2;
        }
        buf[end++] = (byte) b;
        if (end < buf.length) {
            mChunkEnd = end;
        } else {
            doFlush(pipe, end, 0);
        }
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        Pipe pipe = checkClosed();
        byte[] buf = mChunkBuffer;

        if (len > 0) {
            int end = mChunkEnd;

            while (true) {
                int avail = buf.length - end;

                if (len <= avail) {
                    System.arraycopy(b, off, buf, end, len);
                    end += len;
                    if (len < avail) {
                        mChunkEnd = end;
                    } else {
                        doFlush(pipe, end, 0);
                    }
                    return;
                }

                System.arraycopy(b, off, buf, end, avail);
                off += avail;
                len -= avail;

                doFlush(pipe, buf.length, 0);
                end = 2;
            }
        }
    }

    @Override
    public void flush() throws IOException {
        Pipe pipe = checkClosed();
        int end = mChunkEnd;
        if (end > 2) {
            doFlush(pipe, end, 0);
        }
    }

    @Override
    public void close() throws IOException {
        Pipe pipe = mPipe;
        if (pipe == null) {
            return;
        }

        mCursor.reset();

        try {
            doFlush(pipe, mChunkEnd, 0x8000);
        } finally {
            mPipe = null;
        }

        Throwable ex = mException;
        if (ex != null) {
            Utils.closeQuietly(pipe);
            mException = null;
            Utils.rethrow(ex);
        }

        // Wait for ack before the pipe can be safely recycled.

        int ack = pipe.read();
        if (ack < 0) {
            pipe.close();
        } else {
            pipe.recycle();
        }
    }

    // Specified by RemoteOutputControl.
    @Override
    public void exception(Throwable e) {
        mException = e;
    }

    // Specified by RemoteOutputControl.
    @Override
    public void dispose() {
    }

    /**
     * @param finished is 0x8000 when closing; 0 otherwise
     */
    private void doFlush(Pipe pipe, int end, int finished) throws IOException {
        byte[] buf = mChunkBuffer;
        Utils.encodeShortBE(buf, 0, (end - 2) | finished);
        try {
            mPipe.write(buf, 0, end);
            mPipe.flush();
        } catch (Throwable e) {
            Utils.closeQuietly(mPipe);
            mChunkEnd = mChunkBuffer.length;
            Throwable ex = mException;
            if (ex != null) {
                Utils.rethrow(ex);
            }
            throw e;
        }

        mChunkEnd = 2;
    }

    private Pipe checkClosed() {
        Pipe pipe = mPipe;
        if (pipe == null) {
            throw new IllegalStateException("Stream closed");
        }
        return pipe;
    }
}
