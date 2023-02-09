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
 * @see ServerCursor#writeTransfer
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
            flushNoAck(pipe, end);
            end = 2;
        }
        buf[end++] = (byte) b;
        if (end < buf.length) {
            mChunkEnd = end;
        } else {
            flushNoAck(pipe, end);
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
                        flushNoAck(pipe, end);
                    }
                    return;
                }

                System.arraycopy(b, off, buf, end, avail);
                off += avail;
                len -= avail;

                flushNoAck(pipe, buf.length);
                end = 2;
            }
        }
    }

    @Override
    public void flush() throws IOException {
        // After writing a data chunk (if any), also write an empty chunk to indicate that an
        // ack is requested.

        Pipe pipe = checkClosed();

        byte[] buf = mChunkBuffer;
        int end = mChunkEnd;

        try {
            write: {
                if (end > 2) {
                    Utils.encodeShortBE(buf, 0, end - 2); // length of data
                    if (end <= buf.length - 2) {
                        // There's room in the buffer to encode the length of the empty chunk
                        // at the end. This avoids and extra call to the pipe.
                        Utils.encodeShortBE(buf, end, 0);
                        pipe.write(buf, 0, end + 2);
                        mChunkEnd = 2;
                        break write;
                    }
                    pipe.write(buf, 0, end);
                    mChunkEnd = 2;
                }
                pipe.writeShort(0); // length of empty chunk
            }

            pipe.flush();

            // Wait for ack.
            pipe.read();
        } catch (Throwable e) {
            failed();
            throw e;
        }
    }

    @Override
    public void close() throws IOException {
        Pipe pipe = mPipe;
        if (pipe == null) {
            return;
        }

        try {
            try {
                byte[] buf = mChunkBuffer;
                int end = mChunkEnd;
                Utils.encodeShortBE(buf, 0, (end - 2) | 0x8000);
                pipe.write(buf, 0, end);
                pipe.flush();
                mChunkEnd = 2;
            } catch (Throwable e) {
                failed();
                throw e;
            } finally {
                mPipe = null;
            }

            Throwable ex = mException;
            if (ex != null) {
                Utils.closeQuietly(pipe);
                mException = null;
                throw Utils.rethrow(ex);
            }

            // Wait for ack before the pipe can be safely recycled.

            int ack;
            try {
                ack = pipe.read();
            } catch (Throwable e) {
                Utils.closeQuietly(pipe);
                throw e;
            }

            if (ack < 0) {
                pipe.close();
            } else {
                pipe.recycle();
            }
        } finally {
            mCursor.reset();
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
     * @param end must be more than 2, thus ensuring that a non-empty chunk is flushed. An
     * empty chunk requests that an ack be sent back.
     */
    private void flushNoAck(Pipe pipe, int end) throws IOException {
        if (end <= 2) {
            throw new AssertionError();
        }
        try {
            byte[] buf = mChunkBuffer;
            Utils.encodeShortBE(buf, 0, end - 2);
            pipe.write(buf, 0, end);
            mChunkEnd = 2;
            pipe.flush();
        } catch (Throwable e) {
            failed();
            throw e;
        }
    }

    private Pipe checkClosed() {
        Pipe pipe = mPipe;
        if (pipe == null) {
            throw new IllegalStateException("Stream closed");
        }
        return pipe;
    }

    private void failed() {
        Utils.closeQuietly(mPipe);
        mChunkEnd = mChunkBuffer.length;
        Throwable ex = mException;
        if (ex != null) {
            throw Utils.rethrow(ex);
        }
    }
}
