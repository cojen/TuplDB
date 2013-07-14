/*
 *  Copyright 2013 Brian S O'Neill
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

import java.io.Closeable;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Provides random and stream-oriented access to database values. Stream instances can only be
 * safely used by one thread at a time, and they must be {@link #close closed} when no longer
 * needed. Instances can be exchanged by threads, as long as a happens-before relationship is
 * established. Without proper exclusion, multiple threads interacting with a Stream instance
 * may cause database corruption.
 *
 * @author Brian S O'Neill
 */
public abstract class Stream implements Closeable {
    Object mOpenState;

    Stream() {
    }

    /**
     * Opens the stream for accessing the value referenced by the given key. If stream is
     * already opened when this method is called, it is closed first. When opening the stream
     * with a null transaction, a lock is acquired as needed but never retained. To retain a
     * lock, pass a transaction with a lock mode of {@link LockMode#REPEATABLE_READ
     * REPEATABLE_READ} or higher.
     *
     * <p>If the entry must be locked, ownership of the key instance is
     * transferred. The key must not be modified after calling this method.
     *
     * @param txn optional transaction; pass null for {@link LockMode#READ_COMMITTED
     * READ_COMMITTED} locking behavior
     * @param key non-null key
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws NullPointerException if key is null
     * @throws IllegalArgumentException if key is outside allowed range
     */
    public abstract LockResult open(Transaction txn, byte[] key) throws IOException;

    /**
     * Returns the total length of the value accessed by the Stream.
     *
     * @return value length or -1 if it doesn't exist
     * @throws IllegalStateException if closed
     */
    public abstract long length() throws IOException;

    /**
     * Extends or truncates the value accessed by the Stream. When extended, the new portion of
     * the value is zero-filled.
     *
     * @param length new value length; negative length deletes the value
     * @throws IllegalArgumentException if length is too large
     * @throws IllegalStateException if closed
     */
    public abstract void setLength(long length) throws IOException;

    /**
     * Read from the value, starting from any position. The full requested amount of bytes are
     * read, unless the end is reached. A return value of -1 indicates that value doesn't exist
     * at all, even when the requested amount is zero.
     *
     * @param pos start position to read from
     * @param buf buffer to read into
     * @param off buffer start offset
     * @param len maximum amount to read
     * @return actual amount read, which is less than requested only if the end was reached, or
     * -1 of value doesn't exist
     * @throws IllegalArgumentException if position is negative
     * @throws IndexOutOfBoundsException
     * @throws IllegalStateException if closed
     */
    public final int read(long pos, byte[] buf, int off, int len) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException();
        }
        boundsCheck(buf, off, len);
        return doRead(pos, buf, off, len);
    }

    abstract int doRead(long pos, byte[] buf, int off, int len) throws IOException;

    /**
     * Write into the value, starting from any position. Value is extended when writing past
     * the end, even if the written amount is zero.
     *
     * @param pos start position to write to
     * @param buf buffer to write from
     * @param off buffer start offset
     * @param len maximum length to write
     * @throws IllegalArgumentException if position is negative
     * @throws IndexOutOfBoundsException
     * @throws IllegalStateException if closed
     * @throws IllegalUpgradeException if not locked for writing
     */
    public final void write(long pos, byte[] buf, int off, int len) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException();
        }
        boundsCheck(buf, off, len);
        doWrite(pos, buf, off, len);
    }

    abstract void doWrite(long pos, byte[] buf, int off, int len) throws IOException;

    /**
     * Returns a new buffered InputStream instance, which reads from this Stream. When the
     * InputStream is closed, it closes the Stream too. The InputStream is bound to the Stream,
     * and so only one thread can access either at a time.
     *
     * <p>Reading past the end of the stream returns -1 (EOF), as per the InputStream contract.
     * Reading from a value which doesn't exist causes a {@link NoSuchValueException} to be
     * thrown.
     *
     * @param pos start position to read from
     * @return buffered unsynchronized InputStream
     * @throws IllegalArgumentException if position is negative
     */
    public final InputStream newInputStream(long pos) throws IOException {
        return newInputStream(pos, -1);
    }

    /**
     * Returns a new buffered InputStream instance, which reads from this Stream. When the
     * InputStream is closed, it closes the Stream too. The InputStream is bound to the Stream,
     * and so only one thread can access either at a time.
     *
     * <p>Reading past the end of the stream returns -1 (EOF), as per the InputStream contract.
     * Reading from a value which doesn't exist causes a {@link NoSuchValueException} to be
     * thrown.
     *
     * @param pos start position to read from
     * @param bufferSize requested buffer size; actual size may differ
     * @return buffered unsynchronized InputStream
     * @throws IllegalArgumentException if position is negative
     * @throws IllegalStateException if closed
     */
    public final InputStream newInputStream(long pos, int bufferSize) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException();
        }
        checkOpen();
        return new In(mOpenState, pos, allocBuffer(bufferSize));
    }

    /**
     * Returns a new buffered OutputStream instance, which writes to this Stream. When the
     * OutputStream is closed, it closes the Stream too. The OutputStream is bound to the
     * Stream, and so only one thread can access either at a time.
     *
     * @param pos start position to write to
     * @return buffered unsynchronized OutputStream
     * @throws IllegalArgumentException if position is negative
     * @throws IllegalStateException if closed
     */
    public final OutputStream newOutputStream(long pos) throws IOException {
        return newOutputStream(pos, -1);
    }

    /**
     * Returns a new buffered OutputStream instance, which writes to this Stream. When the
     * OutputStream is closed, it closes the Stream too. The OutputStream is bound to the
     * Stream, and so only one thread can access either at a time.
     *
     * @param pos start position to write to
     * @param bufferSize requested buffer size; actual size may differ
     * @return buffered unsynchronized OutputStream
     * @throws IllegalArgumentException if position is negative
     * @throws IllegalStateException if closed
     */
    public final OutputStream newOutputStream(long pos, int bufferSize) throws IOException {
        if (pos < 0) {
            throw new IllegalArgumentException();
        }
        checkOpen();
        return new Out(mOpenState, pos, allocBuffer(bufferSize));
    }

    abstract int pageSize();

    /**
     * @throws IllegalStateException if closed
     */
    abstract void checkOpen();

    void checkOpen(Object openState) {
        if (openState != mOpenState) {
            throw new IllegalStateException("Stream closed");
        }
    }

    public final void close() {
        mOpenState = null;
        doClose();
    }

    final void close(Object openState) {
        if (openState == mOpenState) {
            mOpenState = null;
            doClose();
        }
    }

    abstract void doClose();

    static void boundsCheck(byte[] buf, int off, int len) {
        if ((off | len | (off + len) | (buf.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }
    }

    private byte[] allocBuffer(int bufferSize) {
        if (bufferSize <= 1) {
            if (bufferSize < 0) {
                bufferSize = pageSize();
            } else {
                bufferSize = 1;
            }
        } else if (bufferSize >= 65536) {
            bufferSize = 65536;
        }
        return new byte[bufferSize];
    }

    final class In extends InputStream {
        private Object mOpenState;

        private long mPos;

        private final byte[] mBuffer;
        private int mStart;
        private int mEnd;

        In(Object openState, long pos, byte[] buffer) {
            if (openState == null) {
                Stream.this.mOpenState = openState = this;
            }
            mOpenState = openState;
            mPos = pos;
            mBuffer = buffer;
        }

        @Override
        public int read() throws IOException {
            checkOpen(mOpenState);

            byte[] buf = mBuffer;
            int start = mStart;
            if (start < mEnd) {
                mPos++;
                int b = buf[start] & 0xff;
                mStart = start + 1;
                return b;
            }

            long pos = mPos;
            int amt = Stream.this.read(pos, buf, 0, buf.length);

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
            checkOpen(mOpenState);

            byte[] buf = mBuffer;
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
                    amt = Stream.this.read(mPos, b, off, len);
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
                    amt = Stream.this.read(mPos, buf, 0, buf.length);
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

            amt = off - initialOff;

            if (amt <= 0) {
                if (amt < 0) {
                    throw new NoSuchValueException();
                }
                return -1;
            }

            return amt;
        }

        @Override
        public long skip(long n) throws IOException {
            checkOpen(mOpenState);

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
            long newPos = Math.min(pos + n, length());

            if (newPos > pos) {
                mPos = newPos;
                return newPos - pos;
            } else {
                return 0;
            }
        }

        @Override
        public int available() throws IOException {
            checkOpen(mOpenState);
            return mEnd - mStart;
        }

        @Override
        public void close() throws IOException {
            Object openState = mOpenState;
            mOpenState = null;
            Stream.this.close(openState);
        }
    }

    final class Out extends OutputStream {
        private Object mOpenState;

        private long mPos;

        private final byte[] mBuffer;

        Out(Object openState, long pos, byte[] buffer) {
            if (openState == null) {
                Stream.this.mOpenState = openState = this;
            }
            mOpenState = openState;
            mPos = pos;
            mBuffer = buffer;
        }

        @Override
        public void write(int b) throws IOException {
            checkOpen(mOpenState);
            // FIXME
            throw null;
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            boundsCheck(b, off, len);
            checkOpen(mOpenState);
            // FIXME
            throw null;
        }

        @Override
        public void flush() throws IOException {
            checkOpen(mOpenState);
            // FIXME
            throw null;
        }

        @Override
        public void close() throws IOException {
            Object openState = mOpenState;
            mOpenState = null;
            Stream.this.close(openState);
        }
    }
}
