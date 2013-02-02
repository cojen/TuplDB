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
     * @throws NullPointerException if key is null
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
     * Read from the value, starting from any position.
     *
     * @param pos start position to read from
     * @param buf buffer to read into
     * @param off buffer start offset
     * @param len maximum length to read
     * @return actual amount read; non-negative; is less than the requested length only if read
     * would extend beyond the value size
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
     * InputStream is closed, it closes the Stream too.
     *
     * @param pos start position to read from
     * @return buffered unsynchronized InputStream
     * @throws IllegalArgumentException if position is negative
     * @throws IllegalStateException if closed
     */
    public final InputStream newInputStream(long pos) throws IOException {
        return newInputStream(pos, -1);
    }

    /**
     * Returns a new buffered InputStream instance, which reads from this Stream. When the
     * InputStream is closed, it closes the Stream too.
     *
     * @param pos start position to read from
     * @param bufferSize requested buffer size; actual size may differ
     * @return buffered unsynchronized InputStream
     * @throws IllegalArgumentException if position is negative
     * @throws IllegalStateException if closed
     */
    public final InputStream newInputStream(long pos, int bufferSize) throws IOException {
        // FIXME
        throw null;
    }

    /**
     * Returns a new buffered OutputStream instance, which writes to this Stream. When the
     * OutputStream is closed, it closes the Stream too.
     *
     * @param pos start position to write to
     * @return buffered unsynchronized OutputStream
     * @throws IllegalArgumentException if position is negative
     * @throws IllegalStateException if closed
     * @throws IllegalUpgradeException if not locked for writing
     */
    public final OutputStream newOutputStream(long pos) throws IOException {
        return newOutputStream(pos, -1);
    }

    /**
     * Returns a new buffered OutputStream instance, which writes to this Stream. When the
     * OutputStream is closed, it closes the Stream too.
     *
     * @param pos start position to write to
     * @param bufferSize requested buffer size; actual size may differ
     * @return buffered unsynchronized OutputStream
     * @throws IllegalArgumentException if position is negative
     * @throws IllegalStateException if closed
     * @throws IllegalUpgradeException if not locked for writing
     */
    public final OutputStream newOutputStream(long pos, int bufferSize) throws IOException {
        // FIXME
        throw null;
    }

    public final void close() {
        // FIXME: Ensure that all open InputStream and OutputStreams are disabled. Can use a
        // version number or registry or object ref.
        doClose();
    }

    abstract void doClose();

    static void boundsCheck(byte[] buf, int off, int len) {
        if ((off | len | (off + len) | (buf.length - (off + len))) < 0) {
            throw new IndexOutOfBoundsException();
        }
    }

    final class In extends InputStream {
        @Override
        public int read() throws IOException {
            // FIXME
            throw null;
        }

        @Override
        public int read(byte[] buf, int off, int len) throws IOException {
            boundsCheck(buf, off, len);
            // FIXME
            throw null;
        }

        @Override
        public long skip(long n) throws IOException {
            // FIXME
            throw null;
        }

        @Override
        public int available() throws IOException {
            // FIXME
            throw null;
        }

        @Override
        public void close() throws IOException {
            Stream.this.close();
        }
    }

    final class Out extends OutputStream {
        @Override
        public void write(int b) throws IOException {
            // FIXME
            throw null;
        }

        @Override
        public void write(byte[] buf, int off, int len) throws IOException {
            boundsCheck(buf, off, len);
            // FIXME
            throw null;
        }

        @Override
        public void flush() throws IOException {
            // FIXME
            throw null;
        }

        @Override
        public void close() throws IOException {
            Stream.this.close();
        }
    }
}
