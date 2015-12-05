/*
 *  Copyright 2013-2015 Cojen.org
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
 * @see View#newStream View.newStream
 */
interface Stream extends Closeable {
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
    public LockResult open(Transaction txn, byte[] key) throws IOException;

    /**
     * Link to a transaction, which can be null for auto-commit mode. To continue using a
     * stream after the transaction is complete, link it to null or another
     * transaction. Otherwise, the original transaction will be resurrected.
     *
     * @return prior linked transaction
     * @throws IllegalArgumentException if transaction belongs to another database instance
     * @see Cursor#link
     */
    public Transaction link(Transaction txn);

    /**
     * Returns the transaction the stream is currently linked to.
     */
    public Transaction link();

    /**
     * Returns the total length of the value accessed by the Stream.
     *
     * @return value length or -1 if it doesn't exist
     * @throws IllegalStateException if closed
     */
    public long length() throws IOException;

    /**
     * Extends or truncates the value accessed by the Stream. When extended, the new portion of
     * the value is zero-filled.
     *
     * @param length new value length; negative length deletes the value
     * @throws IllegalArgumentException if length is too large
     * @throws IllegalStateException if closed
     */
    public void setLength(long length) throws IOException;

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
     * -1 if value doesn't exist
     * @throws IllegalArgumentException if position is negative
     * @throws IndexOutOfBoundsException
     * @throws IllegalStateException if closed
     */
    public int read(long pos, byte[] buf, int off, int len) throws IOException;

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
    public void write(long pos, byte[] buf, int off, int len) throws IOException;

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
    public InputStream newInputStream(long pos) throws IOException;

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
    public InputStream newInputStream(long pos, int bufferSize) throws IOException;

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
    public OutputStream newOutputStream(long pos) throws IOException;

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
    public OutputStream newOutputStream(long pos, int bufferSize) throws IOException;

    /**
     * Closes the stream, but does not flush any OutputStream instances.
     */
    @Override
    public void close() throws IOException;
}
