/*
 *  Copyright 2011 Brian S O'Neill
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
import java.io.IOException;

/**
 * Maintains a logical position in an {@link Index}. Cursors must be {@link
 * #close closed} when no longer needed to free up resources. Cursor instances
 * can only be safely used by one thread at a time. Instances can be exchanged
 * by threads, as long as a happens-before relationship is established. Without
 * proper exclusion, multiple threads interacting with a Cursor instance may
 * cause database corruption.
 *
 * @author Brian S O'Neill
 */
public interface Cursor extends Closeable {
    /**
     * Returns an uncopied reference to the current key, or null if Cursor is
     * unpositioned. Array contents must not be modified.
     */
    public byte[] key();

    /**
     * Returns an uncopied reference to the current value, which might be
     * null. Array contents can be safely modified.
     */
    public byte[] value();

    /**
     * Moves the Cursor to find the first available entry. Cursor key and value
     * are set to null if no entries exist, and position is now undefined.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     */
    public LockResult first() throws IOException;

    /**
     * Moves the Cursor to find the last available entry. Cursor key and value
     * are set to null if no entries exist, and position is now undefined.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     */
    public LockResult last() throws IOException;

    /**
     * Moves the Cursor by a relative amount of entries. Pass a positive amount
     * for forward movement, and pass a negative amount for reverse
     * movement. If less than the given amount are available, the Cursor key
     * and value are set to null, and position is now undefined.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult move(long amount) throws IOException;

    /**
     * Advances to the Cursor to the next available entry. Cursor key and value
     * are set to null if no next entry exists, and position is now undefined.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult next() throws IOException;

    /**
     * Advances to the Cursor to the previous available entry. Cursor key and value
     * are set to null if no previous entry exists, and position is now undefined.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult previous() throws IOException;

    /**
     * Moves the Cursor to find the given key. Ownership of the key instance
     * transfers to the Cursor, and it should no longer be modified after
     * calling this method.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult find(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry greater than or equal
     * to the given key. Ownership of the key instance transfers to the Cursor,
     * and it should no longer be modified after calling this method.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult findGe(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry greater than the
     * given key. Ownership of the key instance transfers to the Cursor, and it
     * should no longer be modified after calling this method.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult findGt(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry less than or equal to
     * the given key. Ownership of the key instance transfers to the Cursor,
     * and it should no longer be modified after calling this method.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult findLe(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry less than the given
     * key. Ownership of the key instance transfers to the Cursor, and it
     * should no longer be modified after calling this method.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult findLt(byte[] key) throws IOException;

    /**
     * Optimized version of the regular find method, which can perform fewer
     * search steps if the given key is in close proximity to the current
     * one. Even if not in close proximity, the find behavior is still
     * identicial, although it may perform more slowly.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it should
     * no longer be modified after calling this method.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult findNearby(byte[] key) throws IOException;

    /**
     * Stores a value into the current entry, leaving the position
     * unchanged. An entry may be inserted, updated or deleted by this
     * method. A null value deletes the entry.
     *
     * @param value value to store; pass null to delete
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public void store(byte[] value) throws IOException;

    /**
     * Link to a transaction, which can be null for auto-commit mode.
     */
    public void link(Transaction txn);

    /**
     * Returns a new independent Cursor, positioned where this one is, and
     * linked to the same transaction. The original and copied Cursor can be
     * acted upon without affecting each other's state.
     */
    public Cursor copy();

    /**
     * Closes Cursor and moves it to an undefined position. Cursor is
     * automatically re-opened on demand.
     */
    @Override
    public void close();
}
