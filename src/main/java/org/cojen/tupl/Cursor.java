/*
 *  Copyright 2011-2013 Brian S O'Neill
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

import java.io.IOException;

/**
 * Maintains a logical position in a {@link View}. Cursor instances can only be
 * safely used by one thread at a time, and they must be {@link #reset reset}
 * when no longer needed. Instances can be exchanged by threads, as long as a
 * happens-before relationship is established. Without proper exclusion,
 * multiple threads interacting with a Cursor instance may cause database
 * corruption.
 *
 * <p>Methods which return {@link LockResult} might acquire a lock to access
 * the requested entry. The return type indicates if the lock is still {@link
 * LockResult#isHeld held}, and in what fashion. Except where indicated, a
 * {@link LockTimeoutException} is thrown when a lock cannot be acquired in
 * time. When cursor is {@link #link linked} to a transaction, it defines the
 * locking behavior and timeout. Otherwise, a lock is always acquired, with the
 * {@link DatabaseConfig#lockTimeout default} timeout.
 *
 * <p>If a {@link LockFailureException} is thrown from any method, the Cursor
 * is positioned at the desired key, but the value is {@link #NOT_LOADED}.
 *
 * @author Brian S O'Neill
 * @see View#newCursor View.newCursor
 */
public interface Cursor {
    /**
     * Empty marker which indicates that value exists but has not been {@link
     * #load loaded}.
     */
    public static final byte[] NOT_LOADED = new byte[0];

    /**
     * Returns the key ordering for this cursor.
     */
    public Ordering getOrdering();

    /**
     * Link to a transaction, which can be null for auto-commit mode. All
     * entries visited by the cursor become part of the given transaction.  To
     * continue using a cursor after the transaction is complete, link it to
     * null or another transaction. Otherwise, the orginal transaction will be
     * resurrected.
     *
     * @return prior linked transaction
     * @throws IllegalArgumentException if transaction belongs to another database instance
     */
    public Transaction link(Transaction txn);

    /**
     * Returns an uncopied reference to the current key, or null if Cursor is
     * unpositioned. Array contents must not be modified.
     */
    public byte[] key();

    /**
     * Returns an uncopied reference to the current value, which might be null
     * or {@link #NOT_LOADED}. Array contents can be safely modified.
     */
    public byte[] value();

    /**
     * By default, values are loaded automatically, as they are seen. When
     * disabled, values must be {@link Cursor#load manually loaded}.
     *
     * @param mode false to disable
     * @return prior autoload mode
     */
    public boolean autoload(boolean mode);

    /**
     * Compare the current key to the one given.
     *
     * @param rkey key to compare to
     * @return a negative integer, zero, or a positive integer as current key
     * is less than, equal to, or greater than the rkey.
     * @throws NullPointerException if current key or rkey is null
     */
    public int compareKeyTo(byte[] rkey);

    /**
     * Compare the current key to the one given.
     *
     * @param rkey key to compare to
     * @param offset offset into rkey
     * @param length length of rkey
     * @return a negative integer, zero, or a positive integer as current key
     * is less than, equal to, or greater than the rkey.
     * @throws NullPointerException if current key or rkey is null
     */
    public int compareKeyTo(byte[] rkey, int offset, int length);

    /**
     * Moves the Cursor to find the first available entry. Cursor key and value
     * are set to null if no entries exist, and position will be undefined.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    public LockResult first() throws IOException;

    /**
     * Moves the Cursor to find the last available entry. Cursor key and value
     * are set to null if no entries exist, and position will be undefined.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    public LockResult last() throws IOException;

    /**
     * Moves the Cursor by a relative amount of entries. Pass a positive amount
     * to skip forward, and pass a negative amount to skip backwards. If less
     * than the given amount of entries are available, the Cursor key and value
     * are set to null, and position will be undefined.
     *
     * <p>Skipping by 1 is equivalent to calling {@link #next next}, and
     * skipping by -1 is equivalent to calling {@link #previous previous}. A
     * skip of 0 merely checks and returns the lock state for the current
     * key. Lock acquisition only applies to the target entry &mdash; no locks
     * are acquired for entries in between.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult skip(long amount) throws IOException;

    /**
     * Moves to the Cursor to the next available entry. Cursor key and value
     * are set to null if no next entry exists, and position will be undefined.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult next() throws IOException;

    /**
     * Moves to the Cursor to the next available entry, but only when less than
     * or equal to the given limit key. Cursor key and value are set to null if
     * no applicable entry exists, and position will be undefined.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws NullPointerException if limit key is null
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult nextLe(byte[] limitKey) throws IOException;

    /**
     * Moves to the Cursor to the next available entry, but only when less than
     * the given limit key. Cursor key and value are set to null if no
     * applicable entry exists, and position will be undefined.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws NullPointerException if limit key is null
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult nextLt(byte[] limitKey) throws IOException;

    /**
     * Moves to the Cursor to the previous available entry. Cursor key and
     * value are set to null if no previous entry exists, and position will be
     * undefined.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult previous() throws IOException;

    /**
     * Moves to the Cursor to the previous available entry, but only when
     * greater than or equal to the given limit key. Cursor key and value are
     * set to null if no applicable entry exists, and position will be
     * undefined.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws NullPointerException if limit key is null
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult previousGe(byte[] limitKey) throws IOException;

    /**
     * Moves to the Cursor to the previous available entry, but only when
     * greater than the given limit key. Cursor key and value are set to null
     * if no applicable entry exists, and position will be undefined.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws NullPointerException if limit key is null
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult previousGt(byte[] limitKey) throws IOException;

    /**
     * Moves the Cursor to find the given key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it must
     * not be modified after calling this method.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws NullPointerException if key is null
     */
    public LockResult find(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry greater than or equal
     * to the given key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it must
     * not be modified after calling this method.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws NullPointerException if key is null
     */
    public LockResult findGe(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry greater than the
     * given key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it must
     * not be modified after calling this method.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws NullPointerException if key is null
     */
    public LockResult findGt(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry less than or equal to
     * the given key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it must
     * not be modified after calling this method.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws NullPointerException if key is null
     */
    public LockResult findLe(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry less than the given
     * key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it must
     * not be modified after calling this method.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws NullPointerException if key is null
     */
    public LockResult findLt(byte[] key) throws IOException;

    /**
     * Optimized version of the regular find method, which can perform fewer
     * search steps if the given key is in close proximity to the current
     * one. Even if not in close proximity, the find behavior is still
     * identicial, although it may perform more slowly.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it must
     * not be modified after calling this method.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws NullPointerException if key is null
     */
    public LockResult findNearby(byte[] key) throws IOException;

    /**
     * Moves the Cursor to a random entry, but not guaranteed to be chosen from
     * a uniform distribution. Cursor key and value are set to null if no
     * entries exist, and position will be undefined.
     *
     * @param lowKey inclusive lowest key in the selectable range; pass null for open range
     * @param highKey exclusive highest key in the selectable range; pass null for open range
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException;

    /**
     * Loads or reloads the value at the cursor's current position. Cursor
     * value is set to null if entry no longer exists, but the position remains
     * unmodified.
     *
     * @throws IllegalStateException if position is undefined at invocation time
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    public LockResult load() throws IOException;

    /**
     * Stores a value into the current entry, leaving the position
     * unchanged. An entry may be inserted, updated or deleted by this
     * method. A null value deletes the entry. Unless an exception is thrown,
     * the object returned by the {@link #value value} method will be the same
     * instance as was provided to this method.
     *
     * @param value value to store; pass null to delete
     * @throws IllegalStateException if position is undefined at invocation time
     * @throws ViewConstraintException if value is not permitted
     */
    public void store(byte[] value) throws IOException;

    //public int read(LockResult[] result,int start,byte[] b, int off, int len) throws IOException;

    /**
     * Appends data to the current entry's value, creating it if necessary.
     *
     * @param data non-null data to append
     * @throws NullPointerException if data is null
     * @throws IllegalStateException if position is undefined at invocation time
     */
    //public void append(byte[] data) throws IOException;

    /**
     * Returns a new independent Cursor, positioned where this one is, and
     * linked to the same transaction. The original and copied Cursor can be
     * acted upon without affecting each other's state.
     */
    public Cursor copy();

    /**
     * Resets Cursor and moves it to an undefined position.
     */
    public void reset();
}
