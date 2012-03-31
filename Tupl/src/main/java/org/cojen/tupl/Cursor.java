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

import java.io.IOException;

import java.util.concurrent.TimeUnit;

/**
 * Maintains a logical position in an {@link Index}. Cursor instances can only
 * be safely used by one thread at a time, and they must be {@link #reset
 * reset} when no longer needed. Instances can be exchanged by threads, as long
 * as a happens-before relationship is established. Without proper exclusion,
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
 * @author Brian S O'Neill
 * @see Index#newCursor Index.newCursor
 */
public interface Cursor {
    /**
     * Empty marker which indicates that value exists but has not been loaded.
     */
    public static final byte[] NOT_LOADED = new byte[0];

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
     */
    public void autoload(boolean mode);

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
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     */
    public LockResult first() throws IOException;

    /**
     * Moves the Cursor to find the first available entry. Cursor key and value
     * are set to null if no entries exist, and position will be undefined.
     *
     * <p>If locking is required, entries are <i>skipped</i> when not lockable
     * within the specified maximum wait time. Neither {@link LockTimeoutException}
     * nor {@link DeadlockException} are ever thrown from this method.
     *
     * @param maxWait maximum time to wait for lock before moving to next
     * entry; negative is infinite
     * @param unit required unit if maxWait is more than zero
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     */
    public LockResult first(long maxWait, TimeUnit unit) throws IOException;

    /**
     * Moves the Cursor to find the last available entry. Cursor key and value
     * are set to null if no entries exist, and position will be undefined.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     */
    public LockResult last() throws IOException;

    /**
     * Moves the Cursor to find the last available entry. Cursor key and value
     * are set to null if no entries exist, and position will be undefined.
     *
     * <p>If locking is required, entries are <i>skipped</i> when not lockable
     * within the specified maximum wait time. Neither {@link LockTimeoutException}
     * nor {@link DeadlockException} are ever thrown from this method.
     *
     * @param maxWait maximum time to wait for lock before moving to next
     * entry; negative is infinite
     * @param unit required unit if maxWait is more than zero
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     */
    public LockResult last(long maxWait, TimeUnit unit) throws IOException;

    /**
     * Moves the Cursor by a relative amount of entries. Pass a positive amount
     * for forward movement, and pass a negative amount for reverse
     * movement. If less than the given amount are available, the Cursor key
     * and value are set to null, and position will be undefined.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult move(long amount) throws IOException;

    /**
     * Advances to the Cursor to the next available entry. Cursor key and value
     * are set to null if no next entry exists, and position will be undefined.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult next() throws IOException;

    /**
     * Advances to the Cursor to the next available entry. Cursor key and value
     * are set to null if no next entry exists, and position will be undefined.
     *
     * <p>If locking is required, entries are <i>skipped</i> when not lockable
     * within the specified maximum wait time. Neither {@link LockTimeoutException}
     * nor {@link DeadlockException} are ever thrown from this method.
     *
     * @param maxWait maximum time to wait for lock before moving to next
     * entry; negative is infinite
     * @param unit required unit if maxWait is more than zero
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult next(long maxWait, TimeUnit unit) throws IOException;

    /**
     * Advances to the Cursor to the previous available entry. Cursor key and
     * value are set to null if no previous entry exists, and position will be
     * undefined.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult previous() throws IOException;

    /**
     * Advances to the Cursor to the previous available entry. Cursor key and
     * value are set to null if no previous entry exists, and position will be
     * undefined.
     *
     * <p>If locking is required, entries are <i>skipped</i> when not lockable
     * within the specified maximum wait time. Neither {@link LockTimeoutException}
     * nor {@link DeadlockException} are ever thrown from this method.
     *
     * @param maxWait maximum time to wait for lock before moving to next
     * entry; negative is infinite
     * @param unit required unit if maxWait is more than zero
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public LockResult previous(long maxWait, TimeUnit unit) throws IOException;

    /**
     * Moves the Cursor to find the given key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it should
     * no longer be modified after calling this method.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult find(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry greater than or equal
     * to the given key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it should
     * no longer be modified after calling this method.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult findGe(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry greater than or equal
     * to the given key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it should
     * no longer be modified after calling this method.
     *
     * <p>If locking is required, entries are <i>skipped</i> when not lockable
     * within the specified maximum wait time. Neither {@link LockTimeoutException}
     * nor {@link DeadlockException} are ever thrown from this method.
     *
     * @param maxWait maximum time to wait for lock before moving to next
     * entry; negative is infinite
     * @param unit required unit if maxWait is more than zero
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult findGe(byte[] key, long maxWait, TimeUnit unit) throws IOException;

    /**
     * Moves the Cursor to find the first available entry greater than the
     * given key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it should
     * no longer be modified after calling this method.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult findGt(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry greater than the
     * given key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it should
     * no longer be modified after calling this method.
     *
     * <p>If locking is required, entries are <i>skipped</i> when not lockable
     * within the specified maximum wait time. Neither {@link LockTimeoutException}
     * nor {@link DeadlockException} are ever thrown from this method.
     *
     * @param maxWait maximum time to wait for lock before moving to next
     * entry; negative is infinite
     * @param unit required unit if maxWait is more than zero
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult findGt(byte[] key, long maxWait, TimeUnit unit) throws IOException;

    /**
     * Moves the Cursor to find the first available entry less than or equal to
     * the given key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it should
     * no longer be modified after calling this method.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult findLe(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry less than or equal to
     * the given key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it should
     * no longer be modified after calling this method.
     *
     * <p>If locking is required, entries are <i>skipped</i> when not lockable
     * within the specified maximum wait time. Neither {@link LockTimeoutException}
     * nor {@link DeadlockException} are ever thrown from this method.
     *
     * @param maxWait maximum time to wait for lock before moving to next
     * entry; negative is infinite
     * @param unit required unit if maxWait is more than zero
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult findLe(byte[] key, long maxWait, TimeUnit unit) throws IOException;

    /**
     * Moves the Cursor to find the first available entry less than the given
     * key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it should
     * no longer be modified after calling this method.
     *
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult findLt(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry less than the given
     * key.
     *
     * <p>Ownership of the key instance transfers to the Cursor, and it should
     * no longer be modified after calling this method.
     *
     * <p>If locking is required, entries are <i>skipped</i> when not lockable
     * within the specified maximum wait time. Neither {@link LockTimeoutException}
     * nor {@link DeadlockException} are ever thrown from this method.
     *
     * @param maxWait maximum time to wait for lock before moving to next
     * entry; negative is infinite
     * @param unit required unit if maxWait is more than zero
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     * @throws NullPointerException if key is null
     */
    public LockResult findLt(byte[] key, long maxWait, TimeUnit unit) throws IOException;

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
     * Loads or reloads the value at the cursor's current position. Cursor
     * value is set to null if entry no longer exists, but the position remains
     * the same.
     *
     * @throws IllegalStateException if position is undefined at invocation time
     * @return UNOWNED, ACQUIRED, OWNED_SHARED, OWNED_UPGRADABLE, or
     * OWNED_EXCLUSIVE
     */
    public LockResult load() throws IOException;

    /**
     * Stores a value into the current entry, leaving the position
     * unchanged. An entry may be inserted, updated or deleted by this
     * method. A null value deletes the entry. Unless an exception is thrown,
     * the object returned by the {@link #value} method will be the same
     * instance as was provided to this method.
     *
     * @param value value to store; pass null to delete
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public void store(byte[] value) throws IOException;

    /**
     * Appends data to the current entry's value, creating it if necessary.
     *
     * @param data non-null data to append
     * @throws NullPointerException if data is null
     * @throws IllegalStateException if position is undefined at invocation time
     */
    //public void append(byte[] data) throws IOException;

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
     * Resets Cursor and moves it to an undefined position.
     */
    public void reset();
}
