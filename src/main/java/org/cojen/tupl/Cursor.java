/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl;

import java.io.Closeable;
import java.io.IOException;

import java.util.Comparator;

import static java.util.Arrays.compareUnsigned;

import org.cojen.tupl.views.ViewUtils;

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
 * time. When cursor is {@linkplain #link linked} to a transaction, it defines the
 * locking behavior and timeout. Otherwise, a lock is always acquired, with the
 * {@linkplain DatabaseConfig#lockTimeout default} timeout.
 *
 * <p>If a {@link LockFailureException} is thrown from any method, the Cursor
 * is positioned at the desired key, but the value is {@link #NOT_LOADED}.
 *
 * @author Brian S O'Neill
 * @see View#newCursor View.newCursor
 */
public interface Cursor extends ValueAccessor, Closeable {
    /**
     * Empty marker which indicates that value exists but has not been {@linkplain #load
     * loaded}.
     */
    public static final byte[] NOT_LOADED = new byte[0];

    /**
     * Returns the key ordering for this cursor.
     */
    public Ordering ordering();

    /**
     * Returns a key comparator for the ordering of this view, or null if unordered.
     */
    public default Comparator<byte[]> comparator() {
        return null;
    }

    /**
     * Link to a transaction, which can be null for auto-commit mode. All
     * entries visited by the cursor become part of the given transaction.  To
     * continue using a cursor after the transaction is complete, link it to
     * null or another transaction. Otherwise, the original transaction will be
     * resurrected.
     *
     * @return prior linked transaction
     * @throws IllegalStateException if transaction belongs to another database instance
     */
    public Transaction link(Transaction txn);

    /**
     * Returns the transaction the cursor is currently linked to.
     */
    public Transaction link();

    /**
     * Returns an uncopied reference to the current key, or null if Cursor is
     * unpositioned. Array contents must not be modified.
     */
    public byte[] key();

    /**
     * Returns an uncopied reference to the current value, which might be null or {@link
     * #NOT_LOADED}. Array contents can be safely modified. Altering the value via the {@link
     * ValueAccessor} methods doesn't affect the object returned by this method.
     */
    public byte[] value();

    /**
     * By default, values are loaded automatically, as they are seen. When disabled, values
     * might need to be {@linkplain Cursor#load manually loaded}. When a {@link Transformer} is
     * used, the value might still be loaded automatically. When the value exists but hasn't
     * been loaded, the value field of the cursor is set to {@link #NOT_LOADED}.
     *
     * @param mode false to disable
     * @return prior autoload mode
     */
    public boolean autoload(boolean mode);

    /**
     * Returns the current autoload mode.
     */
    public boolean autoload();

    /**
     * Compare the current key to the one given.
     *
     * @param rkey key to compare to
     * @return a negative integer, zero, or a positive integer as current key
     * is less than, equal to, or greater than the rkey.
     * @throws NullPointerException if current key or rkey is null
     */
    public default int compareKeyTo(byte[] rkey) {
        byte[] lkey = key();
        return compareUnsigned(lkey, rkey);
    }

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
    public default int compareKeyTo(byte[] rkey, int offset, int length) {
        byte[] lkey = key();
        return compareUnsigned(lkey, 0, lkey.length, rkey, offset, offset + length);
    }

    /**
     * Attempt to register this cursor for direct redo operations, which can improve
     * replication performance when modifying a range of values. Without registration, replicas
     * must perform a full find operation for each modification made in the range.
     * Registration isn't useful when using the cursor for single updates.
     *
     * <p>The cursor is automatically {@linkplain #unregister unregistered} when {@linkplain
     * #reset reset}, or when moved to an undefined position, or when moving the cursor
     * non-incrementally. Methods whose name starts with "next", "previous", "skip", or
     * "findNearby" are considered to move the cursor incrementally. The use of these methods
     * generally indicates that registering the cursor might be beneficial.
     *
     * <p>To be effective, cursor registration must be performed <i>after</i> the cursor is
     * initially positioned:
     *
     * {@snippet lang="java" :
     * Cursor c = ...
     * c.findGe(startKey);
     * c.register(); // register after initial positioning
     * while (more updates to perform) {
     *     c.store(...);
     *     c.next(); // incremental move
     * }
     * }
     */
    public default boolean register() throws IOException {
        return false;
    }

    /**
     * Unregisters the cursor for direct redo operations.
     *
     * @see #register
     */
    public default void unregister() {
    }

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
     * than the given amount of entries are available, the Cursor is reset.
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
     * @throws UnpositionedCursorException if position is undefined at invocation time
     */
    public LockResult skip(long amount) throws IOException;

    /**
     * Moves the Cursor by a relative amount of entries, stopping sooner if the limit key is
     * reached. Pass a positive amount to skip forward, and pass a negative amount to skip
     * backwards. If the limit key is reached, or if less than the given amount of entries are
     * available, the Cursor is reset.
     *
     * <p>Skipping by 1 is equivalent to calling {@link #nextLe nextLe}, {@link #nextLt nextLt}
     * or {@link #next next}, depending on which type of limit was provided. Likewise, skipping
     * by -1 is equivalent to calling {@link #previousGe previousGe}, {@link #previousGt
     * previousGt} or {@link #previous previous}. A skip of 0 merely checks and returns the
     * lock state for the current key. Lock acquisition only applies to the target entry
     * &mdash; no locks are acquired for entries in between.
     *
     * @param limitKey limit key; pass null for no limit
     * @param inclusive true if limit is inclusive, false for exclusive
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws UnpositionedCursorException if position is undefined at invocation time
     */
    public default LockResult skip(long amount, byte[] limitKey, boolean inclusive)
        throws IOException
    {
        return ViewUtils.skip(this, amount, limitKey, inclusive);
    }

    /**
     * Moves to the Cursor to the next available entry. Cursor key and value
     * are set to null if no next entry exists, and position will be undefined.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws UnpositionedCursorException if position is undefined at invocation time
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
     * @throws UnpositionedCursorException if position is undefined at invocation time
     */
    public default LockResult nextLe(byte[] limitKey) throws IOException {
        return ViewUtils.nextCmp(this, limitKey, 1);
    }

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
     * @throws UnpositionedCursorException if position is undefined at invocation time
     */
    public default LockResult nextLt(byte[] limitKey) throws IOException {
        return ViewUtils.nextCmp(this, limitKey, 0);
    }

    /**
     * Moves to the Cursor to the previous available entry. Cursor key and
     * value are set to null if no previous entry exists, and position will be
     * undefined.
     *
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     * @throws UnpositionedCursorException if position is undefined at invocation time
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
     * @throws UnpositionedCursorException if position is undefined at invocation time
     */
    public default LockResult previousGe(byte[] limitKey) throws IOException {
        return ViewUtils.previousCmp(this, limitKey, -1);
    }

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
     * @throws UnpositionedCursorException if position is undefined at invocation time
     */
    public default LockResult previousGt(byte[] limitKey) throws IOException {
        return ViewUtils.previousCmp(this, limitKey, 0);
    }

    /**
     * Moves the Cursor to find the given key. If no such key exists, the cursor is still
     * positioned at the key, but the value is null.
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
     * Moves the Cursor to find the closest available entry greater than or equal to the given
     * key. If no such key exists, the cursor is unpositioned. Logically equivalent to {@link
     * java.util.NavigableMap#ceilingEntry(Object) NavigableMap.ceilingEntry}.
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
    public default LockResult findGe(byte[] key) throws IOException {
        LockResult result = find(key);
        if (value() == null) {
            if (result == LockResult.ACQUIRED) {
                link().unlock();
            }
            result = next();
        }
        return result;
    }

    /**
     * Moves the Cursor to find the closest available entry greater than the given key. If no
     * such key exists, the cursor is unpositioned. Logically equivalent to {@link
     * java.util.NavigableMap#higherEntry(Object) NavigableMap.higherEntry}.
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
    public default LockResult findGt(byte[] key) throws IOException {
        ViewUtils.findNoLock(this, key);
        return next();
    }

    /**
     * Moves the Cursor to find the closest available entry less than or equal to the given
     * key. If no such key exists, the cursor is unpositioned. Logically equivalent to {@link
     * java.util.NavigableMap#floorEntry(Object) NavigableMap.floorEntry}.
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
    public default LockResult findLe(byte[] key) throws IOException {
        LockResult result = find(key);
        if (value() == null) {
            if (result == LockResult.ACQUIRED) {
                link().unlock();
            }
            result = previous();
        }
        return result;
    }

    /**
     * Moves the Cursor to find the closest available entry less than the given key. If no such
     * key exists, the cursor is unpositioned. Logically equivalent to {@link
     * java.util.NavigableMap#lowerEntry(Object) NavigableMap.lowerEntry}.
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
    public default LockResult findLt(byte[] key) throws IOException {
        ViewUtils.findNoLock(this, key);
        return previous();
    }

    /**
     * Optimized version of the regular {@link #find find} method, which can perform fewer
     * search steps if the given key is in close proximity to the current one. Even if not in
     * close proximity, the find outcome is identical, although it may perform more slowly.
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
    public default LockResult findNearby(byte[] key) throws IOException {
        return find(key);
    }

    /**
     * Optimized version of the regular {@link #findGe findGe} method, which can perform fewer
     * search steps if the given key is in close proximity to the current one. Even if not in
     * close proximity, the find outcome is identical, although it may perform more slowly.
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
    public default LockResult findNearbyGe(byte[] key) throws IOException {
        LockResult result = findNearby(key);
        if (value() == null) {
            if (result == LockResult.ACQUIRED) {
                link().unlock();
            }
            result = next();
        }
        return result;
    }

    /**
     * Optimized version of the regular {@link #findGt findGt} method, which can perform fewer
     * search steps if the given key is in close proximity to the current one. Even if not in
     * close proximity, the find outcome is identical, although it may perform more slowly.
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
    public default LockResult findNearbyGt(byte[] key) throws IOException {
        ViewUtils.findNearbyNoLock(this, key);
        return next();
    }

    /**
     * Optimized version of the regular {@link #findLe findLe} method, which can perform fewer
     * search steps if the given key is in close proximity to the current one. Even if not in
     * close proximity, the find outcome is identical, although it may perform more slowly.
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
    public default LockResult findNearbyLe(byte[] key) throws IOException {
        LockResult result = findNearby(key);
        if (value() == null) {
            if (result == LockResult.ACQUIRED) {
                link().unlock();
            }
            result = previous();
        }
        return result;
    }

    /**
     * Optimized version of the regular {@link #findLt findLt} method, which can perform fewer
     * search steps if the given key is in close proximity to the current one. Even if not in
     * close proximity, the find outcome is identical, although it may perform more slowly.
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
    public default LockResult findNearbyLt(byte[] key) throws IOException {
        ViewUtils.findNearbyNoLock(this, key);
        return previous();
    }

    /**
     * Moves the Cursor to a random entry, but not guaranteed to be chosen from a uniform
     * distribution. If no entries exists, or if random searches aren't supported, the cursor
     * is unpositioned.
     *
     * @param lowKey inclusive lowest key in the selectable range; pass null for open range
     * @param highKey exclusive highest key in the selectable range; pass null for open range
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    public default LockResult random(byte[] lowKey, byte[] highKey) throws IOException {
        return random(lowKey, true, highKey, false);
    }

    /**
     * Moves the Cursor to a random entry, but not guaranteed to be chosen from a uniform
     * distribution. If no entries exists, or if random searches aren't supported, the cursor
     * is unpositioned.
     *
     * @param lowKey lowest key in the selectable range; pass null for open range
     * @param lowInclusive true for inclusive key, false for exclusive, ignored if key is null
     * @param highKey highest key in the selectable range; pass null for open range
     * @param highInclusive true for inclusive key, false for exclusive, ignored if key is null
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    public LockResult random(byte[] lowKey, boolean lowInclusive,
                             byte[] highKey, boolean highInclusive) throws IOException;

    /**
     * Quickly check if the value still exists without locking it.
     *
     * @throws UnpositionedCursorException if position is undefined at invocation time
     */
    public boolean exists() throws IOException;

    /**
     * Locks the current entry, as if by calling load. Locking is performed automatically
     * within transactions, and so invocation of this method is necessary only when manually
     * tweaking the lock mode. If a lock was acquired (even if not retained), the cursor value
     * field is updated according to the current autoload mode.
     *
     * <p>By default, this method simply calls load. Subclasses are encouraged to provide a
     * more efficient implementation.
     *
     * @throws UnpositionedCursorException if position is undefined at invocation time
     * @return {@link LockResult#UNOWNED UNOWNED}, {@link LockResult#ACQUIRED
     * ACQUIRED}, {@link LockResult#OWNED_SHARED OWNED_SHARED}, {@link
     * LockResult#OWNED_UPGRADABLE OWNED_UPGRADABLE}, or {@link
     * LockResult#OWNED_EXCLUSIVE OWNED_EXCLUSIVE}
     */
    public default LockResult lock() throws IOException {
        return load();
    }

    /**
     * Loads or reloads the value at the cursor's current position. Cursor value is set to null
     * if entry no longer exists, but the position remains unmodified.
     *
     * @throws UnpositionedCursorException if position is undefined at invocation time
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
     * @throws UnpositionedCursorException if position is undefined at invocation time
     * @throws ViewConstraintException if value is not permitted
     */
    public void store(byte[] value) throws IOException;

    /**
     * Deletes the value for the current entry, leaving the position unchanged. Unless an
     * exception is thrown, the object returned by the {@link #value value} method will be null
     * after calling this method.
     *
     * @throws UnpositionedCursorException if position is undefined at invocation time
     * @throws ViewConstraintException if not permitted
     */
    public default void delete() throws IOException {
        store(null);
    }

    /**
     * Combined store and commit to the linked transaction. Although similar to storing and
     * committing explicitly, additional optimizations can be applied. In particular, no undo
     * log entry is required when committing the outermost transaction scope. This is the same
     * optimization used by null transactions (auto-commit).
     *
     * @param value value to store; pass null to delete
     * @throws UnpositionedCursorException if position is undefined at invocation time
     * @throws ViewConstraintException if value is not permitted
     * @see CommitCallback
     */
    public default void commit(byte[] value) throws IOException {
        ViewUtils.commit(this, value);
    }

    /**
     * Returns a new independent Cursor, positioned where this one is, and
     * linked to the same transaction. The original and copied Cursor can be
     * acted upon without affecting each other's state.
     */
    public Cursor copy();

    /**
     * Resets the Cursor and moves it to an undefined position. The key and value references
     * are set to null.
     */
    public void reset();

    /**
     * Equivalent to the reset method, which moves the Cursor to an undefined position. The
     * Cursor is re-opened automatically if positioned again.
     */
    @Override
    public default void close() throws IOException {
        reset();
    }
}
