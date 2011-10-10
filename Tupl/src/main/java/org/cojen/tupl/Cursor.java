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

/**
 * Maintains a logical position in an {@link Index}. Cursors must be {@link
 * #reset reset} when no longer needed to free up memory. Although not
 * necessarily practical, multiple threads may safely interact with Cursor
 * instances.
 *
 * @author Brian S O'Neill
 */
public interface Cursor {
    /**
     * Returns a copy of the key and value at the Cursor's current position. If
     * entry doesn't exist, value is assigned null and false is returned. A
     * non-null key is always available, even for entries which don't exist.
     *
     * @param entry entry to fill in; pass null to just check if entry exists
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public boolean get(Entry entry) throws IOException;

    // FIXME: Remove all the move methods which don't accept an entry.

    /**
     * Moves the Cursor to find the first available entry, unless none exists.
     *
     * @return false if no entries exist and position is now undefined
     */
    public boolean first() throws IOException;

    /**
     * Moves the Cursor to find the first available entry, unless none exists.
     *
     * @param entry optional entry to fill in
     * @return false if no entries exist and position is now undefined
     */
    public boolean first(Entry entry) throws IOException;

    /**
     * Moves the Cursor to find the last available entry, unless none exists.
     *
     * @return false if no entries exist and position is now undefined
     */
    public boolean last() throws IOException;

    /**
     * Moves the Cursor to find the last available entry, unless none exists.
     *
     * @param entry optional entry to fill in
     * @return false if no entries exist and position is now undefined
     */
    public boolean last(Entry entry) throws IOException;

    /**
     * Moves the Cursor by a relative amount of entries. Pass a positive amount
     * for forward movement, and pass a negative amount for reverse
     * movement. The actual movement amount can be less than the requested
     * amount if the start or end is reached. After this happens, the position
     * is undefined.
     *
     * @return actual amount moved; if less, position is now undefined
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public long move(long amount) throws IOException;

    /**
     * Moves the Cursor by a relative amount of entries. Pass a positive amount
     * for forward movement, and pass a negative amount for reverse
     * movement. The actual movement amount can be less than the requested
     * amount if the start or end is reached. After this happens, the position
     * is undefined.
     *
     * @param entry optional entry to fill in
     * @return actual amount moved; if less, position is now undefined
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public long move(long amount, Entry entry) throws IOException;

    /**
     * Advances to the Cursor to the next available entry, unless none
     * exists. Equivalent to:
     *
     * <pre>
     * return cursor.move(1) != 0;</pre>
     *
     * @return false if no next entry and position is now undefined
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public boolean next() throws IOException;

    /**
     * Advances to the Cursor to the next available entry, unless none
     * exists. Equivalent to:
     *
     * <pre>
     * return cursor.move(1, entry) != 0;</pre>
     *
     * @param entry optional entry to fill in
     * @return false if no next entry and position is now undefined
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public boolean next(Entry entry) throws IOException;

    /**
     * Advances to the Cursor to the previous available entry, unless none
     * exists. Equivalent to:
     *
     * <pre>
     * return cursor.move(-1) != 0;</pre>
     *
     * @return false if no previous entry and position is now undefined
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public boolean previous() throws IOException;

    /**
     * Advances to the Cursor to the previous available entry, unless none
     * exists. Equivalent to:
     *
     * <pre>
     * return cursor.move(-1, entry) != 0;</pre>
     *
     * @param entry optional entry to fill in
     * @return false if no previous entry and position is now undefined
     * @throws IllegalStateException if position is undefined at invocation time
     */
    public boolean previous(Entry entry) throws IOException;

    /**
     * Moves the Cursor to find the given key, returning true if a matching
     * entry exists. If false is returned, an uncopied reference to the key is
     * retained. The key reference is released when the Cursor position changes
     * or a matching entry is created.
     *
     * @return false if entry not found
     * @throws NullPointerException if key is null
     */
    public boolean find(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry greater than or equal
     * to the given key. Equivalent to:
     *
     * <pre>
     * return cursor.find(key) ? true : cursor.next();</pre>
     *
     * @return false if entry not found and position is now undefined
     * @throws NullPointerException if key is null
     */
    public boolean findGe(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry greater than the
     * given key. Equivalent to:
     *
     * <pre>
     * cursor.find(key); return cursor.next();</pre>
     *
     * @return false if entry not found and position is now undefined
     * @throws NullPointerException if key is null
     */
    public boolean findGt(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry less than or equal to
     * the given key. Equivalent to:
     *
     * <pre>
     * return cursor.find(key) ? true : cursor.previous();</pre>
     *
     * @return false if entry not found and position is now undefined
     * @throws NullPointerException if key is null
     */
    public boolean findLe(byte[] key) throws IOException;

    /**
     * Moves the Cursor to find the first available entry less than the given
     * key. Equivalent to:
     *
     * <pre>
     * cursor.find(key); return cursor.previous();</pre>
     *
     * @return false if entry not found and position is now undefined
     * @throws NullPointerException if key is null
     */
    public boolean findLt(byte[] key) throws IOException;

    /**
     * Optimized version of the regular find method, which can perform fewer
     * search steps if the given key is in close proximity to the current one.
     * Even if not in close proximity, the find behavior is still identicial,
     * although it may perform more slowly.
     *
     * @return false if entry not found
     * @throws NullPointerException if key is null
     */
    public boolean findNearby(byte[] key) throws IOException;

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
     * Returns true if Cursor is currently at a defined position.
     */
    public boolean isPositioned();

    /**
     * Resets the Cursor position to be undefined.
     */
    public void reset();

}
