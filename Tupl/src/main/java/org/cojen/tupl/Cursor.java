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
 *
 *
 * @author Brian S O'Neill
 */
public interface Cursor {
    /**
     * Returns the key at the cursor's position. An empty key is the least
     * possible, and a null key is higher than the greatest possible. No value
     * can be mapped to a null key.
     *
     * <p>cost: O(1)
     *
     * @throws IllegalStateException if key position is undefined
     */
    byte[] getKey() throws IOException;

    /**
     * Returns the value at the cursor's position. Null is returned for
     * non-existant entries.
     *
     * <p>cost: O(1)
     *
     * @throws IllegalStateException if key position is undefined
     */
    byte[] getValue() throws IOException;

    /**
     * Set the value at the cursor's position. A null value deletes the entry.
     *
     * <p>cost: O(1)
     *
     * @throws IllegalStateException if key position is undefined or if key is null
     */
    void setValue(byte[] value) throws IOException;

    /**
     * Move the cursor to find the given key, returning true if it exists. An
     * empty key moves to the first possible entry, and a null key moves past
     * the greatest possible key. No value can be mapped to a null key.
     *
     * <p>cost: O(log n)
     */
    boolean find(byte[] key) throws IOException;

    /**
     * Move the cursor by a relative amount of entries, which may be less if
     * not enough entries exist. Pass a positive amount for forward movement,
     * and pass a negative amount for reverse movement.
     *
     * <p>cost: O(log n)
     *
     * @return actual amount moved
     * @throws IllegalStateException if key position is undefined
     */
    long move(long amount) throws IOException;

    /**
     * Resets the cursor position to be undefined.
     *
     * <p>cost: O(1)
     */
    void reset();

    // All remaining methods can be derived from the core methods.

    /**
     * Advances to the cursor to the next available entry, unless none
     * exists. If false is returned, the current key remains unchanged.
     *
     * <pre>
     * return move(1) != 0;
     * </pre>
     *
     * <p>cost: O(1)
     *
     * @throws IllegalStateException if key position is undefined
     */
    boolean next() throws IOException;

    /**
     * Advances to the cursor to the previous available entry, unless none
     * exists. If false is returned, the current key remains unchanged.
     *
     * <pre>
     * return move(-1) != 0;
     * </pre>
     *
     * <p>cost: O(1)
     *
     * @throws IllegalStateException if key position is undefined
     */
    boolean previous() throws IOException;

    /**
     * Move the cursor to find the first available entry greater than or equal
     * to the given key. If false is returned, the cursor is at positioned at
     * the given key.
     *
     * <pre>
     * return find(key) ? true : next();
     * </pre>
     */
    boolean findGe(byte[] key) throws IOException;

    /**
     * Move the cursor to find the first available entry less than or equal to
     * the given key. If false is returned, the cursor is at positioned at the
     * given key.
     *
     * <pre>
     * return find(key) ? true : previous();
     * </pre>
     *
     * <p>cost: O(log n)
     */
    boolean findLe(byte[] key) throws IOException;

    /**
     * Move the cursor to find the first available entry less than the given
     * key. If false is returned, the cursor is at positioned at the given key.
     *
     * <pre>
     * find(key); return previous();
     * </pre>
     *
     * <p>cost: O(log n)
     */
    boolean findLt(byte[] key) throws IOException;

    /**
     * Move the cursor to find the first available entry greater than the given
     * key. If false is returned, the cursor is at positioned at the given key.
     *
     * <pre>
     * find(key); return next();
     * </pre>
     *
     * <p>cost: O(log n)
     */
    boolean findGt(byte[] key) throws IOException;

    /**
     * Move the cursor to find the first available entry, unless none
     * exists. If false is returned, the current key is empty.
     *
     * <pre>
     * return findGe(new byte[0]);
     * </pre>
     *
     * <p>cost: O(log n)
     */
    boolean first() throws IOException;

    /**
     * Move the cursor to find the last available entry, unless none exists. If
     * false is returned, the current key is null.
     *
     * <pre>
     * return findLt(null);
     * </pre>
     *
     * <p>cost: O(log n)
     */
    boolean last() throws IOException;
}
