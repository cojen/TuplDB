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

import java.io.IOException;

/**
 * Represents an operation which combines two values that map to the same key.
 *
 * @author Brian S O'Neill
 * @see View#viewUnion viewUnion
 * @see View#viewIntersection viewIntersection
 * @see View#viewDifference viewDifference
 */
@FunctionalInterface
public interface Combiner {
    /**
     * Returns a Combiner that chooses the first value and discards the second value.
     */
    public static Combiner first() {
        return SelectCombiner.First.THE;
    }

    /**
     * Returns a Combiner that chooses the second value and discards the first value.
     */
    public static Combiner second() {
        return SelectCombiner.Second.THE;
    }

    /**
     * Returns a Combiner that discards both values (always returns null). When used with a
     * {@link View#viewUnion union}, this causes it to compute the <i>symmetric set difference</i>.
     */
    public static Combiner discard() {
        return SelectCombiner.Discard.THE;
    }

    /**
     * Return a combined value derived from the given key and value pair. Null can be returned
     * to completely discard both values.
     *
     * @param key non-null associated key
     * @param first non-null first value in the pair
     * @param second non-null second value in the pair
     * @return combined value or null to discard both values
     */
    public byte[] combine(byte[] key, byte[] first, byte[] second) throws IOException;

    /**
     * Returns true by default, indicating that the combine method always requires loaded value
     * instances to be provided.
     *
     * @return true if loaded values must always be passed into the combine method
     */
    public default boolean requireValues() {
        return true;
    }

    /**
     * Returns false by default, indicating that when loads of the first key acquire a lock, it
     * doesn't need to be held while a lock on the second key is acquired. This option is
     * applicable when using the {@link LockMode#READ_COMMITTED READ_COMMITTED} lock mode or a
     * null transaction. When storing into a view, acquired locks are always combined.
     */
    public default boolean combineLocks() {
        return false;
    }

    /**
     * If {@link #requireValues requireValues} always returns false, consider overriding this
     * method and implement a more efficient load for two views in a union.
     */
    public default byte[] loadUnion(Transaction txn, byte[] key, View first, View second)
        throws IOException
    {
        byte[] v1 = first.load(txn, key);
        byte[] v2 = second.load(txn, key);
        return v1 == null ? v2 : (v2 == null ? v1 : combine(key, v1, v2));
    }

    /**
     * If {@link #requireValues requireValues} always returns false, consider overriding this
     * method and implement a more efficient load for two views in an intersection.
     */
    public default byte[] loadIntersection(Transaction txn, byte[] key, View first, View second)
        throws IOException
    {
        byte[] v1 = first.load(txn, key);
        if (v1 == null) {
            // Always need to lock the second entry too, for consistency and to avoid any odd
            // deadlocks if the store method is called.
            second.touch(txn, key);
            return null;
        }
        byte[] v2 = second.load(txn, key);
        return v2 == null ? null : combine(key, v1, v2);
    }

    /**
     * If {@link #requireValues requireValues} always returns false, consider overriding this
     * method and implement a more efficient load for two views in a difference.
     */
    public default byte[] loadDifference(Transaction txn, byte[] key, View first, View second)
        throws IOException
    {
        byte[] v1 = first.load(txn, key);
        if (v1 == null) {
            // Always need to lock the second entry too, for consistency and to avoid any odd
            // deadlocks if the store method is called.
            second.touch(txn, key);
            return null;
        }
        byte[] v2 = second.load(txn, key);
        return v2 == null ? v1 : combine(key, v1, v2);
    }

    /**
     * Separates a combined result, for use when storing a value into a view which uses this
     * combiner. Returns null by default, which then causes a {@link ViewConstraintException}
     * to be thrown.
     *
     * @param key non-null associated key
     * @param value non-null combined value
     * @return first and second value, neither of which can be null
     */
    public default byte[][] separate(byte[] key, byte[] value) throws IOException {
        return null;
    }
}
