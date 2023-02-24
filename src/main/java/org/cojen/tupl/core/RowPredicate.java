/*
 *  Copyright (C) 2021 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import java.util.function.Predicate;

/**
 * Defines an interface intended for locking rows based on a rule that matches on a row.
 *
 * @author Brian S O'Neill
 * @see RowPredicateLock
 */
public interface RowPredicate<R> extends Predicate<R> {
    /**
     * Test against a completely filled in row.
     */
    @Override
    public boolean test(R row);

    /**
     * Test against a partially filled in row, only considering dirty columns. Additional
     * columns are decoded as necessary from the given key and value.
     */
    public default boolean testP(R row, byte[] key, byte[] value) {
        return test(key, value);
    }

    /**
     * Test against an encoded key and value. All columns must be decoded as necessary.
     */
    public boolean test(byte[] key, byte[] value);

    /**
     * Determine if a lock held against the given key matches the row predicate. This variant
     * is called for transactions which were created before the predicate lock. When undecided,
     * this method should return true.
     */
    public default boolean test(byte[] key) {
        return true;
    }

    /**
     * Returns a predicate that always evaluates to true.
     */
    @SuppressWarnings("unchecked")
    public static <R> RowPredicate<R> all() {
        return All.THE;
    }

    /**
     * Defines a predicate that always evaluates to false.
     */
    public static class None<R> implements RowPredicate<R> {
        @Override
        public final boolean test(R row) {
            return false;
        }

        @Override
        public final boolean test(byte[] key, byte[] value) {
            return false;
        }

        @Override
        public final boolean test(byte[] key) {
            return false;
        }
    }

    static final class All implements RowPredicate {
        static final All THE = new All();

        @Override
        public boolean test(Object row) {
            return true;
        }

        @Override
        public boolean test(byte[] key, byte[] value) {
            return true;
        }
    }
}
