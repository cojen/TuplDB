/*
 *  Copyright (C) 2017 Cojen.org
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
 * 
 *
 * @author Brian S O'Neill
 */
abstract class SelectCombiner implements Combiner {
    static final class First extends SelectCombiner {
        static final Combiner THE = new First();

        @Override
        public byte[] combine(byte[] key, byte[] first, byte[] second) {
            return first;
        }

        @Override
        public byte[] loadUnion(Transaction txn, byte[] key, View first, View second)
            throws IOException
        {
            byte[] v1 = first.load(txn, key);
            if (v1 == null) {
                return second.load(txn, key);
            } else {
                // Always need to lock the second entry too, for consistency and to avoid any
                // odd deadlocks if the store method is called.
                second.touch(txn, key);
                return v1;
            }
        }

        @Override
        public byte[] loadIntersection(Transaction txn, byte[] key, View first, View second)
            throws IOException
        {
            byte[] v1 = first.load(txn, key);
            if (v1 == null) {
                // Always need to lock the second entry too, for consistency and to avoid any odd
                // deadlocks if the store method is called.
                second.touch(txn, key);
                return null;
            }
            return second.exists(txn, key) ? v1 : null;
        }

        @Override
        public byte[] loadDifference(Transaction txn, byte[] key, View first, View second)
            throws IOException
        {
            byte[] v1 = first.load(txn, key);
            if (v1 == null) {
                // Always need to lock the second entry too, for consistency and to avoid any odd
                // deadlocks if the store method is called.
                second.touch(txn, key);
                return null;
            }
            return v1;
        }
    };

    static final class Second extends SelectCombiner {
        static final Combiner THE = new Second();

        @Override
        public byte[] combine(byte[] key, byte[] first, byte[] second) {
            return second;
        }

        @Override
        public byte[] loadUnion(Transaction txn, byte[] key, View first, View second)
            throws IOException
        {
            // Lock the first, for consistency.
            first.touch(txn, key);
            byte[] v2 = second.load(txn, key);
            return v2 == null ? first.load(txn, key) : v2;
        }

        @Override
        public byte[] loadIntersection(Transaction txn, byte[] key, View first, View second)
            throws IOException
        {
            // Lock the first, for consistency.
            first.touch(txn, key);
            byte[] v2 = second.load(txn, key);
            return v2 == null ? null : first.exists(txn, key) ? v2 : null;
        }
    };

    static final class Discard extends SelectCombiner {
        static final Combiner THE = new Discard();

        @Override
        public byte[] combine(byte[] key, byte[] first, byte[] second) {
            return null;
        }

        @Override
        public byte[] loadUnion(Transaction txn, byte[] key, View first, View second)
            throws IOException
        {
            byte[] v1 = first.load(txn, key);
            if (v1 == null) {
                return second.load(txn, key);
            } else {
                return second.exists(txn, key) ? null : v1;
            }
        }

        @Override
        public byte[] loadIntersection(Transaction txn, byte[] key, View first, View second)
            throws IOException
        {
            // Must always lock the keys.
            first.touch(txn, key);
            second.touch(txn, key);
            return null;
        }

        @Override
        public byte[] loadDifference(Transaction txn, byte[] key, View first, View second)
            throws IOException
        {
            byte[] v1 = first.load(txn, key);
            if (v1 == null) {
                // Always need to lock the second entry too, for consistency and to avoid any odd
                // deadlocks if the store method is called.
                second.touch(txn, key);
                return null;
            }
            return second.exists(txn, key) ? null : v1;
        }
    };

    @Override
    public boolean requireValues() {
        return false;
    }
}
