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

package org.cojen.tupl.rows;

import java.util.Arrays;
import java.util.Objects;

/**
 * If only arrays behaved like this already...
 *
 * @author Brian S O'Neill
 */
abstract class ArrayKey {
    static Bytes make(byte[] array) {
        return new Bytes(array);
    }

    static PrefixBytes make(int prefix, byte[] array) {
        return new PrefixBytes(prefix, array);
    }

    static ObjPrefixBytes make(Object prefix, byte[] array) {
        return new ObjPrefixBytes(prefix, array);
    }

    static Obj make(Object first, Object[] rest) {
        var array = new Object[1 + rest.length];
        array[0] = first;
        System.arraycopy(rest, 0, array, 1, rest.length);
        return new Obj(array);
    }

    static final class Bytes {
        final byte[] array;

        Bytes(byte[] array) {
            this.array = array;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj || obj instanceof Bytes other
                && Arrays.equals(array, other.array);
        }

        @Override
        public String toString() {
            return Arrays.toString(array);
        }
    }

    static final class PrefixBytes {
        final int prefix;
        final byte[] array;

        PrefixBytes(int prefix, byte[] array) {
            this.prefix = prefix;
            this.array = array;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(array) * 31 + prefix;
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj || obj instanceof PrefixBytes other
                && prefix == other.prefix && Arrays.equals(array, other.array);
        }

        @Override
        public String toString() {
            return prefix + ", " + Arrays.toString(array);
        }
    }

    static final class ObjPrefixBytes {
        final Object prefix;
        final byte[] array;

        ObjPrefixBytes(Object prefix, byte[] array) {
            this.prefix = prefix;
            this.array = array;
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(prefix) * 31 + Arrays.hashCode(array);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj || obj instanceof ObjPrefixBytes other
                && Objects.equals(prefix, other.prefix) && Arrays.equals(array, other.array);
        }

        @Override
        public String toString() {
            return prefix + ", " + Arrays.toString(array);
        }
    }

    static final class Obj {
        final Object[] array;

        Obj(Object[] array) {
            this.array = array;
        }

        @Override
        public int hashCode() {
            return Arrays.deepHashCode(array);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj || obj instanceof Obj other
                && Arrays.deepEquals(array, other.array);
        }

        @Override
        public String toString() {
            return Arrays.deepToString(array);
        }
    }
}
