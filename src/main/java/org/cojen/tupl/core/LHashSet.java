/*
 *  Copyright (C) 2024 Cojen.org
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

/**
 * Simple set of long values backed by a hash table. The values are internally scrambled to
 * reduce hash collisions.
 *
 * @author Brian S. O'Neill
 */
class LHashSet {
    private final LHashTable.Obj<Object> mTable;

    LHashSet(int capacity) {
        mTable = new LHashTable.Obj<Object>(capacity);
    }

    final int size() {
        return mTable.size();
    }

    final boolean contains(long v) {
        return mTable.get(scramble(v)) != null;
    }

    final boolean add(long v) {
        return mTable.insert(scramble(v)) != null;
    }

    final boolean remove(long v) {
        return mTable.remove(scramble(v)) != null;
    }

    final void addAll(LHashSet other) {
        other.mTable.traverse(e -> {
            mTable.insert(e.key);
            return false;
        });
    }

    final <X extends Exception> void traverse(Visitor<X> v) throws X {
        mTable.traverse(e -> v.visit(unscramble(e.key)));
    }

    @FunctionalInterface
    static interface Visitor<X extends Exception> {
        /**
         * @return true if entry should be deleted
         */
        boolean visit(long v) throws X;
    }

    protected long scramble(long v) {
        return Utils.scramble(v);
    }

    protected long unscramble(long v) {
        return Utils.unscramble(v);
    }
}
