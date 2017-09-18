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

package org.cojen.tupl.repl;

/**
 * 
 *
 * @author Brian S O'Neill
 */
interface LKey<T extends LKey> extends Comparable<LKey<T>> {
    long key();

    @Override
    public default int compareTo(LKey<T> other) {
        return Long.compare(key(), other.key());
    }

    static class Finder<T extends LKey> implements LKey<T> {
        private final long mKey;

        Finder(long key) {
            mKey = key;
        }

        @Override
        public long key() {
            return mKey;
        }
    }
}
