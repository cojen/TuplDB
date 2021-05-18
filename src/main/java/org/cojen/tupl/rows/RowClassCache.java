/*
 *  Copyright 2021 Cojen.org
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

package org.cojen.tupl.rows;

/**
 * Caches generated classes derived from RowInfo state.
 *
 * @author Brian S O'Neill
 */
abstract class RowClassCache extends WeakCache<Class<?>, Class<?>> {
    Class<?> find(Class<?> rowType) {
        Class clazz = get(rowType);
        if (clazz != null) {
            return clazz;
        }
        synchronized (this) {
            clazz = get(rowType);
            if (clazz != null) {
                return clazz;
            }
        }

        RowGen rowGen = RowInfo.find(rowType).rowGen();

        synchronized (this) {
            clazz = get(rowType);
            if (clazz == null) {
                clazz = generate(rowType, rowGen);
                put(rowType, clazz);
            }
            return clazz;
        }
    }

    protected abstract Class<?> generate(Class<?> rowType, RowGen rowGen);
}
