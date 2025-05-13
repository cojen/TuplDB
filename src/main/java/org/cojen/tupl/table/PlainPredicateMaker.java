/*
 *  Copyright (C) 2023 Cojen.org
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

package org.cojen.tupl.table;

import java.lang.invoke.MethodHandle;

import java.util.function.Predicate;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.table.filter.FalseFilter;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;

/**
 * Generates plain Predicate instances which check that columns are set.
 *
 * @see org.cojen.tupl.Table#predicate
 * @see RowPredicateMaker
 * @author Brian S O'Neill
 */
public final class PlainPredicateMaker {
    private static final WeakCache<TupleKey, MethodHandle, RowFilter> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            public MethodHandle newValue(TupleKey key, RowFilter filter) {
                var rowType = (Class<?>) key.get(0);
                String query = key.getString(1);
                if (filter == null) {
                    filter = StoredTable.parseFilter(rowType, query);
                }
                String filterStr = filter.toString();
                if (filterStr.equals(query)
                    || filter == TrueFilter.THE || filter == FalseFilter.THE)
                {
                    RowInfo info = RowInfo.find(rowType);
                    var maker = new RowPredicateMaker(rowType, info.rowGen(), filter, filterStr);
                    return maker.finishPlain();
                } else {
                    return obtain(TupleKey.make.with(rowType, filterStr), null);
                }
            }
        };
    }

    /**
     * Returns a new predicate instance.
     */
    @SuppressWarnings("unchecked")
    public static <R> Predicate<R> predicate(Class<R> rowType, String query, Object... args) {
        try {
            return (Predicate<R>) predicateHandle(rowType, query).invokeExact(args);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    /**
     * Returns a new predicate instance.
     */
    @SuppressWarnings("unchecked")
    public static <R> Predicate<R> predicate(Class<R> rowType, RowFilter filter, Object... args) {
        try {
            return (Predicate<R>) predicateHandle(rowType, filter).invokeExact(args);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    /**
     * MethodHandle signature: Predicate xxx(Object... args)
     */
    static MethodHandle predicateHandle(Class<?> rowType, String query) {
        return cCache.obtain(TupleKey.make.with(rowType, query), null);
    }

    /**
     * MethodHandle signature: Predicate xxx(Object... args)
     */
    static MethodHandle predicateHandle(Class<?> rowType, RowFilter filter) {
        return cCache.obtain(TupleKey.make.with(rowType, filter.toString()), filter);
    }
}
