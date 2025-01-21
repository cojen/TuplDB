/*
 *  Copyright (C) 2025 Cojen.org
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

import java.io.IOException;

import org.cojen.tupl.Query;
import org.cojen.tupl.Table;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.table.expr.CompiledQuery;
import org.cojen.tupl.table.expr.Parser;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class ConcatTable<R> extends MultiSourceTable<R> {
    private static final WeakCache<TupleKey, ConcatTable, Object> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            @SuppressWarnings("unchecked")
            public ConcatTable newValue(TupleKey key, Object unused) {
                var targetType = (Class) key.get(0);
                var sources = (Table[]) key.get(1);
                try {
                    return doConcat(targetType, sources);
                } catch (IOException e) {
                    throw RowUtils.rethrow(e);
                }
            }
        };
    }

    /**
     * @param sources must have at least one element
     */
    @SuppressWarnings("unchecked")
    public static <R> ConcatTable<R> concat(Class<R> targetType, Table<?>... sources)
        throws IOException
    {
        sources = sources.clone();
        return cCache.obtain(TupleKey.make.with(targetType, sources), null);
    }

    /**
     * @param sources must have at least one element
     */
    @SuppressWarnings("unchecked")
    private static <R> ConcatTable<R> doConcat(Class<R> targetType, Table<?>... sources)
        throws IOException
    {
        if (sources.length == 0) {
            throw new IllegalArgumentException();
        }

        int num = 0;
        for (var source : sources) {
            if (source instanceof ConcatTable ct && ct.rowType() == targetType) {
                num += ct.mSources.length;
            } else {
                num++;
            }
        }

        Table<?>[] allSources = new Table[num];
        int allPos = 0;

        for (int i=0; i<sources.length; i++) {
            Table<?> source = sources[i];
            if (source instanceof ConcatTable ct && ct.rowType() == targetType) {
                Table<?>[] subSources = ct.mSources;
                System.arraycopy(subSources, 0, allSources, allPos, subSources.length);
                allPos += subSources.length;
            } else {
                allSources[allPos++] = source.map(targetType);
            }
        }

        assert allPos == allSources.length;

        return new ConcatTable(allSources);
    }

    /**
     * @param sources must have at least one element
     */
    private ConcatTable(Table<R>[] sources) {
        super(sources);
    }

    @Override
    public boolean hasPrimaryKey() {
        return false;
    }

    @Override
    public Table<R> distinct() throws IOException {
        return AggregatedTable.distinct(this);
    }

    @Override // MultiCache
    protected Object cacheNewValue(Type type, Object key, Object helper) throws IOException {
        if (type == TYPE_2) { // see the inherited derive method
            return CompiledQuery.makeDerived(this, type, key, helper);
        }

        if (type != TYPE_1) { // see the inherited query method
            throw new AssertionError();
        }

        var queryStr = (String) key;

        String orderBy = Parser.parse(this, rowType(), queryStr).orderBySpec();

        @SuppressWarnings("unchecked") Query<R>[] queries = new Query[mSources.length];
        for (int i=0; i<queries.length; i++) {
            queries[i] = mSources[i].query(queryStr);
        }

        if (orderBy.isEmpty()) {
            return new ConcatQuery<R>(queries);
        } else {
            return new MergeQuery<R>(comparator(orderBy), queries);
        }
    }
}
