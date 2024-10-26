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

package org.cojen.tupl.table.expr;

import java.io.IOException;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.cojen.tupl.QueryException;
import org.cojen.tupl.Table;

import org.cojen.tupl.table.RowUtils;
import org.cojen.tupl.table.ViewedTable;

import org.cojen.tupl.table.filter.ColumnToConstantFilter;
import org.cojen.tupl.table.filter.FalseFilter;
import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;

/**
 * Defines a QueryExpr which doesn't need a Mapper.
 *
 * @author Brian S. O'Neill
 * @see QueryExpr#make
 */
final class UnmappedQueryExpr extends QueryExpr {
    /**
     * Returns an UnmappedQueryExpr if necessary. If nothing is filtered, or if all columns are
     * projected, then the original from expression is returned.
     *
     * @param projection must only consist of wrapped ColumnExpr instances; can be null to
     * project all columns
     * @see QueryExpr#make
     */
    static RelationExpr make(int startPos, int endPos,
                             RelationExpr from, RowFilter rowFilter, List<ProjExpr> projection,
                             int maxArgument)
    {
        RelationType type = from.type();

        if (projection != null && type.rowType().isFullProjection(projection)) {
            projection = null;
        }

        if (rowFilter == TrueFilter.THE && projection == null) {
            return from;
        }

        Map<Object, Integer> argMap;

        if (rowFilter == TrueFilter.THE || rowFilter == FalseFilter.THE) {
            argMap = null;
        } else {
            final var fArgMap = new LinkedHashMap<Object, Integer>();

            rowFilter = rowFilter.constantsToArguments((ColumnToConstantFilter f) -> {
                Object value = ((ConstantExpr) f.constant()).value();
                return fArgMap.computeIfAbsent(value, k -> maxArgument + fArgMap.size() + 1);
            });

            int size = fArgMap.size();

            if (size == 0) {
                argMap = null;
            } else {
                argMap = fArgMap;
            }
        }

        type = type.withCardinality(type.cardinality().filter(rowFilter));

        return new UnmappedQueryExpr
            (startPos, endPos, type, from, rowFilter, projection, maxArgument, argMap);
    }

    private final Map<Object, Integer> mArgMap;

    private UnmappedQueryExpr(int startPos, int endPos, RelationType type,
                              RelationExpr from, RowFilter filter, List<ProjExpr> projection,
                              int maxArgument, Map<Object, Integer> argMap)
    {
        super(startPos, endPos, type, from, filter, projection, maxArgument);
        mArgMap = argMap;
    }

    @Override
    public QuerySpec querySpec() {
        if (!(mFrom instanceof TableExpr)) {
            throw new QueryException("Query has an intermediate transform step");
        }
        if (mArgMap != null) {
            throw new QueryException("Query has literals");
        }
        return super.querySpec(false);
    }

    @Override
    public QuerySpec tryQuerySpec(Class<?> rowType) {
        if (mFrom.rowTypeClass() == rowType && mFrom instanceof TableExpr && mArgMap == null) {
            return super.querySpec(false);
        }
        return null;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            super.doEncodeKey(enc);

            if (mArgMap == null) {
                enc.encodeUnsignedVarInt(0);
            } else {
                enc.encodeUnsignedVarInt(mArgMap.size() + 1);
                for (Map.Entry<Object, Integer> e : mArgMap.entrySet()) {
                    enc.encodeReference(e.getKey());
                    enc.encodeInt(e.getValue());
                }
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public CompiledQuery<?> makeCompiledQuery() throws IOException {
        CompiledQuery<?> source = mFrom.makeCompiledQuery();

        if (mRowFilter == TrueFilter.THE && mProjection == null) {
            return source;
        }

        // TODO: Store in the Parser cache -- map the string to this UnmappedQueryExpr, but
        // without mArgMap. Can use this if mArgMap is already null.
        String viewQuery = rowQueryString();

        int baseArgCount = mMaxArgument;
        Object[] viewArgs;

        if (mArgMap == null) {
            viewArgs = RowUtils.NO_ARGS;
        } else {
            viewArgs = mArgMap.keySet().toArray(new Object[mArgMap.size()]);
        }

        if (baseArgCount == 0) {
            return CompiledQuery.make(ViewedTable.view(source.table(), viewQuery, viewArgs));
        }

        if (viewArgs.length == 0) {
            return new CompiledQuery.Wrapped(source, baseArgCount) {
                @Override
                public Table table(Object... args) throws IOException {
                    return ViewedTable.view(source.table(args), viewQuery, args);
                }
            };
        }

        return new CompiledQuery.Wrapped(source, baseArgCount) {
            @Override
            public Table table(Object... args) throws IOException {
                int argCount = checkArgumentCount(args);
                var fullArgs = new Object[argCount + viewArgs.length];
                System.arraycopy(args, 0, fullArgs, 0, argCount);
                System.arraycopy(viewArgs, 0, fullArgs, argCount, viewArgs.length);
                return ViewedTable.view(source.table(args), viewQuery, fullArgs);
            }
        };
    }
}
