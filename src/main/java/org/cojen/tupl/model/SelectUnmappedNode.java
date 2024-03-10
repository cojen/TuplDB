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

package org.cojen.tupl.model;

import java.util.LinkedHashMap;
import java.util.Map;

import org.cojen.tupl.Table;

import org.cojen.tupl.table.RowUtils;

import org.cojen.tupl.table.filter.ColumnToConstantFilter;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;

/**
 * Defines a SelectNode which doesn't need a Mapper.
 *
 * @author Brian S. O'Neill
 * @see SelectNode#make
 */
final class SelectUnmappedNode extends SelectNode {
    /**
     * @see SelectNode#make
     */
    static SelectUnmappedNode make(TupleType type,
                                   RelationNode from, RowFilter filter, Node[] projection,
                                   int maxArgument)
    {
        if (from.type().tupleType().matches(projection)) {
            // Full projection, in the natural order, with no renames.
            projection = null;
        }

        Map<Object, Integer> argMap;

        if (filter == TrueFilter.THE) {
            argMap = null;
        } else {
            final var fArgMap = new LinkedHashMap<Object, Integer>();
            final int fMaxArgument = maxArgument;

            filter = filter.constantsToArguments((ColumnToConstantFilter f) -> {
                Object value = ((ConstantNode) f.constant()).value();
                Integer arg = fArgMap.get(value);
                if (arg == null) {
                    arg = fMaxArgument + fArgMap.size() + 1;
                    fArgMap.put(value, arg);
                }
                return arg;
            });

            int size = fArgMap.size();

            if (size == 0) {
                argMap = null;
            } else {
                argMap = fArgMap;
                maxArgument += size;
            }
        }

        return new SelectUnmappedNode(type, null, from, filter, projection, maxArgument, argMap);
    }

    private final Map<Object, Integer> mArgMap;

    private SelectUnmappedNode(TupleType type, String name,
                               RelationNode from, RowFilter filter, Node[] projection,
                               int maxArgument, Map<Object, Integer> argMap)
    {
        super(type, name, from, filter, projection, maxArgument);
        mArgMap = argMap;
    }

    private SelectUnmappedNode(RelationType type, String name,
                               RelationNode from, RowFilter filter, Node[] projection,
                               int maxArgument, Map<Object, Integer> argMap)
    {
        super(type, name, from, filter, projection, maxArgument);
        mArgMap = argMap;
    }

    private SelectUnmappedNode(SelectUnmappedNode node, String name) {
        super(node.type(), name, node.mFrom, node.mFilter, node.mProjection, node.mMaxArgument);
        mArgMap = node.mArgMap;
    }

    @Override
    public SelectUnmappedNode withName(String name) {
        return name.equals(name()) ? this : new SelectUnmappedNode(this, name);
    }

    @Override
    public SelectUnmappedNode replaceConstants(Map<ConstantNode, FieldNode> map, String prefix) {
        RelationNode from = mFrom.replaceConstants(map, prefix);
        Node[] projection = Node.replaceConstants(mProjection, map, prefix);
        if (from == mFrom && projection == mProjection) {
            return this;
        }
        return new SelectUnmappedNode(type(), name(), from, mFilter, projection,
                                      mMaxArgument, mArgMap);
    }

    @Override
    @SuppressWarnings("unchecked")
    protected TableProvider<?> doMakeTableProvider() {
        TableProvider<?> source = mFrom.makeTableProvider();

        if (mFilter == TrueFilter.THE && mProjection == null) {
            return source;
        }

        Map<String, String> projectionMap;

        // Build up the full query string.
        var qb = new StringBuilder().append('{');
        if (mProjection == null) {
            projectionMap = null;
            qb.append('*');
        } else {
            projectionMap = new LinkedHashMap<>();
            for (int i=0; i<mProjection.length; i++) {
                if (i > 0) {
                    qb.append(", ");
                }
                var cn = (ColumnNode) mProjection[i];
                String fieldName = cn.column().name();
                qb.append(fieldName);
                projectionMap.put(fieldName, cn.name());
            }
        }
        qb.append('}');

        if (mFilter != TrueFilter.THE) {
            qb.append(' ').append(filterString());
        }

        String viewQuery = qb.toString();

        int baseArgCount = mMaxArgument;
        Object[] viewArgs;

        if (mArgMap == null) {
            viewArgs = RowUtils.NO_ARGS;
        } else {
            int size = mArgMap.size();
            baseArgCount -= size;
            viewArgs = mArgMap.keySet().toArray(new Object[size]);
        }

        if (baseArgCount == 0) {
            return TableProvider.make(source.table().view(viewQuery, viewArgs), projectionMap);
        }

        if (viewArgs.length == 0) {
            return new TableProvider.Wrapped(source, projectionMap, baseArgCount) {
                @Override
                public Table table(Object... args) {
                    return source.table(args).view(viewQuery, args);
                }
            };
        }

        return new TableProvider.Wrapped(source, projectionMap, baseArgCount) {
            @Override
            public Table table(Object... args) {
                int argCount = checkArgumentCount(args);
                var fullArgs = new Object[argCount + viewArgs.length];
                System.arraycopy(args, 0, fullArgs, 0, argCount);
                System.arraycopy(viewArgs, 0, fullArgs, argCount, viewArgs.length);
                return source.table(args).view(viewQuery, fullArgs);
            }
        };
    }
}