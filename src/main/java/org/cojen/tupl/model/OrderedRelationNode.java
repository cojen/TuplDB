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

package org.cojen.tupl.model;

import java.util.Arrays;
import java.util.Map;

import org.cojen.tupl.Table;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class OrderedRelationNode extends RelationNode {
    /**
     * @param from required
     * @param orderBy required; must refer to field names of the from table
     * @param orderByFlags supported flags are Type.TYPE_NULL_LOW and Type.TYPE_DESCENDING.
     * @param projectionMap table provider projection; pass null to project all from columns
     * @see SelectNode#make
     */
    static OrderedRelationNode make(RelationNode from,
                                    String[] orderBy, int[] orderByFlags,
                                    Map<String, String> projectionMap)
    {
        return new OrderedRelationNode(null, from, orderBy, orderByFlags, projectionMap);
    }

    private final RelationNode mFrom;
    private final String[] mOrderBy;
    private final int[] mOrderByFlags;
    private final Map<String, String> mProjectonMap;

    private TableProvider<?> mTableProvider;

    private OrderedRelationNode(String name, RelationNode from,
                                String[] orderBy, int[] orderByFlags,
                                Map<String, String> projectionMap)
    {
        super(from.type(), name);
        mFrom = from;
        mOrderBy = orderBy;
        mOrderByFlags = orderByFlags;
        mProjectonMap = projectionMap;
    }

    @Override
    public OrderedRelationNode withName(String name) {
        return name.equals(name()) ? this
            : new OrderedRelationNode(name, mFrom, mOrderBy, mOrderByFlags, mProjectonMap);
    }

    @Override
    public int maxArgument() {
        return mFrom.maxArgument();
    }

    @Override
    public boolean isPureFunction() {
        return mFrom.isPureFunction();
    }

    @Override
    public OrderedRelationNode replaceConstants(Map<ConstantNode, FieldNode> map, String prefix) {
        RelationNode from = mFrom.replaceConstants(map, prefix);
        if (from == mFrom) {
            return this;
        }
        return new OrderedRelationNode(name(), from, mOrderBy, mOrderByFlags, mProjectonMap);
    }

    @Override
    public TableProvider<?> makeTableProvider() {
        if (mTableProvider == null) {
            mTableProvider = doMakeTableProvider();
        }
        return mTableProvider;
    }

    @SuppressWarnings("unchecked")
    private TableProvider doMakeTableProvider() {
        TableProvider source = mFrom.makeTableProvider();

        var b = new StringBuilder().append('{').append('*');

        for (int i=0; i<mOrderBy.length; i++) {
            b.append(", ");
            int flags = i < mOrderByFlags.length ? mOrderByFlags[i] : 0;
            b.append((flags & Type.TYPE_DESCENDING) != 0 ? '-' : '+');
            if ((flags & Type.TYPE_NULL_LOW) != 0) {
                b.append('!');
            }
            b.append(mOrderBy[i]);
        }

        String spec = b.append('}').toString();

        Map<String, String> projectionMap = mProjectonMap;
        if (projectionMap == null) {
            projectionMap = source.projection();
        }

        int argCount = source.argumentCount();

        if (argCount == 0) {
            return TableProvider.make(source.table().view(spec), projectionMap);
        }

        return new TableProvider.Wrapped(source, projectionMap, argCount) {
            @Override
            public Table table(Object... args) {
                return source.table(args).view(spec);
            }
        };
    }

    @Override
    public int hashCode() {
        int hash = mFrom.hashCode();
        hash = hash * 31 + Arrays.hashCode(mOrderBy);
        hash = hash * 31 + Arrays.hashCode(mOrderByFlags);
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof OrderedRelationNode orn
            && mFrom.equals(orn.mFrom)
            && Arrays.equals(mOrderBy, orn.mOrderBy)
            && Arrays.equals(mOrderByFlags, orn.mOrderByFlags);
    }
}
