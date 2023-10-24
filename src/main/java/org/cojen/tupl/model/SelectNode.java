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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.cojen.tupl.Table;

import org.cojen.tupl.rows.filter.ComplexFilterException;
import org.cojen.tupl.rows.filter.RowFilter;
import org.cojen.tupl.rows.filter.TrueFilter;

import org.cojen.tupl.rows.IdentityTable;
import org.cojen.tupl.rows.RowInfo;

/**
 * Defines a node which represents a basic SQL select statement.
 *
 * @author Brian S. O'Neill
 */
public abstract sealed class SelectNode extends RelationNode
    permits SelectUnmappedNode, SelectMappedNode
{
    // FIXME: For supporting order-by, define a separate RelationNode type which wraps another
    // RelationNode. It should just create a view. The SQL order-by clause supports original
    // columns names, expressions, and the "as" aliases. For this reason, the order-by
    // specification might need to be assigned to the SelectNode. When factory method is called
    // it applies the necessary transformations.

    /**
     * @param name can be null to automatically assign a name
     * @param from can be null if not selecting from any table at all
     * @param where can be null if not filtered, or else type must be boolean or be convertable
     * to boolean
     * @param projection can be null to project all columns
     */
    public static RelationNode make(String name, RelationNode from, Node where, Node[] projection) {
        if (from == null) {
            from = TableNode.make(null, IdentityTable.THE);
        }

        if (where != null) {
            where = where.asType(BasicType.BOOLEAN);
            if (where instanceof ConstantNode cn && cn.value() == Boolean.TRUE) {
                where = null;
            }
        }

        // FIXME: Perform type checking / validation.

        TupleType fromType = from.type().tupleType();

        int maxArgument = from.maxArgument();

        // Maps actual column names to target names (renames). Is null when:
        // - projection is null (all columns are projected)
        // - or the requested projection refers to a column not known by the "from" relation
        // - or if anything is projected more than once
        Map<String, String> pureProjection;

        if (projection == null) {
            pureProjection = null;
        } else {
            for (Node node : projection) {
                maxArgument = Math.max(maxArgument, node.maxArgument());
            }

            pureProjection = new HashMap<String, String>();

            for (Node node : projection) {
                if (!(node instanceof ColumnNode cn)) {
                    // Projecting something not known by the relation.
                    pureProjection = null;
                    break;
                }
                if (pureProjection.putIfAbsent(cn.column().name(), cn.name()) != null) {
                    // Column is projected more than once.
                    pureProjection = null;
                    break;
                }
            }

            if (pureProjection != null && pureProjection.size() == fromType.numColumns()) {
                // All columns are projected.
                projection = null;
                pureProjection = null;
            }
        }

        // Split the filter such that the unmapped part can be pushed down. The remaining
        // mapped part must be handled by a Mapper. The unmapped part is guaranteed to have no
        // OpaqueFilters. If it did, SelectUnmappedNode would produce a broken query string.
        RowFilter unmappedFilter, mappedFilter;

        if (where == null) {
            unmappedFilter = mappedFilter = TrueFilter.THE;
        } else {
            maxArgument = Math.max(maxArgument, where.maxArgument());

            RowInfo info = RowInfo.find(fromType.clazz());

            RowFilter filter = where.toFilter(info);
            try {
                filter = filter.cnf();
            } catch (ComplexFilterException e) {
                // The split won't be as effective, and so the Mapper might end up doing more
                // filtering work.
            }

            var whereFilters = new RowFilter[2];
            filter.split(info.allColumns, whereFilters);

            unmappedFilter = whereFilters[0];
            mappedFilter = whereFilters[1];
        }

        // Attempt to push down filtering and projection by replacing the "from" node with a
        // SelectUnmappedNode.
        pushDown: {
            Node[] fromProjection;

            if (mappedFilter == TrueFilter.THE && projection != null && pureProjection != null) {
                // Can push down filtering and projection entirely; no Mapper is needed.
                fromType = TupleType.make(fromType.clazz(), pureProjection);
                fromProjection = projection;
                projection = null;
            } else if (unmappedFilter == TrueFilter.THE) {
                // There's no filter and all columns need to be projected, so there's nothing
                // to push down.
                break pushDown;
            } else {
                fromProjection = null;
            }

            from = SelectUnmappedNode.make
                (fromType, name, from, unmappedFilter, fromProjection, maxArgument);
        }

        if (mappedFilter == TrueFilter.THE && projection == null) {
            if (!Objects.equals(name, from.name())) {
                from = SelectUnmappedNode.rename(from, name);
            }
            return from;
        }

        // A custom row type and Mapper is required.

        return new SelectMappedNode
            (TupleType.make(projection), name, from, mappedFilter, projection, maxArgument);
    }

    protected final RelationNode mFrom;
    protected final RowFilter mFilter;
    protected final Node[] mProjection;
    protected final int mMaxArgument;

    private Query<?> mQuery;

    protected SelectNode(TupleType type, String name,
                         RelationNode from, RowFilter filter, Node[] projection,
                         int maxArgument)
    {
        this(RelationType.make(type, from.type().cardinality()),
             name, from, filter, projection, maxArgument);
    }

    protected SelectNode(RelationType type, String name,
                         RelationNode from, RowFilter filter, Node[] projection,
                         int maxArgument)
    {
        super(type, name);
        mFrom = from;
        mFilter = filter;
        mProjection = projection;
        mMaxArgument = maxArgument;
    }

    @Override
    public final int maxArgument() {
        return mMaxArgument;
    }

    @Override
    public final boolean isPureFunction() {
        if (!mFrom.isPureFunction()) {
            return false;
        }

        // FIXME: Need a Visitor to examine the OpaqueFilters.
        if (mFilter != TrueFilter.THE) {
            throw null;
        }

        if (mProjection != null) {
            for (Node node : mProjection) {
                if (!node.isPureFunction()) {
                    return false;
                }
            }
        }

        return true;
    }

    @Override
    public final Query<?> makeQuery() {
        if (mQuery == null) {
            mQuery = doMakeQuery();
        }
        return mQuery;
    }

    protected abstract Query<?> doMakeQuery();

    @Override
    public final int hashCode() {
        int hash = mFrom.hashCode();
        hash = hash * 31 + mFilter.hashCode();
        hash = hash * 31 + Arrays.hashCode(mProjection);
        return hash;
    }

    @Override
    public final boolean equals(Object obj) {
        return obj instanceof SelectNode sn
            && mFrom.equals(sn.mFrom)
            && mFilter.equals(sn.mFilter)
            && Arrays.equals(mProjection, sn.mProjection);
    }
}
