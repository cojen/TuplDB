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
import java.util.Objects;

import org.cojen.tupl.Table;

import org.cojen.tupl.rows.IdentityTable;

/**
 * Defines a node which represents a basic SQL select statement.
 *
 * @author Brian S. O'Neill
 */
public abstract sealed class SelectNode extends RelationNode
    permits SelectTableNode, SelectMappedNode
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
    public static SelectNode make(String name, RelationNode from, Node where, Node[] projection) {
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

        SelectTableNode stn = trySelectTable(name, from, where, projection);
        if (stn != null) {
            return stn;
        }

        // A custom row type and Mapper is required.

        var columns = new Column[projection.length];
        for (int i=0; i<projection.length; i++) {
            Node node = projection[i];
            // FIXME: Try to infer if column is a key or not. If full primary key is composite,
            // then all of its columns must be projected in order for any columns to have
            // key=true.
            columns[i] = new Column(node.type(), node.name(), false);
        }

        return new SelectMappedNode(TupleType.make(columns), name, from, where, projection);
    }

    /**
     * Returns a SelectTableNode if possible.
     */
    private static SelectTableNode trySelectTable(String name, RelationNode from,
                                                  Node where, Node[] projection)
    {
        if (!(from instanceof TableNode fromTable)) {
            return null;
        }

        if (where != null && !where.isPureFilter()) {
            return null;
        }

        // Maps actual column names to target names (renames).
        var projMap = new HashMap<String, String>();

        if (projection != null) {
            for (Node node : projection) {
                if (!(node instanceof ColumnNode cn) || cn.from() != from) {
                    return null;
                }
                if (projMap.putIfAbsent(cn.column().name(), cn.name()) != null) {
                    return null;
                }
            }
        }

        var type = TupleType.make(fromTable.table().rowType(), projMap);

        return new SelectTableNode(type, name, fromTable, where, projection);
    }

    protected final RelationNode mFrom;
    protected final Node mWhere;
    protected final Node[] mProjection;

    private Query<?> mQuery;

    protected SelectNode(TupleType type, String name,
                         RelationNode from, Node where, Node[] projection)
    {
        this(RelationType.make(type, from.type().cardinality()), name, from, where, projection);
    }

    protected SelectNode(RelationType type, String name,
                         RelationNode from, Node where, Node[] projection)
    {
        super(type, name);
        mFrom = from;
        mWhere = where;
        mProjection = projection;
    }

    @Override
    public final Node asType(Type type) {
        // FIXME: Should this ever be supported? Yes, for supporting renames, and for
        // converting to basic nodes (depending on cardinality).
        throw null;
    }

    @Override
    public final int highestParamOrdinal() {
        int ordinal = mFrom.highestParamOrdinal();
        if (mWhere != null) {
            ordinal = Math.max(ordinal, mWhere.highestParamOrdinal());
        }
        if (mProjection != null) {
            for (Node node : mProjection) {
                ordinal = Math.max(ordinal, node.highestParamOrdinal());
            }
        }
        return ordinal;
    }

    @Override
    public final boolean isPureFunction() {
        if (!mFrom.isPureFunction()) {
            return false;
        }

        if (mWhere != null && !mWhere.isPureFunction()) {
            return false;
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
        hash = hash * 31 + Objects.hashCode(mWhere);
        hash = hash * 31 + Arrays.hashCode(mProjection);
        return hash;
    }

    @Override
    public final boolean equals(Object obj) {
        return obj instanceof SelectNode sn
            && mFrom.equals(sn.mFrom)
            && Objects.equals(mWhere, sn.mWhere)
            && Arrays.equals(mProjection, sn.mProjection);
    }
}
