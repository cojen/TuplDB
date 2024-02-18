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

import java.util.Map;
import java.util.Objects;

import org.cojen.tupl.table.ColumnInfo;

import org.cojen.tupl.table.filter.AndFilter;
import org.cojen.tupl.table.filter.ColumnFilter;
import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.ColumnToConstantFilter;
import org.cojen.tupl.table.filter.GroupFilter;
import org.cojen.tupl.table.filter.OpaqueFilter;
import org.cojen.tupl.table.filter.OrFilter;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.Visitor;

/**
 * Converts a RowFilter back into a Node.
 *
 * @author Brian S. O'Neill
 */
final class ToNodeVisitor implements Visitor {
    private final Map<String, ColumnNode> mColumns;

    /**
     * @param columns map which was filled in by the Node.toRowFilter method.
     */
    ToNodeVisitor(Map<String, ColumnNode> columns) {
        mColumns = columns;
    }

    Node apply(RowFilter filter) {
        filter.accept(this);
        return mCurrent;
    }

    private Node mCurrent;

    @Override
    public void visit(OrFilter filter) {
        visit(filter, BinaryOpNode.OP_OR);
    }

    @Override
    public void visit(AndFilter filter) {
        visit(filter, BinaryOpNode.OP_AND);
    }

    private void visit(GroupFilter filter, int op) {
        RowFilter[] subFilters = filter.subFilters();

        if (subFilters.length == 0) {
            mCurrent = ConstantNode.make(op == BinaryOpNode.OP_AND);
            return;
        }

        subFilters[0].accept(this);
        Node node = mCurrent;

        for (int i=1; i<subFilters.length; i++) {
            subFilters[i].accept(this);
            node = BinaryOpNode.make(op, node, mCurrent);
        }

        mCurrent = node;
    }

    @Override
    public void visit(ColumnToArgFilter filter) {
        finish(filter, ParamNode.make(null, filter.argument()));
    }

    @Override
    public void visit(ColumnToColumnFilter filter) {
        finish(filter, toColumNode(filter.otherColumn()));
    }

    @Override
    public void visit(ColumnToConstantFilter filter) {
        finish(filter, (ConstantNode) filter.constant());
    }

    private void finish(ColumnFilter filter, Node right) {
        mCurrent = BinaryOpNode.make(filter.operator(), toColumNode(filter.column()), right);
    }

    private ColumnNode toColumNode(ColumnInfo info) {
        return Objects.requireNonNull(mColumns.get(info.name));
    }

    @Override
    public void visit(OpaqueFilter filter) {
        mCurrent = (Node) filter.attachment();
    }
}
