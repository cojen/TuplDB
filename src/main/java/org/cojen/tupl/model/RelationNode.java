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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import java.util.function.Consumer;

import org.cojen.maker.Variable;

/**
 * Defines a node which accesses a relation/table of rows.
 *
 * @author Brian S. O'Neill
 */
public abstract sealed class RelationNode extends Node
    permits JoinNode, OrderedRelationNode, SelectNode, TableNode
{
    private final RelationType mType;
    private final String mName;

    /**
     * @param name can be null to automatically assign a name
     */
    protected RelationNode(RelationType type, String name) {
        mType = type;
        mName = name != null ? name : type.tupleType().clazz().getName();
    }

    @Override
    public final RelationType type() {
        return mType;
    }

    @Override
    public final Node asType(Type type) {
        // FIXME: Should this ever be supported? Yes, for supporting renames, and for
        // converting to basic nodes (depending on cardinality).
        throw null;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public final void evalColumns(Set<String> columns) {
        // FIXME: revise when makeEval is implemented
    }

    @Override
    public final Variable makeEval(EvalContext context) {
        // FIXME: makeEval - return a Table
        throw null;
    }

    public final String name() {
        return mName;
    }

    @Override
    public abstract RelationNode withName(String name);

    /**
     * Find a column in this relation which matches the given name.
     *
     * @param name qualified or unqualified column name to find
     * @return column with a fully qualified name, with the canonical case
     * @throws IllegalArgumentException if not found or is ambiguous
     */
    public final ColumnNode findColumn(String name) {
        return findColumn(name, name);
    }

    /**
     * Find a column node in this relation which matches the given name. The name of the node's
     * column is a fully qualified field, which means that for joins, it's a dotted name.
     *
     * @param name qualified or unqualified column name to find
     * @param label label/alias to use instead of original column name (can have spaces)
     * @return column with a fully qualified name, with the canonical case
     * @throws IllegalArgumentException if not found or is ambiguous
     */
    public final ColumnNode findColumn(String name, String label) {
        int dotIx = name.indexOf('.');
        if (dotIx >= 0 && name.startsWith(name())) {
            name = name.substring(dotIx + 1);
        }

        return ColumnNode.make(this, label, type().tupleType().findColumn(name, true));
    }

    /**
     * Returns all of this relation's columns in a new array. Each column has a fully qualified
     * name as if it was found using findColumn.
     */
    public final ColumnNode[] allColumns() {
        var columns = new ArrayList<ColumnNode>(type().tupleType().numColumns());
        allColumns(columns);
        return columns.toArray(new ColumnNode[columns.size()]);
    }

    /**
     * Put all of the columns of this relation into the given list. Each column has a fully
     * qualified name as if it was found using findColumn.
     */
    public final void allColumns(Collection<? super ColumnNode> columns) {
        allColumns(columns::add);
    }

    /**
     * Put all of the columns of this relation into the given list. Each column has a fully
     * qualified name as if it was found using findColumn.
     */
    public final void allColumns(Consumer<? super ColumnNode> consumer) {
        allColumns(consumer, type().tupleType(), "", "");
    }

    private void allColumns(Consumer<? super ColumnNode> consumer, TupleType tt,
                            String namePrefix, String fieldPrefix)
    {
        int num = tt.numColumns();
        for (int i=0; i<num; i++) {
            Column column = tt.column(i);
            String field = tt.field(i);
            if (column.type() instanceof TupleType ctt) {
                allColumns(consumer, ctt, namePrefix + column.name() + '.',
                           fieldPrefix + field + '.');
            } else {
                consumer.accept(ColumnNode.make(this, namePrefix + column.name(),
                                                column.withName(fieldPrefix + field)));
            }
        }
    }

    /**
     * Put all of the matching columns of this relation into the given list. Each column has a
     * fully qualified name as if it was found using findColumn using a label which is prefixed
     * by the given table name.
     *
     * @param table name of table to obtain all columns for (it's treated as if it had a
     * wildcard at the end)
     */
    public final void allTableColumns(Collection<? super ColumnNode> columns, String table) {
        final int originalSize = columns.size();

        TupleType tt = type().tupleType();
        int num = tt.numColumns();
        for (int i=0; i<num; i++) {
            Column column = tt.column(i);
            String field = tt.field(i);
            if (column.type() instanceof TupleType ctt) {
                if (table.equals(column.name())) {
                    allTableColumns(columns, ctt, column.name() + '.', field + '.');
                }
            } else {
                if (table.equals(mName)) {
                    columns.add(ColumnNode.make(this, table + '.' + column.name(),
                                                column.withName(field)));
                }
            }
        }

        if (columns.size() == originalSize) {
            throw new IllegalArgumentException("Table isn't found: " + table);
        }
    }

    private void allTableColumns(Collection<? super ColumnNode> columns, TupleType tt,
                                 String namePrefix, String fieldPrefix)
    {
        int num = tt.numColumns();
        for (int i=0; i<num; i++) {
            Column column = tt.column(i);
            if (column.type() instanceof TupleType ctt) {
                // FIXME: Is this possible?
            } else {
                columns.add(ColumnNode.make(this, namePrefix + column.name(),
                                            column.withName(fieldPrefix + tt.field(i))));
            }
        }
    }

    /**
     * Makes a fully functional TableProvider from this node. It can then be used with
     * DbQueryMaker to make DbQuery instances.
     */
    public abstract TableProvider<?> makeTableProvider();
}
