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

import java.util.Map;
import java.util.Set;

import org.cojen.tupl.Table;

import org.cojen.tupl.table.JoinIdentityTable;

import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.TrueFilter;

/**
 * Defines an expression which accesses an ordinary table.
 *
 * @author Brian S. O'Neill
 */
public final class TableExpr extends RelationExpr {
    /**
     * Uses an unmodifiable table consisting of one row with no columns.
     */
    public static TableExpr joinIdentity() {
        return make(-1, -1, JoinIdentityTable.THE);
    }

    /**
     * @param startPos source code start position, zero-based, inclusive; is -1 if not applicable
     * @param endPos source code end position, zero-based, exclusive; is -1 if not applicable
     */
    public static TableExpr make(int startPos, int endPos, Table table) {
        return make(startPos, endPos, table, null);
    }

    /**
     * @param startPos source code start position, zero-based, inclusive; is -1 if not applicable
     * @param endPos source code end position, zero-based, exclusive; is -1 if not applicable
     * @param projection consists of column names; can pass null to project all columns
     */
    public static TableExpr make(int startPos, int endPos, Table table, Set<String> projection) {
        var cardinality = table instanceof JoinIdentityTable ? Cardinality.ONE : Cardinality.MANY;
        var type = RelationType.make(TupleType.forClass(table.rowType(), projection), cardinality);
        return new TableExpr(startPos, endPos, type, table);
    }

    /**
     * By making a TableExpr against just a row type, calling the makeCompiledQuery method
     * throws an IllegalStateException.
     *
     * @param startPos source code start position, zero-based, inclusive; is -1 if not applicable
     * @param endPos source code end position, zero-based, exclusive; is -1 if not applicable
     */
    public static TableExpr make(int startPos, int endPos, Class<?> rowType) {
        var type = RelationType.make(TupleType.forClass(rowType, null), Cardinality.MANY);
        return new TableExpr(startPos, endPos, type, null);
    }

    /**
     * By making a TableExpr against just a row type, calling the makeCompiledQuery method
     * throws an IllegalStateException.
     *
     * @param startPos source code start position, zero-based, inclusive; is -1 if not applicable
     * @param endPos source code end position, zero-based, exclusive; is -1 if not applicable
     * @param projection consists of column names; can pass null to project all columns
     */
    public static TableExpr make(int startPos, int endPos,
                                 Class<?> rowType, Set<String> projection)
    {
        var type = RelationType.make(TupleType.forClass(rowType, projection), Cardinality.MANY);
        return new TableExpr(startPos, endPos, type, null);
    }

 
    private final Table<?> mTable;

    private TableExpr(int startPos, int endPos, RelationType type, Table table) {
        super(startPos, endPos, type);
        mTable = table;
    }

    @Override
    public int maxArgument() {
        return 0;
    }

    @Override
    public boolean isPureFunction() {
        return mTable != null && mTable instanceof JoinIdentityTable;
    }

    @Override
    public boolean isOrderDependent() {
        return false;
    }

    @Override
    public boolean isGrouping() {
        return false;
    }

    @Override
    public boolean isAccumulating() {
        return false;
    }

    @Override
    public boolean isAggregating() {
        return false;
    }

    @Override
    public Expr asAggregate(Set<String> group) {
        return this;
    }

    @Override
    public Expr asWindow(Map<ColumnExpr, AssignExpr> newAssignments) {
        return this;
    }

    @Override
    public String orderBySpec() {
        return "";
    }

    @Override
    public QuerySpec querySpec() {
        return new QuerySpec(null, null, TrueFilter.THE);
    }

    @Override
    public QuerySpec tryQuerySpec(Class<?> rowType) {
        return rowType == rowTypeClass() ? new QuerySpec(null, null, TrueFilter.THE) : null;
    }

    @Override
    public CompiledQuery<?> makeCompiledQuery() {
        if (mTable == null) {
            throw new IllegalStateException();
        }
        return CompiledQuery.make(mTable);
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            // Note that the Table instance isn't encoded, only the row type it acts upon. This
            // is because generated code doesn't maintain static references to Tables, and thus
            // Table instances don't affect what the code looks like.
            enc.encodeClass(rowTypeClass());
        }
    }

    @Override
    public int hashCode() {
        return type().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj instanceof TableExpr te && type().equals(te.type());
    }

    @Override
    public String toString() {
        return defaultToString();
    }

    @Override
    public void appendTo(StringBuilder b) {
        // TODO: If not all columns are projected, don't use the wildcard character.
        b.append(rowTypeClass().getName()).append(' ').append("{*}");
    }
}
