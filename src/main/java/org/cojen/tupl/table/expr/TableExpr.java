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

import org.cojen.tupl.Table;

import org.cojen.tupl.table.IdentityTable;

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
    public static TableExpr identity() {
        return make(-1, -1, IdentityTable.THE);
    }

    /**
     * @param startPos source code start position, zero-based, inclusive; is -1 if not applicable
     * @param endPos source code end position, zero-based, exclusive; is -1 if not applicable
     */
    public static TableExpr make(int startPos, int endPos, Table table) {
        var cardinality = table instanceof IdentityTable ? Cardinality.ONE : Cardinality.MANY;
        var type = RelationType.make(TupleType.make(table.rowType(), null), cardinality);
        return new TableExpr(startPos, endPos, type, table);
    }

    private final Table<?> mTable;

    private TableExpr(int startPos, int endPos, RelationType type, Table table) {
        super(startPos, endPos, type);
        mTable = table;
    }

    public Table table() {
        return mTable;
    }

    @Override
    public int maxArgument() {
        return 0;
    }

    @Override
    public boolean isPureFunction() {
        return mTable instanceof IdentityTable;
    }

    @Override
    public QuerySpec querySpec(Table<?> table) {
        return table == mTable ? new QuerySpec(null, null, TrueFilter.THE) : null;
    }

    @Override
    public CompiledQuery<?> makeCompiledQuery() {
        return CompiledQuery.make(mTable);
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            // Note that the Table instance isn't encoded, only the row type it acts upon. This
            // is because generated code doesn't maintain static references to Tables, and thus
            // Table instances doesn't affect what the code looks like.
            enc.encodeClass(rowTypeClass());
        }
    }

    @Override
    public int hashCode() {
        return mTable.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj instanceof TableExpr te && mTable == te.mTable;
    }

    @Override
    public String toString() {
        return defaultToString();
    }

    @Override
    protected void appendTo(StringBuilder b) {
        // FIXME: Need to revise the syntax for the "from" portion. Pipeline syntax?
        b.append(rowTypeClass().getName()).append(' ').append("{*}");
    }
}
