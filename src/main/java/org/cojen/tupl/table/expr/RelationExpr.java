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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import java.util.function.Consumer;

import org.cojen.tupl.Table;

import org.cojen.tupl.table.filter.QuerySpec;

import org.cojen.maker.Variable;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public abstract sealed class RelationExpr extends Expr permits TableExpr, QueryExpr {
    private final RelationType mType;

    protected RelationExpr(int startPos, int endPos, RelationType type) {
        super(startPos, endPos);
        mType = type;
    }

    @Override
    public final RelationType type() {
        return mType;
    }

    @Override
    public final Expr asType(Type type) {
        // FIXME: Should this ever be supported? Yes, for supporting renames, and for
        // converting to basic expressions (depending on cardinality).
        throw null;
    }

    @Override
    public final boolean isNullable() {
        return false;
    }

    @Override
    public final void gatherEvalColumns(Consumer<Column> c) {
        // FIXME: revise when makeEval is implemented
        throw null;
    }

    @Override
    public final Variable makeEval(EvalContext context) {
        // FIXME: makeEval - return a Table
        throw null;
    }

    /**
     * Returns all of this relation's full qualified columns in a new array.
     */
    public final List<ProjExpr> fullProjection() {
        var columns = new ArrayList<ProjExpr>(type().rowType().numColumns());
        fullProjection(columns);
        return columns;
    }

    /**
     * Put all of the fully qualified columns of this relation into the given list.
     */
    public final void fullProjection(Collection<? super ProjExpr> columns) {
        fullProjection(columns::add);
    }

    /**
     * Pass all of the fully qualified columns of this relation to the given consumer.
     */
    public final void fullProjection(Consumer<? super ProjExpr> consumer) {
        fullProjection(consumer, type().rowType(), "", "");
    }

    private void fullProjection(Consumer<? super ProjExpr> consumer, TupleType tt,
                                String namePrefix, String fieldPrefix)
    {
        int num = tt.numColumns();
        for (int i=0; i<num; i++) {
            Column column = tt.column(i);
            if (column.type() instanceof TupleType ctt) {
                fullProjection(consumer, ctt, namePrefix + column.name() + '.',
                               fieldPrefix + column.fieldName() + '.');
            } else {
                column = Column.make
                    (column.type(), namePrefix + column.name(), fieldPrefix + column.fieldName());
                consumer.accept(ProjExpr.make(-1, -1, ColumnExpr.make(-1, -1, tt, column), 0));
            }
        }
    }

    /**
     * Returns a QuerySpec if this RelationExpr can be represented by one, against the given
     * table. Returns null otherwise.
     */
    public abstract QuerySpec querySpec(Table<?> table);

    /**
     * Makes a fully functional TableProvider from this expression.
     */
    public abstract TableProvider<?> makeTableProvider();
}
