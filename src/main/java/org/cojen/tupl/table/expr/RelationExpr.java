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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import java.util.function.Consumer;

import org.cojen.tupl.Row;
import org.cojen.tupl.Table;

import org.cojen.tupl.table.RowMethodsMaker;

import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.TrueFilter;

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

    public final TupleType rowType() {
        return type().rowType();
    }

    public final Class<?> rowTypeClass() {
        return type().rowType().clazz();
    }

    /**
     * Returns all of this relation's fully qualified columns in a new array.
     */
    public final List<ProjExpr> fullProjection() {
        var columns = new ArrayList<ProjExpr>(rowType().numColumns());
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
        fullProjection(consumer, rowType(), "");
    }

    private void fullProjection(Consumer<? super ProjExpr> consumer, TupleType tt, String prefix) {
        for (Column column : tt) {
            if (column.type() instanceof TupleType ctt) {
                fullProjection(consumer, ctt, prefix + column.name() + '.');
            } else {
                column = Column.make(column.type(), prefix + column.name(), column.isHidden());
                consumer.accept(ProjExpr.make(-1, -1, ColumnExpr.make(-1, -1, tt, column), 0));
            }
        }
    }

    /**
     * Returns a QuerySpec if this RelationExpr can be represented by one, against the given
     * row type. A QueryException is thrown otherwise.
     */
    public abstract QuerySpec querySpec(Class<?> rowType) throws QueryException;

    /**
     * Intended to be used by querySpec implementations.
     */
    protected final void checkRowType(Class<?> rowType) throws QueryException {
        if (rowType != rowTypeClass()) {
            throw new QueryException("Mismatched row type");
        }
    }

    /**
     * Returns a QuerySpec if this RelationExpr can be represented by one, against the given
     * row type. Returns null otherwise.
     */
    public abstract QuerySpec tryQuerySpec(Class<?> rowType);

    /**
     * Makes a fully functional CompiledQuery from this expression.
     */
    public abstract CompiledQuery<?> makeCompiledQuery() throws IOException;

    /**
     * If possible, makes a fully functional CompiledQuery from this expression, composed of
     * the given row type.
     *
     * @throws QueryException if the query derives columns not available to the given row type
     */
    @SuppressWarnings("unchecked")
    public final <R> CompiledQuery<R> makeCompiledQuery(Class<R> rowTypeClass)
        throws IOException, QueryException
    {
        TupleType thisRowType = rowType();

        if (rowTypeClass.isAssignableFrom(thisRowType.clazz())) {
            return (CompiledQuery<R>) makeCompiledQuery();
        }

        TupleType otherRowType = TupleType.make(rowTypeClass, null);

        var b = new StringBuilder().append("Query derives new or mismatched columns: ");
        final int originalLength = b.length();

        for (Column c : thisRowType) {
            String name = c.name();
            Column otherColumn = otherRowType.tryColumnFor(name);
            if (otherColumn == null || !c.type().equals(otherColumn.type())) {
                if (b.length() != originalLength) {
                    b.append(", ");
                }
                c.type().appendTo(b, true);
                b.append(' ').append(RowMethodsMaker.unescape(name));
                if (otherColumn != null) {
                    b.append(" != ");
                    otherColumn.type().appendTo(b, true);
                    b.append(' ').append(RowMethodsMaker.unescape(name));
                }
            }
        }

        if (b.length() == originalLength) {
            // Shouldn't happen. See TupleType.canRepresent.
            b.append('?');
        }

        throw new QueryException(b.toString());
    }

    /**
     * Makes a fully functional CompiledQuery from this expression, composed of rows which
     * implement the Row interface.
     */
    @SuppressWarnings("unchecked")
    public final CompiledQuery<Row> makeCompiledRowQuery() throws IOException {
        if (Row.class.isAssignableFrom(rowTypeClass())) {
            return (CompiledQuery<Row>) makeCompiledQuery();
        }

        List<ProjExpr> projection = fullProjection();

        var newType = RelationType.make(TupleType.make(projection), type().cardinality());

        return new MappedQueryExpr
            (-1, -1, newType, this, TrueFilter.THE, null, projection, maxArgument())
            .makeCompiledRowQuery();
    }
}
