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
import java.util.HashSet;
import java.util.List;

import java.util.function.Consumer;

import org.cojen.tupl.PrimaryKey;
import org.cojen.tupl.QueryException;
import org.cojen.tupl.Row;

import org.cojen.tupl.table.OrderBy;

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
        // TODO: Should this ever be supported? Yes, for supporting renames, and for
        // converting to basic expressions (depending on cardinality).
        throw null;
    }

    @Override
    public final boolean isNullable() {
        return false;
    }

    @Override
    public final boolean isConstant() {
        return false;
    }

    @Override
    public final void gatherEvalColumns(Consumer<Column> c) {
        // TODO: revise when makeEval is implemented
        throw null;
    }

    @Override
    protected final Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
        // TODO: makeEval - return a Table
        throw null;
    }

    public final TupleType rowType() {
        return type().rowType();
    }

    public final Class<?> rowTypeClass() {
        return type().rowType().clazz();
    }

    /**
     * Returns all of this relation's columns in a new array.
     */
    public final List<ProjExpr> fullProjection() {
        var columns = new ArrayList<ProjExpr>(rowType().numColumns());
        fullProjection(columns);
        return columns;
    }

    /**
     * Put all of the columns of this relation into the given list.
     */
    public final void fullProjection(Collection<? super ProjExpr> columns) {
        fullProjection(columns::add);
    }

    /**
     * Pass all of the columns of this relation to the given consumer.
     */
    public final void fullProjection(Consumer<? super ProjExpr> consumer) {
        fullProjection(consumer, rowType());
    }

    private static void fullProjection(Consumer<? super ProjExpr> consumer, TupleType tt) {
        for (Column column : tt) {
            consumer.accept(ProjExpr.make(-1, -1, ColumnExpr.make(-1, -1, tt, column), 0));
        }
    }

    /**
     * Returns the explictly specified relation ordering, which is empty if unspecified.
     */
    public abstract String orderBySpec();

    /**
     * Returns a QuerySpec if this RelationExpr can be represented by one, against this
     * relation's row type. A QueryException is thrown otherwise.
     */
    public abstract QuerySpec querySpec() throws QueryException;

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
        throw new QueryException("Mismatched row type");
    }

    /**
     * Makes a fully functional CompiledQuery from this expression, composed of rows which
     * implement the Row interface.
     */
    @SuppressWarnings("unchecked")
    public final CompiledQuery<Row> makeCompiledRowQuery() throws IOException {
        Class<?> rowTypeClass = rowTypeClass();

        if (Row.class.isAssignableFrom(rowTypeClass)) {
            return (CompiledQuery<Row>) makeCompiledQuery();
        }

        List<ProjExpr> projection = fullProjection();

        PrimaryKey ann = rowTypeClass.getAnnotation(PrimaryKey.class);
        String[] primaryKey = ann == null ? null : ann.value();

        if (primaryKey != null) {
            // Verify that all primary key columns are projected.
            if (projection.size() < primaryKey.length) {
                // Not possible.
                primaryKey = null;
            } else {
                var projSet = new HashSet<String>(projection.size() << 1);
                for (ProjExpr pe : projection) {
                    projSet.add(pe.name());
                }
                for (String name : primaryKey) {
                    if (!projSet.contains(name)) {
                        primaryKey = null;
                        break;
                    }
                }
            }
        }

        var newType = RelationType.make(rowType().withPrimaryKey(primaryKey), type().cardinality());

        return MappedQueryExpr.make
            (-1, -1, newType, this, TrueFilter.THE, null, projection, maxArgument(), null)
            .makeCompiledRowQuery();
    }
}
