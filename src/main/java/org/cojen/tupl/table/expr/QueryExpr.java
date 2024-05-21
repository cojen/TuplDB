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
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.OrderBy;
import org.cojen.tupl.table.RowInfo;

import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.ColumnToConstantFilter;
import org.cojen.tupl.table.filter.ComplexFilterException;
import org.cojen.tupl.table.filter.ExprFilter;
import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;
import org.cojen.tupl.table.filter.Visitor;

/**
 * Defines a fully parsed query.
 *
 * @author Brian S. O'Neill
 */
public abstract sealed class QueryExpr extends RelationExpr
    permits MappedQueryExpr, UnmappedQueryExpr
{
    /**
     * @param from can be null if not selecting from any table at all
     * @param filter can be null if not filtered, or else filter type must be boolean or be
     * convertible to boolean
     * @param projection can be null to project all columns
     */
    public static RelationExpr make(int startPos, int endPos,
                                    RelationExpr from, Expr filter, List<ProjExpr> projection)
    {
        if (from == null) {
            from = TableExpr.identity();
        }

        if (filter != null) {
            filter = filter.asType(BasicType.BOOLEAN);
            if (filter instanceof ConstantExpr ce && ce.value() == Boolean.TRUE) {
                filter = null;
            }
        }

        final TupleType fromType = from.rowType();

        int maxArgument = from.maxArgument();

        // Determine the exact projection to be applied to the "from" source. Each ProjExpr
        // wraps a simple ColumnExpr -- that is, they don't have any paths. If these
        // collections are null, then all of the "from" columns are to be projected.
        final LinkedHashMap<String, ProjExpr> fromProjMap;
        final ArrayList<ProjExpr> fromProjection;

        if (projection == null) {
            fromProjMap = null;
            fromProjection = null;
        } else {
            fromProjMap = new LinkedHashMap<String, ProjExpr>();

            for (ProjExpr pe : projection) {
                maxArgument = Math.max(maxArgument, pe.maxArgument());

                int flags = pe.flags();

                if ((flags & ProjExpr.F_ORDER_BY) != 0) {
                    // Force the projection to be included, because it might be required by a
                    // sort operation.
                    // FIXME: If necessary, remove it later, by obtaining a view.
                    flags &= ~ProjExpr.F_EXCLUDE;
                } else if ((flags & ProjExpr.F_EXCLUDE) != 0) {
                    continue;
                }

                pe.gatherEvalColumns(fromType, fromProjMap, flags, null);
            }

            fromProjection = new ArrayList<>(fromProjMap.values());
        }

        // Split the filter such that the unmapped part can be pushed down. The remaining
        // mapped part must be handled by a Mapper. The unmapped part is guaranteed to have no
        // ExprFilters. If it did, UnmappedQueryExpr would produce a broken query string.
        RowFilter unmappedRowFilter, mappedRowFilter;
        Expr mappedFilter;

        if (filter == null) {
            unmappedRowFilter = mappedRowFilter = TrueFilter.THE;
            mappedFilter = null;
        } else {
            maxArgument = Math.max(maxArgument, filter.maxArgument());

            RowInfo info = RowInfo.find(fromType.clazz());

            final var columns = new HashMap<String, ColumnExpr>();
            final RowFilter originalRowFilter = filter.toRowFilter(info, columns);

            // Try to transform the filter to cnf, to make split method be more effective. If
            // this isn't possible, then filtering might not be pushed down as much.
            RowFilter rowFilter;

            try {
                rowFilter = originalRowFilter.cnf();

                if (hasRepeatedNonPureFunctions(rowFilter)) {
                    // Assume that duplication was caused by the cnf transform. Non-pure
                    // functions can yield different results each time, and so they cannot be
                    // invoked multiple times within the filter.
                    rowFilter = originalRowFilter;
                }
            } catch (ComplexFilterException e) {
                rowFilter = originalRowFilter;
            }

            var splitRowFilters = new RowFilter[2];
            rowFilter.split(info.allColumns, splitRowFilters);

            unmappedRowFilter = splitRowFilters[0];
            mappedRowFilter = splitRowFilters[1];

            if (unmappedRowFilter == TrueFilter.THE) {
                if (mappedRowFilter == TrueFilter.THE) {
                    // The filter pushes down entirely; just use the original row filter.
                    unmappedRowFilter = originalRowFilter;
                } else {
                    // Nothing pushes down; just use the original row filter.
                    mappedRowFilter = originalRowFilter;
                }
                mappedFilter = filter;
            } else if (mappedRowFilter == TrueFilter.THE) {
                // The filter pushes down entirely; just use the original row filter.
                unmappedRowFilter = originalRowFilter;
                mappedFilter = null;
            } else if (mappedRowFilter == originalRowFilter) {
                // Nothing pushes down, and the original row filter is to be used.
                mappedFilter = filter;
            } else {
                // The filter has been split, and a mapped filter expression must be made.
                mappedFilter = new ToExprVisitor(columns).apply(mappedRowFilter.reduceMore());
            }
        }

        boolean needsMapper = mappedRowFilter != TrueFilter.THE ||
            (projection != null && !projection.equals(fromProjection));

        String mappedOrderBy = null;

        if (needsMapper && fromProjMap != null) {
            // Additional columns might need to be projected by the "from" source.

            if (mappedFilter != null) {
                mappedFilter.gatherEvalColumns(fromType, fromProjMap, 0, fromProjection::add);
            }

            // Projected columns which were initially excluded must be projected too. Also
            // determine if any columns are ordered, for possibly performing sorting at the
            // mapper layer.

            boolean hasOrderBy = false;
            boolean orderByDerived = false;

            for (ProjExpr pe : projection) {
                if (pe.hasExclude()) {
                    int flags = pe.flags() & ~ProjExpr.F_EXCLUDE;
                    pe.gatherEvalColumns(fromType, fromProjMap, flags, fromProjection::add);
                }

                if (pe.hasOrderBy()) {
                    hasOrderBy = true;
                    if (pe.sourceColumn() == null) {
                        orderByDerived = true;
                    }
                }
            }

            if (orderByDerived || (hasOrderBy && mappedRowFilter != TrueFilter.THE)) {
                // Strip away order-by flags in the "from" projection and build an order-by
                // expression for the mapper layer to use.

                for (int i=0; i<fromProjection.size(); i++) {
                    ProjExpr pe = fromProjection.get(i);
                    fromProjection.set(i, pe.withNoOrderBy());
                }

                var b = new StringBuilder().append('{').append('*');

                for (ProjExpr pe : projection) {
                    if (pe.hasOrderBy()) {
                        b.append(", ");
                        pe.appendTo(b, true);
                    }
                }

                mappedOrderBy = b.append('}').toString();
            }
        }

        // Attempt to push down filtering and projection by replacing the "from" expression
        // with an UnmappedQueryExpr. This might still be the original "from" expression.
        from = UnmappedQueryExpr.make(-1, -1, from, unmappedRowFilter, fromProjection, maxArgument);

        if (!needsMapper) {
            return from;
        }

        // A Mapper is required.

        TupleType rowType;

        if (projection == null) {
            // Use the existing row type.
            rowType = fromType;
            projection = from.fullProjection();
        } else if (fromType.canRepresent(projection)) {
            // Use the existing row type.
            rowType = fromType;
        } else {
            // Use a custom row type.
            rowType = TupleType.make(projection);
        }

        RelationType type = RelationType.make
            (rowType, from.type().cardinality().filter(mappedRowFilter));

        return new MappedQueryExpr
            (-1, -1, type, from, mappedRowFilter, mappedFilter, projection, maxArgument,
             mappedOrderBy);
    }

    private static boolean hasRepeatedNonPureFunctions(RowFilter filter) {
        var visitor = new Visitor() {
            Set<Expr> nonPure;
            boolean hasRepeats;

            @Override
            public void visit(ColumnToArgFilter filter) {
                // Nothing to do.
            }

            @Override
            public void visit(ColumnToColumnFilter filter) {
                // Nothing to do.
            }

            @Override
            public void visit(ColumnToConstantFilter filter) {
                // Nothing to do.
            }

            @Override
            public void visit(ExprFilter filter) {
                var expr = filter.expression();
                if (!expr.isPureFunction()) {
                    if (nonPure == null) {
                        nonPure = new HashSet<>();
                    }
                    if (nonPure.add(expr)) {
                        hasRepeats = true;
                    }
                }
            }
        };

        filter.accept(visitor);

        return visitor.hasRepeats;
    }

    protected final RelationExpr mFrom;
    protected final RowFilter mRowFilter;
    protected final List<ProjExpr> mProjection;
    protected final int mMaxArgument;

    protected QueryExpr(int startPos, int endPos, RelationType type,
                        RelationExpr from, RowFilter rowFilter, List<ProjExpr> projection,
                        int maxArgument)
    {
        super(startPos, endPos, type.withProjection(projection));
        mFrom = from;
        mRowFilter = rowFilter;
        mProjection = projection;
        mMaxArgument = maxArgument;
    }

    @Override
    public final int maxArgument() {
        return mMaxArgument;
    }

    @Override
    public boolean isPureFunction() {
        if (!mFrom.isPureFunction()) {
            return false;
        }

        if (mProjection != null) {
            for (Expr expr : mProjection) {
                if (!expr.isPureFunction()) {
                    return false;
                }
            }
        }

        // Note that mRowFilter isn't checked. The filter used by the UnmappedQueryExpr
        // subclass never has ExprFilters, and so it's always pure. The MappedQueryExpr
        // subclass overrides this method and checks the filter expression too.

        return true;
    }

    /**
     * @param strict when true return null when projecting anything other than a plain column;
     * when false, those projections are dropped
     */
    protected final QuerySpec querySpec(boolean strict) {
        List<ProjExpr> projection = mProjection;

        if (projection == null) {
            return new QuerySpec(null, null, mRowFilter);
        }

        var projMap = new LinkedHashMap<String, ColumnInfo>(projection.size() * 2);
        var orderBy = new OrderBy(projection.size() * 2);
        boolean hasPaths = false;

        RowInfo info = RowInfo.find(mFrom.rowTypeClass());

        for (ProjExpr pe : projection) {
            if (pe.shouldExclude()) {
                continue;
            }

            ColumnInfo ci;
            if (!(pe.wrapped() instanceof ColumnExpr ce) || (ci = ce.tryFindColumn(info)) == null) {
                if (strict) {
                    return null;
                }
                continue;
            }

            hasPaths |= ce.isPath();

            projMap.put(ci.name, ci);

            if (pe.hasOrderBy()) {
                orderBy.put(ci.name, new OrderBy.Rule(ci, pe.applyOrderBy(ci.typeCode)));
            }
        }

        if (!hasPaths && projMap.size() == info.allColumns.size()) {
            // Full projection.
            projMap = null;
        }

        return new QuerySpec(projMap, orderBy, mRowFilter);
    }

    protected final void doEncodeKey(KeyEncoder enc) {
        mFrom.encodeKey(enc);
        enc.encodeString(mRowFilter.toString());
        enc.encodeExprs(mProjection);
    }

    @Override
    public final int hashCode() {
        int hash = mFrom.hashCode();
        hash = hash * 31 + mRowFilter.hashCode();
        hash = hash * 31 + Objects.hashCode(mProjection);
        return hash;
    }

    @Override
    public final boolean equals(Object obj) {
        return obj == this ||
            obj instanceof QueryExpr qe
            && getClass() == qe.getClass()
            && mFrom.equals(qe.mFrom)
            && mRowFilter.equals(qe.mRowFilter)
            && Objects.equals(mProjection, qe.mProjection);
    }

    @Override
    public final String toString() {
        return defaultToString();
    }


    protected final String rowQueryString() {
        var b = new StringBuilder();
        appendRowQueryString(b);
        return b.toString();
    }

    protected final void appendRowQueryString(StringBuilder b) {
        if (mProjection == null) {
            if (mRowFilter == TrueFilter.THE) {
                b.append("{*}");
            } else {
                mRowFilter.appendTo(b);
            }
        } else {
            b.append('{');

            int i = 0;
            for (ProjExpr pe : mProjection) {
                if (i > 0) {
                    b.append(", ");
                }
                pe.appendTo(b);
                i++;
            }

            b.append('}');

            if (mRowFilter != TrueFilter.THE) {
                b.append(' ');
                mRowFilter.appendTo(b);
            }
        }
    }

    @Override
    public final void appendTo(StringBuilder b) {
        // FIXME: Need to revise the syntax for the "from" portion. Pipeline syntax? It must
        // always be parseable, so perhaps no table at all for now?
        b.append('(');
        mFrom.appendTo(b);
        b.append(')').append(' ');

        appendRowQueryString(b);
    }
}
