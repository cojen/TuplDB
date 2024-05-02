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

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.cojen.tupl.table.RowInfo;

import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.ColumnToConstantFilter;
import org.cojen.tupl.table.filter.ComplexFilterException;
import org.cojen.tupl.table.filter.OpaqueFilter;
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
     * convertable to boolean
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

        TupleType fromType = from.type().rowType();

        int maxArgument = from.maxArgument();

        /*
          Maps source column names to target names (renames). Is null when:

          - the projection is null (all columns are projected)
          - or the requested projection doesn't refer to a column
          - or any source column is a path
          - or any source column is projected more than once
        */
        Map<String, String> pureProjection;

        pure: if (projection == null) {
            pureProjection = null;
        } else {
            for (ProjExpr expr : projection) {
                maxArgument = Math.max(maxArgument, expr.maxArgument());
            }

            pureProjection = new LinkedHashMap<String, String>();

            for (ProjExpr pe : projection) {
                if (!(pe.wrapped() instanceof ColumnExpr ce)) {
                    // Not projecting a column.
                    pureProjection = null;
                    break pure;
                }
                Column c = ce.column();
                if (c.subNames().size() > 1) {
                    // Projecting a path.
                    pureProjection = null;
                    break pure;
                }
                if (pureProjection.putIfAbsent(c.name(), pe.name()) != null) {
                    // Column is projected more than once.
                    pureProjection = null;
                    break pure;
                }
            }

            if (fromType.matchesNames(pureProjection.keySet())) {
                // All columns are projected, in the natural order, with no renames.
                projection = null;
                pureProjection = null;
            }
        }

        // Split the filter such that the unmapped part can be pushed down. The remaining
        // mapped part must be handled by a Mapper. The unmapped part is guaranteed to have no
        // OpaqueFilters. If it did, UnmappedQueryExpr would produce a broken query string.
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
                // Nothing will push down, so just use the original row filter.
                mappedRowFilter = originalRowFilter;
                mappedFilter = filter;
            } else if (mappedRowFilter == TrueFilter.THE) {
                mappedFilter = null;
            } else if (mappedRowFilter == originalRowFilter) {
                mappedFilter = filter;
            } else {
                mappedFilter = new ToExprVisitor(columns).apply(mappedRowFilter.reduceMore());
            }
        }

        // Attempt to push down filtering and projection by replacing the "from" expression
        // with an UnmappedQueryExpr.
        pushDown: {
            List<ProjExpr> fromProjection;

            if (mappedRowFilter == TrueFilter.THE && projection != null && pureProjection != null) {
                // Can push down filtering and projection entirely; no Mapper is needed.
                fromType = TupleType.make(fromType.clazz(), pureProjection);
                fromProjection = projection;
                projection = null;
            } else if (unmappedRowFilter == TrueFilter.THE) {
                // There's no filter and all columns need to be projected, so there's nothing
                // to push down.
                break pushDown;
            } else {
                fromProjection = null;
            }

            from = UnmappedQueryExpr.make
                (-1, -1, fromType, from, unmappedRowFilter, fromProjection, maxArgument);
        }

        if (mappedRowFilter == TrueFilter.THE && projection == null) {
            return from;
        }

        // A Mapper is required.

        TupleType type;

        if (projection == null) {
            // Use the existing row type.
            type = from.type().rowType();
            projection = from.fullProjection();
        } else {
            // Use a custom row type.
            type = TupleType.make(projection);
        }

        return new MappedQueryExpr
            (-1, -1, type, from, mappedRowFilter, mappedFilter, projection, maxArgument);
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
            public void visit(OpaqueFilter filter) {
                var expr = (Expr) filter.attachment();
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

    private String mRowFilterString;

    private TableProvider<?> mTableProvider;

    protected QueryExpr(int startPos, int endPos, TupleType type,
                        RelationExpr from, RowFilter rowFilter, List<ProjExpr> projection,
                        int maxArgument)
    {
        this(startPos, endPos, RelationType.make(type, from.type().cardinality()),
             from, rowFilter, projection, maxArgument);
    }

    protected QueryExpr(int startPos, int endPos, RelationType type,
                        RelationExpr from, RowFilter rowFilter, List<ProjExpr> projection,
                        int maxArgument)
    {
        super(startPos, endPos, type);
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

        // Note that mRowFilter isn't checked. The filter used by the UnmappedQueryExpr subclass
        // never has OpaqueFilters, and so it's always pure. The MappedQueryExpr subclass
        // overrides this method and checks the filter expression too.

        return true;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected final void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            mFrom.encodeKey(enc);
            enc.encodeString(rowFilterString());
            enc.encodeExprs(mProjection);
        }
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
            && mFrom.equals(qe.mFrom)
            && mRowFilter.equals(qe.mRowFilter)
            && Objects.equals(mProjection, qe.mProjection);
    }

    @Override
    public final String toString() {
        return defaultToString();
    }

    @Override
    public final TableProvider<?> makeTableProvider() {
        if (mTableProvider == null) {
            mTableProvider = doMakeTableProvider();
        }
        return mTableProvider;
    }

    protected abstract TableProvider<?> doMakeTableProvider();

    protected final String rowFilterString() {
        if (mRowFilterString == null) {
            mRowFilterString = mRowFilter.toString();
        }
        return mRowFilterString;
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
                b.append(pe);
                i++;
            }

            b.append('}');

            if (mRowFilter != TrueFilter.THE) {
                b.append(' ').append(rowFilterString());
            }
        }
    }

    @Override
    protected final void appendTo(StringBuilder b) {
        // FIXME: Need to revise the syntax for the "from" portion. Pipeline syntax?
        b.append('(');
        mFrom.appendTo(b);
        b.append(')').append(' ');

        appendRowQueryString(b);
    }
}
