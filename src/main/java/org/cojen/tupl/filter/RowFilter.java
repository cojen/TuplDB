/*
 *  Copyright 2021 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.filter;

import java.util.Objects;
import java.util.Set;

import org.cojen.tupl.rows.ColumnInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see Parser
 */
public abstract class RowFilter implements Comparable<RowFilter> {
    private final int mHash;

    RowFilter(int hash) {
        mHash = hash;
    }

    public abstract void accept(Visitor visitor);

    public abstract int numTerms();

    /**
     * Apply partial or full reduction of the filter.
     */
    public abstract RowFilter reduce();

    /**
     * @return true if this filter is in disjunctive normal form
     */
    public abstract boolean isDnf();

    /**
     * Returns this filter in disjunctive normal form.
     *
     * @throws ComplexFilterException if cannot be quickly transformed
     */
    public abstract RowFilter dnf();

    /**
     * @return true if this filter is in conjunctive normal form
     */
    public abstract boolean isCnf();

    /**
     * Returns this filter in conjunctive normal form.
     *
     * @throws ComplexFilterException if cannot be quickly transformed
     */
    public abstract RowFilter cnf();

    /**
     * Checks if the given filter (or its inverse) matches this one. If the filter consists of
     * a commutative group of sub-filters, then exact order isn't required for a match.
     *
     * @return 0 if doesn't match, 1 if equal match, or -1 if inverse equally matches
     */
    public abstract int isMatch(RowFilter filter);

    /**
     * Checks if the given filter (or its inverse) matches this one, or if the given filter
     * matches against a sub-filter of this one.
     *
     * @return 0 if doesn't match, 1 if equal match, or -1 if inverse equally matches
     */
    public abstract int isSubMatch(RowFilter filter);

    /**
     * Returns a hash code for use with the isMatch method.
     */
    public abstract int matchHashCode();

    /**
     * Returns the inverse of this filter.
     */
    public abstract RowFilter not();

    public RowFilter or(RowFilter filter) {
        Objects.requireNonNull(filter);
        RowFilter[] subFilters = {this, filter};
        return OrFilter.flatten(subFilters, 0, subFilters.length);
    }

    public RowFilter and(RowFilter filter) {
        Objects.requireNonNull(filter);
        RowFilter[] subFilters = {this, filter};
        return AndFilter.flatten(subFilters, 0, subFilters.length);
    }

    /**
     * Returns this filter with a canonical sort order, for performing equivalence comparisons.
     */
    public abstract RowFilter sort();

    /**
     * Re-orders the terms of this filter such that the given columns are evaluated first.
     */
    // TODO: Intended for use by RowPredicate::test(R, byte[]) to examine keys first.
    public abstract RowFilter prioritize(Set<ColumnInfo> columns);

    /**
     * Remove terms which refer to columns which aren't in the given set.
     *
     * @param columns columns to remove (not retain)
     * @param undecided default filter to use when the resulting filter cannot be certain of a
     * match (usually TRUE or FALSE)
     */
    // TODO: Intended for use by RowPredicate::test(byte[]), with TRUE for undecided.
    public abstract RowFilter retain(Set<ColumnInfo> columns, RowFilter undecided);

    /**
     * Given a set of columns corresponding to the primary key of an index, extract a suitable
     * range for performing an efficient index scan against this filter. For best results, this
     * method should be called on a conjunctive normal form filter.
     *
     * <ul>
     * <li>The remaining filter that must be applied, or null if none
     * <li>A range low filter, or null if open
     * <li>A range high filter, or null if open
     * </ul>
     *
     * If no optimization is possible, then the remaining filter is the same as this, and the
     * range filters are both null (open).
     *
     * <p>The range filters are composed of the key columns, in their original order. If
     * multiple key columns are used, they are combined as an 'and' filter. The number of terms
     * never exceeds the number of key columns provided.
     *
     * <p>The last operator of the low range is >= or >, and the last operator of the high
     * range is <= or <. All prior operators (if any) are always ==. For "fuzzy" BigDecimal
     * matches, the last operator is always ==.
     *
     * @param keyColumns must provide at least one
     */
    public RowFilter[] rangeExtract(ColumnInfo... keyColumns) {
        return new RowFilter[] {this, null, null};
    }

    /**
     * Given a set of columns corresponding to the primary key of an index, extract ranges for
     * performing an efficient index scan against this filter. For best results, this method
     * should be called on a disjunctive normal form filter.
     *
     * <p>For each range, a separate scan must be performed, and they can be stitched together
     * as one. The order of the ranges doesn't match the natural order of the index, and it
     * cannot be known until actual argument values are specified.
     *
     * <p>An array of arrays is returned, where each range is described by the
     * {@see #rangeExtract} method.
     *
     * @param disjoint pass true to extract disjoint ranges
     * @param reverse pass true if scan is to be performed in reverse order; note that the
     * returned ranges are never swapped
     * @param keyColumns must provide at least one
     * @throws ComplexFilterException if cannot be quickly reduced; call rangeExtract instead
     */
    public RowFilter[][] multiRangeExtract(boolean disjoint,
                                           boolean reverse, ColumnInfo... keyColumns)
    {
        RowFilter[] range = rangeExtract(keyColumns);
        return range == null ? null : new RowFilter[][] {range};
    }

    @Override
    public final int hashCode() {
        return mHash;
    }

    @Override
    public final String toString() {
        var b = new StringBuilder();
        appendTo(b);
        return b.toString();
    }

    abstract void appendTo(StringBuilder b);

    @Override
    public int compareTo(RowFilter filter) {
        return getClass().getName().compareTo(filter.getClass().getName());
    }
}
