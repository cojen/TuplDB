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

import java.util.Map;
import java.util.Objects;

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
     * Attempt a more aggressive reduction.
     */
    public RowFilter reduceMore() {
        RowFilter filter = reduce();
        int numTerms = -1;

        try {
            RowFilter dnf = filter.dnf();
            numTerms = filter.numTerms();
            if (dnf.numTerms() < numTerms) {
                return dnf;
            }
        } catch (ComplexFilterException e) {
        }

        try {
            RowFilter cnf = filter.cnf();
            if (numTerms < 0) {
                numTerms = filter.numTerms();
            }
            if (cnf.numTerms() < numTerms) {
                return cnf;
            }
        } catch (ComplexFilterException e) {
        }

        return filter;
    }

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
    public abstract RowFilter prioritize(Map<String, ColumnInfo> columns);

    /**
     * Returns true if this filter only uses the given columns.
     */
    public abstract boolean onlyUses(Map<String, ColumnInfo> columns);

    /**
     * Remove terms which refer to columns which aren't in the given set.
     *
     * @param columns columns to retain (not remove)
     * @param undecided default filter to use when the resulting filter cannot be certain of a
     * match (usually TRUE or FALSE)
     */
    public abstract RowFilter retain(Map<String, ColumnInfo> columns, RowFilter undecided);

    /**
     * Split this filter by extracting columns such that the first returned filter only
     * references those columns. The second returned filter references the remaining columns,
     * but it also may reference the given columns if they couldn't be fully extracted. For
     * best results, this method should be called on a conjunctive normal form filter.
     *
     * <p>The two returned filters can always be recombined with the 'and' method. If nothing
     * could be extracted, the first filter is TrueFilter, and the remainder is this filter. If
     * there's no remainder, it's represented by TrueFilter.
     *
     * <p>Note: No attempt is made to reduce the returned filters.
     *
     * @param columns columns to extract
     * @return two filters: the first doesn't reference the extracted columns and the second does
     */
    public RowFilter[] split(Map<String, ColumnInfo> columns) {
        var result = new RowFilter[2];
        if (onlyUses(columns)) {
            result[0] = this;
            result[1] = TrueFilter.THE;
        } else {
            result[0] = TrueFilter.THE;
            result[1] = this;
        }
        return result;
    }

    /**
     * Combine the split result together with 'and'.
     */
    protected void splitCombine(Map<String, ColumnInfo> columns, RowFilter[] result) {
        int ix = onlyUses(columns) ? 0 : 1;
        result[ix] = result[ix].and(this);
    }

    /**
     * Given a set of columns corresponding to the primary key of an index, extract a suitable
     * range for performing an efficient index scan against this filter. For best results, this
     * method should be called on a conjunctive normal form filter.
     *
     * <ul>
     * <li>A range low filter, or null if open
     * <li>A range high filter, or null if open
     * <li>The remaining filter that must be applied, or null if none
     * <li>A null array element, which the caller can use if splitting the remainder
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
        return new RowFilter[] {null, null, this, null};
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
     * {@link #rangeExtract} method.
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

    /**
     * For each result from the {@link multiRangeExtract} method with a remainder, {@link
     * split} it into the last two elements of the range array.
     *
     * @param columns columns to extract
     * @param ranges result from calling multiRangeExtract
     */
    public static void splitRemainders(Map<String, ColumnInfo> columns, RowFilter[]... ranges) {
        for (RowFilter[] range : ranges) {
            RowFilter remainder = range[2];
            if (remainder != null) {
                RowFilter original = remainder;
                try {
                    remainder = remainder.cnf();
                } catch (ComplexFilterException e) {
                }

                RowFilter[] split = remainder.split(columns);

                if (split[0] == TrueFilter.THE && !isReduced(original, remainder)) {
                    split[1] = original;
                } else if (split[1] == TrueFilter.THE && !isReduced(original, remainder)) {
                    split[0] = original;
                }
                
                range[2] = split[0];
                range[3] = split[1];
            }
        }
    }

    private static boolean isReduced(RowFilter from, RowFilter to) {
        return to.numTerms() < from.numTerms();
    }

    /**
     * Returns true if the given range (as provided by rangeExtract or multiRangeExtract)
     * exactly matches one row.
     */
    public static boolean matchesOne(RowFilter[] range, ColumnInfo... keyColumns) {
        if (range[0] != null) {
            return false;
        }
        RowFilter low = range[1];
        RowFilter high = range[2];
        return low != null && high != null && low.matchesOne(high, keyColumns);
    }

    /**
     * Returns true if this low filter and the given high filter fully matches all the given
     * key columns. False is returned if a column is a "fuzzy" match.
     */
    boolean matchesOne(RowFilter high, ColumnInfo... keyColumns) {
        return false;
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
