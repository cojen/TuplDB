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

package org.cojen.tupl.table.filter;

import java.util.Map;
import java.util.Objects;

import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

import org.cojen.tupl.table.ColumnInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see Parser
 */
public abstract sealed class RowFilter implements Comparable<RowFilter>
    permits TermFilter, GroupFilter
{
    private final int mHash;

    RowFilter(int hash) {
        mHash = hash;
    }

    public abstract void accept(Visitor visitor);

    public abstract int numTerms();

    /**
     * @return 0 if filter has no arguments
     */
    public final int maxArgument() {
        return maxArgument(0);
    }

    /**
     * @return max if filter has no arguments
     */
    protected abstract int maxArgument(int max);

    /**
     * Apply partial or full reduction of the filter.
     */
    public abstract RowFilter reduce();

    /**
     * Attempt a more aggressive reduction.
     */
    public final RowFilter reduceMore() {
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
     * @param limit complexity limit
     * @param merge when true, also perform operator reduction
     */
    abstract RowFilter reduce(long limit, boolean merge);

    /**
     * Returns this filter with some operators expanded into "or" filters.
     *
     * <ul>
     * <li>"a >= ?1" expands to "a > ?1 || a == ?1"
     * <li>"a <= ?1" expands to "a < ?1 || a == ?1"
     * </ul>
     *
     * @param force when false, only expand if the filter has multiple levels
     */
    abstract RowFilter expandOperators(boolean force);

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
     * @param limit complexity limit
     * @param merge when true, also perform operator reduction
     */
    abstract RowFilter dnf(long limit, boolean merge);

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
     * @param limit complexity limit
     * @param merge when true, also perform operator reduction
     */
    abstract RowFilter cnf(long limit, boolean merge);

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
     * Returns this filter modified such that all arguments matched by the given function are
     * replaced. If no arguments have changed, then the original filter instance is returned.
     */
    public abstract RowFilter replaceArguments(IntUnaryOperator function);

    /**
     * Returns this filter modified such that comparisons by non-nullable columns to the given
     * argument are compared to null. This causes the affected terms to always return true or
     * false, depending on the comparison operator. If no terms are affected, then the original
     * filter instance is returned.
     */
    public abstract RowFilter argumentAsNull(int argNum);

    /**
     * Returns this filter modified such that all constants matched by the given function are
     * converted to arguments. If no constants have been replaced, then the original filter
     * instance is returned.
     *
     * @param function accepts a constant filter and returns an argument; it can return 0 if
     * the filter shouldn't be converted
     */
    public abstract RowFilter constantsToArguments(ToIntFunction<ColumnToConstantFilter> function);

    /**
     * Remove terms which refer to columns which aren't in the given set. A strict parameter
     * controls the behavior of column-to-column terms. When true, both columns must be
     * retained columns. When false, at least one column must be a retained column.
     *
     * @param pred checks if column name should be retained (not removed)
     * @param strict true if returned filter must only refer to the given columns
     * @param undecided default filter to use when the resulting filter cannot be certain of a
     * match (usually TrueFilter or FalseFilter)
     */
    public abstract RowFilter retain(Predicate<String> pred, boolean strict, RowFilter undecided);

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
     * @param check function which checks a ColumnFilter subclass; return null if it cannot
     * be extracted, return the original or a replacement filter otherwise
     * @param split the two filters are stored here
     */
    public void split(Function<ColumnFilter, RowFilter> check, RowFilter[] split) {
        RowFilter extracted = trySplit(check);
        if (extracted != null) {
            split[0] = extracted;
            split[1] = TrueFilter.THE;
        } else {
            split[0] = TrueFilter.THE;
            split[1] = this;
        }
    }

    /**
     * Split variant which operates against a column map.
     */
    public final void split(Map<String, ? extends ColumnInfo> columns, RowFilter[] split) {
        split((ColumnFilter filter) -> filter.canSplit(columns) ? filter : null, split);
    }

    protected abstract RowFilter trySplit(Function<ColumnFilter, RowFilter> check);

    /**
     * Combine the split result together with 'and'.
     */
    protected void splitCombine(Function<ColumnFilter, RowFilter> check, RowFilter[] split) {
        RowFilter extracted = trySplit(check);
        if (extracted != null) {
            split[0] = split[0].and(extracted);
        } else {
            split[1] = split[1].and(this);
        }
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
     * For each result from the {@link #multiRangeExtract} method with a remainder, {@link
     * #split} it into the last two elements of the range array.
     *
     * @param columns columns to extract
     * @param ranges result from calling multiRangeExtract
     */
    public static void splitRemainders(Map<String, ? extends ColumnInfo> columns,
                                       RowFilter[]... ranges)
    {
        splitRemainders((ColumnFilter filter) -> filter.canSplit(columns) ? filter : null, ranges);
    }

    /**
     * For each result from the {@link #multiRangeExtract} method with a remainder, {@link
     * #split} it into the last two elements of the range array.
     *
     * @param check function which checks a ColumnFilter subclass; return null if it cannot
     * be extracted, return the original or a replacement filter otherwise
     * @param ranges result from calling multiRangeExtract
     */
    public static void splitRemainders(Function<ColumnFilter, RowFilter> check,
                                       RowFilter[]... ranges)
    {
        RowFilter[] split = null;

        for (RowFilter[] range : ranges) {
            RowFilter remainder = range[2];
            if (remainder != null) {
                RowFilter original = remainder;
                try {
                    remainder = remainder.cnf();
                } catch (ComplexFilterException e) {
                }

                if (split == null) {
                    split = new RowFilter[2];
                }
                remainder.split(check, split);

                if (split[0] == TrueFilter.THE) {
                    split[1] = reduceFromCnf(original, remainder);
                } else if (split[1] == TrueFilter.THE) {
                    split[0] = reduceFromCnf(original, remainder);
                }
                
                range[2] = split[0];
                range[3] = split[1];
            }
        }
    }

    /**
     * @param cnf the cnf version of the from filter
     */
    private static RowFilter reduceFromCnf(RowFilter from, RowFilter cnf) {
        int fromTerms = from.numTerms();
        if (cnf.numTerms() < fromTerms) {
            // Reduced enough already.
            return cnf;
        }

        // Try converting to dnf to see if that reduces it.
        try {
            RowFilter dnf = cnf.dnf();
            if (dnf.numTerms() < fromTerms) {
                return dnf;
            }
        } catch (ComplexFilterException e) {
        }

        return from;
    }

    /**
     * Returns true if the bounding range defined by this low filter and the given high filter
     * matches at most one row. False is returned if a column is a "fuzzy" BigDecimal match.
     */
    public boolean matchesOne(RowFilter high, ColumnInfo... keyColumns) {
        return false;
    }

    /**
     * Returns true if all rows which match this filter will have the same value for the given
     * column. False is returned if the column is a "fuzzy" BigDecimal match.
     */
    public abstract boolean uniqueColumn(String columnName);

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

    public abstract void appendTo(StringBuilder b);

    @Override
    public int compareTo(RowFilter filter) {
        return getClass().getName().compareTo(filter.getClass().getName());
    }
}
