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

import java.util.Arrays;

import java.util.function.Predicate;

import org.cojen.tupl.table.ColumnInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public sealed class OrFilter extends GroupFilter permits FalseFilter {
    /**
     * Combines the given filters together into a flattened OrFilter or just one RowFilter.
     * This operation isn't recursive -- it only applies one round of flattening.
     */
    static RowFilter flatten(RowFilter[] subFilters, int off, int len) {
        if (len == 1) {
            return subFilters[off];
        }

        if (len == 0) {
            return FalseFilter.THE;
        }

        int count = 0;
        for (int i=off; i<off+len; i++) {
            RowFilter sub = subFilters[i];
            if (sub instanceof OrFilter of) {
                count += of.mSubFilters.length;
            } else if (sub instanceof TrueFilter) {
                return TrueFilter.THE;
            } else {
                count++;
            }
        }

        if (count == 0) {
            return FalseFilter.THE;
        }

        var newSubFilters = new RowFilter[count];
        for (int i=off, j=0; j<newSubFilters.length; ) {
            RowFilter sub = subFilters[i++];
            if (sub instanceof OrFilter of) {
                var orSubFilters = of.mSubFilters;
                for (int k=0; k<orSubFilters.length; k++) {
                    newSubFilters[j++] = orSubFilters[k];
                }
            } else {
                newSubFilters[j++] = sub;
            }
        }

        if (newSubFilters.length == 1) {
            return newSubFilters[0];
        }

        return new OrFilter(newSubFilters);
    }

    /**
     * Construct from sub-filters which themselves must not directly be OrFilters.
     */
    OrFilter(RowFilter... subFilters) {
        super(~Arrays.hashCode(subFilters), subFilters);
    }

    @Override
    public final void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public final void appendTo(StringBuilder b) {
        if (mSubFilters.length == 0) {
            b.append("false");
        } else {
            super.appendTo(b);
        }
    }

    @Override
    final RowFilter expandOperators(boolean force) {
        RowFilter expanded = super.expandOperators(force);
        if (expanded == this) {
            return this;
        }
        RowFilter[] subFilters = ((OrFilter) expanded).mSubFilters;
        return flatten(subFilters, 0, subFilters.length);
    }

    @Override
    public final boolean isDnf() {
        if ((mFlags & FLAG_DNF_SET) != 0) {
            return (mFlags & FLAG_IS_DNF) != 0;
        }
        for (RowFilter sub : mSubFilters) {
            if (!sub.isDnf()) {
                mFlags |= FLAG_DNF_SET;
                return false;
            }
        }
        mFlags |= FLAG_DNF_SET | FLAG_IS_DNF;
        mDnf = this;
        return true;
    }

    @Override
    public final boolean isCnf() {
        if ((mFlags & FLAG_CNF_SET) != 0) {
            return (mFlags & FLAG_IS_CNF) != 0;
        }
        for (RowFilter sub : mSubFilters) {
            if (sub instanceof AndFilter || !sub.isCnf()) {
                mFlags |= FLAG_CNF_SET;
                return false;
            }
        }
        mFlags |= FLAG_CNF_SET | FLAG_IS_CNF;
        mCnf = this;
        return true;
    }

    @Override
    public final int isMatch(RowFilter filter) {
        if (equals(filter)) {
            return 1; // equal
        }
        if (filter instanceof OrFilter of) {
            return matchSet().equalMatches(of.matchSet());
        }
        if (filter instanceof AndFilter af) {
            return matchSet().inverseMatches(af.matchSet());
        }
        return 0; // doesn't match
    }

    @Override
    public AndFilter not() {
        return new AndFilter(subNot());
    }

    @Override
    public final RowFilter retain(Predicate<String> pred, boolean strict, RowFilter undecided) {
        RowFilter[] subFilters = mSubFilters;
        if (subFilters.length == 0) {
            return this;
        }

        subFilters = mSubFilters.clone();

        int len = 0;
        for (int i=0; i<subFilters.length; i++) {
            RowFilter sub = subFilters[i].retain(pred, strict, undecided);
            if (sub == TrueFilter.THE) {
                return sub;
            }
            if (sub != FalseFilter.THE) {
                subFilters[len++] = sub;
            }
        }

        return newInstance(subFilters, 0, len);
    }

    @Override
    public final RowFilter[][] multiRangeExtract(boolean disjoint,
                                                 boolean reverse, ColumnInfo... keyColumns)
    {
        if (mSubFilters.length <= 1) {
            return super.multiRangeExtract(disjoint, reverse, keyColumns);
        }
        return doMultiRangeExtract(disjoint, reverse, keyColumns);
    }

    private RowFilter[][] doMultiRangeExtract(boolean disjoint,
                                              boolean reverse, ColumnInfo... keyColumns)
    {
        RowFilter[] subFilters = mSubFilters;

        RowFilter[] rangeFilters = null;
        RowFilter[][] ranges = null;
        int numRanges = 0;

        outer: for (int i=0; i<subFilters.length; i++) {
            RowFilter sub = subFilters[i];

            RowFilter[] range = sub.rangeExtract(keyColumns);

            if (range[0] == null && range[1] == null && sub.equals(range[2])) {
                // Full scan.
                return super.multiRangeExtract(disjoint, reverse, keyColumns);
            }

            if (numRanges == 0) {
                rangeFilters = new RowFilter[subFilters.length];
                ranges = new RowFilter[subFilters.length][];
                rangeFilters[0] = sub;
                ranges[0] = range;
                numRanges++;
                continue outer;
            }

            // Check if the range can merge with another one. Check against the low range, or
            // the high range if a reverse scan is to be performed.

            for (int j=0; j<numRanges; j++) {
                int which = reverse ? 1 : 0;
                RowFilter check = ranges[j][which];
                if (check == null || range[which] == null) {
                    continue;
                }
                if (check.isSubMatch(range[which]) > 0 || range[which].isSubMatch(check) > 0) {
                    RowFilter mergedFilter = sub.or(rangeFilters[j]).cnf();
                    RowFilter[] mergedRange = mergedFilter.rangeExtract(keyColumns);
                    rangeFilters[j] = mergedFilter;
                    ranges[j] = mergedRange;
                    continue outer;
                }
            }

            rangeFilters[numRanges] = sub;
            ranges[numRanges] = range;
            numRanges++;
        }

        if (disjoint) {
            // Modify the range filters to be disjoint and rebuild the ranges.
            for (int i=1; i<numRanges; i++) {
                rangeFilters[i] = rangeFilters[i].and(rangeFilters[i - 1].not()).reduce();
                ranges[i] = rangeFilters[i].rangeExtract(keyColumns);
            }
        }

        if (numRanges < ranges.length) {
            ranges = Arrays.copyOfRange(ranges, 0, numRanges);
        }

        return ranges;
    }

    @Override
    public final boolean uniqueColumn(String columnName) {
        // To return true, none of the sub filters can match more than one, and all of the sub
        // filters must be the same. In practice, this method will always return false due to
        // filter reduction.
        RowFilter prev = null;
        for (RowFilter sub : mSubFilters) {
            if (!sub.uniqueColumn(columnName)) {
                return false;
            }
            if (prev != null && !prev.equals(sub)) {
                return false;
            }
            prev = sub;
        }
        return true;
    }

    @Override
    public final char opChar() {
        return '|';
    }

    @Override
    final RowFilter newInstance(RowFilter[] subFilters, int off, int len) {
        return OrFilter.flatten(subFilters, off, len);
    }

    @Override
    final RowFilter newFlippedInstance(RowFilter... subFilters) {
        return AndFilter.flatten(subFilters, 0, subFilters.length);
    }

    @Override
    final RowFilter emptyInstance() {
        return FalseFilter.THE;
    }

    @Override
    final RowFilter emptyFlippedInstance() {
        return TrueFilter.THE;
    }

    @Override
    final int reduceOperator(ColumnFilter a, ColumnFilter b) {
        return a.reduceOperatorForOr(b);
    }
}
