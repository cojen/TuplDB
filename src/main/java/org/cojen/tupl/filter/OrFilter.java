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

import java.util.Arrays;

import org.cojen.tupl.rows.ColumnInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class OrFilter extends GroupFilter {
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
            } else {
                count++;
            }
        }

        if (count == len) {
            if (off != 0 || len != subFilters.length) {
                subFilters = Arrays.copyOfRange(subFilters, off, off + len);
            }
            return new OrFilter(subFilters);
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
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    void appendTo(StringBuilder b) {
        if (mSubFilters.length == 0) {
            b.append('F');
        } else {
            super.appendTo(b);
        }
    }

    @Override
    public boolean isDnf() {
        for (RowFilter sub : mSubFilters) {
            if (!sub.isDnf()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isCnf() {
        for (RowFilter sub : mSubFilters) {
            if (sub instanceof AndFilter || !sub.isCnf()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int isMatch(RowFilter filter) {
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
    public RowFilter[][] multiRangeExtract(boolean disjoint,
                                           boolean reverse, ColumnInfo... keyColumns)
    {
        if (mSubFilters.length <= 1) {
            return super.multiRangeExtract(disjoint, reverse, keyColumns);
        }
        GroupFilter.cReduceLimit.set(new long[1]);
        try {
            return doMultiRangeExtract(disjoint, reverse, keyColumns);
        } finally {
            GroupFilter.cReduceLimit.remove();
        }
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

            if (range[1] == null && range[2] == null && sub.equals(range[0])) {
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
                int which = reverse ? 2 : 1;
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
    final char opChar() {
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