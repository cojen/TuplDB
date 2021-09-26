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

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class OrFilter extends GroupFilter {
    /**
     * An empty 'or' filter always evaluates to false.
     */
    static final OrFilter FALSE;

    static {
        FALSE = new OrFilter();
        FALSE.mReduced = true;
    }

    /**
     * Combines the given filters together into a flattened OrFilter or just one RowFilter.
     * This operation isn't recursive -- it only applies one round of flattening.
     */
    static RowFilter flatten(RowFilter[] subFilters, int off, int len) {
        if (len == 1) {
            return subFilters[off];
        }

        if (len == 0) {
            return FALSE;
        }

        int count = 0;
        for (int i=off; i<off+len; i++) {
            RowFilter sub = subFilters[i];
            if (sub instanceof OrFilter) {
                count += ((OrFilter) sub).mSubFilters.length;
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
           return FALSE;
        }

        var newSubFilters = new RowFilter[count];
        for (int i=off, j=0; j<newSubFilters.length; ) {
            RowFilter sub = subFilters[i++];
            if (sub instanceof OrFilter) {
                var orSubFilters = ((OrFilter) sub).mSubFilters;
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
        if (filter instanceof OrFilter) {
            return matchSet().equalMatches(((OrFilter) filter).matchSet());
        }
        if (filter instanceof AndFilter) {
            return matchSet().inverseMatches(((AndFilter) filter).matchSet());
        }
        return 0; // doesn't match
    }

    @Override
    public AndFilter not() {
        return new AndFilter(subNot());
    }

    @Override
    char opChar() {
        return '|';
    }

    @Override
    RowFilter newInstance(RowFilter[] subFilters, int off, int len) {
        return OrFilter.flatten(subFilters, off, len);
    }

    @Override
    RowFilter newFlippedInstance(RowFilter... subFilters) {
        return AndFilter.flatten(subFilters, 0, subFilters.length);
    }

    @Override
    RowFilter emptyInstance() {
        return FALSE;
    }

    @Override
    RowFilter emptyFlippedInstance() {
        return AndFilter.TRUE;
    }

    @Override
    int reduceOperator(ColumnFilter a, ColumnFilter b) {
        return a.reduceOperatorForOr(b);
    }
}
