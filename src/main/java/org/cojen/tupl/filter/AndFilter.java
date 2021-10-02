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

import static org.cojen.tupl.filter.ColumnFilter.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class AndFilter extends GroupFilter {
    /**
     * Combines the given filters together into a flattened AndFilter or just one RowFilter.
     * This operation isn't recursive -- it only applies one round of flattening.
     */
    static RowFilter flatten(RowFilter[] subFilters, int off, int len) {
        if (len == 1) {
            return subFilters[off];
        }

        if (len == 0) {
            return TrueFilter.THE;
        }

        int count = 0;
        for (int i=off; i<off+len; i++) {
            RowFilter sub = subFilters[i];
            if (sub instanceof AndFilter) {
                count += ((AndFilter) sub).mSubFilters.length;
            } else {
                count++;
            }
        }

        if (count == len) {
            if (off != 0 || len != subFilters.length) {
                subFilters = Arrays.copyOfRange(subFilters, off, off + len);
            }
            return new AndFilter(subFilters);
        }

        if (count == 0) {
            return TrueFilter.THE;
        }

        var newSubFilters = new RowFilter[count];
        for (int i=off, j=0; j<newSubFilters.length; ) {
            RowFilter sub = subFilters[i++];
            if (sub instanceof AndFilter) {
                var andSubFilters = ((AndFilter) sub).mSubFilters;
                for (int k=0; k<andSubFilters.length; k++) {
                    newSubFilters[j++] = andSubFilters[k];
                }
            } else {
                newSubFilters[j++] = sub;
            }
        }

        if (newSubFilters.length == 1) {
            return newSubFilters[0];
        }

        return new AndFilter(newSubFilters);
    }

    /**
     * Construct from sub-filters which themselves must not directly be AndFilters.
     */
    AndFilter(RowFilter... subFilters) {
        super(Arrays.hashCode(subFilters), subFilters);
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    void appendTo(StringBuilder b) {
        if (mSubFilters.length == 0) {
            b.append('T');
        } else {
            super.appendTo(b);
        }
    }

    @Override
    public boolean isDnf() {
        for (RowFilter sub : mSubFilters) {
            if (sub instanceof OrFilter || !sub.isDnf()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean isCnf() {
        for (RowFilter sub : mSubFilters) {
            if (!sub.isCnf()) {
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
        if (filter instanceof AndFilter) {
            return matchSet().equalMatches(((AndFilter) filter).matchSet());
        }
        if (filter instanceof OrFilter) {
            return matchSet().inverseMatches(((OrFilter) filter).matchSet());
        }
        return 0; // doesn't match
    }

    @Override
    public OrFilter not() {
        return new OrFilter(subNot());
    }

    @Override
    public RowFilter[] rangeExtract(boolean reverse, ColumnInfo... keyColumns) {
        if (!mReduced) {
            return reduce().rangeExtract(reverse, keyColumns);
        }

        RowFilter[] subFilters = mSubFilters;

        ColumnToArgFilter[] lowTerms = null, highTerms = null;

        keys: for (int k = 0; k < keyColumns.length; k++) {
            String keyName = keyColumns[k].name;

            for (int match = 0; match <= 2; match++) {
                for (int s = 0; s < subFilters.length; s++) {
                    RowFilter sub = subFilters[s];

                    if (!(sub instanceof ColumnToArgFilter)) {
                        continue;
                    }

                    var term = (ColumnToArgFilter) sub;
                    if (!keyName.equals(term.mColumn.name)) {
                        continue;
                    }

                    int op = term.mOperator;

                    if (match == 0) {
                        if (op == OP_EQ) {
                            if (lowTerms == null) {
                                lowTerms = new ColumnToArgFilter[keyColumns.length];
                            }
                            if (highTerms == null) {
                                highTerms = new ColumnToArgFilter[keyColumns.length];
                            }
                            lowTerms[k] = term;
                            highTerms[k] = term;
                            subFilters = removeSub(subFilters, s);
                            continue keys;
                        }
                    } else if ((match == 1 && !reverse) || (match != 1 && reverse)) {
                        if (op == OP_GT || op == OP_GE) {
                            if (lowTerms == null) {
                                lowTerms = new ColumnToArgFilter[keyColumns.length];
                            }
                            if (lowTerms[k] == null) {
                                lowTerms[k] = term;
                                subFilters = removeSub(subFilters, s);
                            }
                        }
                    } else {
                        if (op == OP_LT || op == OP_LE) {
                            if (highTerms == null) {
                                highTerms = new ColumnToArgFilter[keyColumns.length];
                            }
                            if (highTerms[k] == null) {
                                highTerms[k] = term;
                                subFilters = removeSub(subFilters, s);
                            }
                        }
                    }
                }
            }

            // Is either a range match against the key column or none, so can't continue.
            break keys;
        }

        RowFilter lowRange = combineRange(lowTerms, OP_GE);
        RowFilter highRange = combineRange(highTerms, OP_LE);

        RowFilter remaining = this;

        if (subFilters != mSubFilters) {
            remaining = flatten(subFilters, 0, subFilters.length);
            if (remaining == TrueFilter.THE) {
                remaining = null;
            }
        }

        return new RowFilter[] {remaining, lowRange, highRange};
    }

    private RowFilter[] removeSub(RowFilter[] subFilters, int s) {
        if (subFilters == mSubFilters) {
            subFilters = mSubFilters.clone();
        }
        subFilters[s] = TrueFilter.THE;
        return subFilters;
    }

    /**
     * @param lastOp if the last operator is ==, replace it with lastOp if the key is partially
     * specified
     */
    private static RowFilter combineRange(ColumnToArgFilter[] terms, int lastOp) {
        if (terms == null) {
            return null;
        }

        int len = 0;
        while (true) {
            if (terms[len] == null) {
                if (terms[len - 1].operator() == OP_EQ) {
                    terms[len - 1] = terms[len - 1].withOperator(lastOp);
                }
                break;
            } else if (++len >= terms.length) {
                break;
            }
        }

        return flatten(terms, 0, len);
    }

    @Override
    char opChar() {
        return '&';
    }

    @Override
    RowFilter newInstance(RowFilter[] subFilters, int off, int len) {
        return AndFilter.flatten(subFilters, off, len);
    }

    @Override
    RowFilter newFlippedInstance(RowFilter... subFilters) {
        return OrFilter.flatten(subFilters, 0, subFilters.length);
    }

    @Override
    RowFilter emptyInstance() {
        return TrueFilter.THE;
    }

    @Override
    RowFilter emptyFlippedInstance() {
        return FalseFilter.THE;
    }

    @Override
    int reduceOperator(ColumnFilter a, ColumnFilter b) {
        return a.reduceOperatorForAnd(b);
    }
}
