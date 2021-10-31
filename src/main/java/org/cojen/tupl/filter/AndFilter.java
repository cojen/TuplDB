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

import java.math.BigDecimal;

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
            if (sub instanceof AndFilter af) {
                count += af.mSubFilters.length;
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
            if (sub instanceof AndFilter af) {
                for (RowFilter andSubFilter : af.mSubFilters) {
                    newSubFilters[j++] = andSubFilter;
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
        if (filter instanceof AndFilter af) {
            return matchSet().equalMatches(af.matchSet());
        }
        if (filter instanceof OrFilter of) {
            return matchSet().inverseMatches(of.matchSet());
        }
        return 0; // doesn't match
    }

    @Override
    public OrFilter not() {
        return new OrFilter(subNot());
    }

    @Override
    public RowFilter[] rangeExtract(ColumnInfo... keyColumns) {
        if (!mReduced) {
            return reduce().rangeExtract(keyColumns);
        }

        RowFilter[] subFilters = mSubFilters;

        final var lowTerms = new ColumnToArgFilter[keyColumns.length];
        final var highTerms = new ColumnToArgFilter[keyColumns.length];

        boolean fuzzy = false;

        keys: for (int k = 0; k < keyColumns.length; k++) {
            String keyName = keyColumns[k].name;

            for (int match = 0; match <= 1; match++) {
                for (int s = 0; s < subFilters.length; s++) {
                    RowFilter sub = subFilters[s];

                    if (!(sub instanceof ColumnToArgFilter term)) {
                        continue;
                    }

                    if (!keyName.equals(term.mColumn.name)) {
                        continue;
                    }

                    int op = term.mOperator;

                    if (match == 0) {
                        if (op == OP_EQ) {
                            lowTerms[k] = term;
                            highTerms[k] = term;
                            if (term.mColumn.type != BigDecimal.class) {
                                subFilters = removeSub(subFilters, s);
                                continue keys;
                            }
                            // BigDecimal matches on a range of values, so need to double-check
                            // against the original term. Cannot continue after a range match.
                            fuzzy = true;
                        }
                    } else if (!term.mColumn.isDescending()) {
                        if (op == OP_GT || op == OP_GE) {
                            if (lowTerms[k] == null) {
                                lowTerms[k] = term;
                                subFilters = removeSub(subFilters, s);
                            }
                        } else if (op == OP_LT || op == OP_LE) {
                            if (highTerms[k] == null) {
                                highTerms[k] = term;
                                subFilters = removeSub(subFilters, s);
                            }
                        }
                    } else {
                        if (op == OP_GT || op == OP_GE) {
                            if (highTerms[k] == null) {
                                highTerms[k] = ColumnToArgFilter.descending(term);
                                subFilters = removeSub(subFilters, s);
                            }
                        } else if (op == OP_LT || op == OP_LE) {
                            if (lowTerms[k] == null) {
                                lowTerms[k] = ColumnToArgFilter.descending(term);
                                subFilters = removeSub(subFilters, s);
                            }
                        }
                    }
                }
            }

            // Is either a range match against the key column or none, so can't continue.
            break keys;
        }

        RowFilter lowRange = combineRange(lowTerms, fuzzy, OP_GE);
        RowFilter highRange = combineRange(highTerms, fuzzy, OP_LE);

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
     * @param lastOp if !fuzzy and the last operator is ==, replace it with lastOp
     */
    private static RowFilter combineRange(ColumnToArgFilter[] terms, boolean fuzzy, int lastOp) {
        int len;
        for (len = 0; len < terms.length && terms[len] != null; len++);

        if (len == 0) {
            return null;
        }

        if (!fuzzy && terms[len - 1].operator() == OP_EQ) {
            terms[len - 1] = terms[len - 1].withOperator(lastOp);
        }

        return flatten(terms, 0, len);
    }

    @Override
    final char opChar() {
        return '&';
    }

    @Override
    final RowFilter newInstance(RowFilter[] subFilters, int off, int len) {
        return AndFilter.flatten(subFilters, off, len);
    }

    @Override
    final RowFilter newFlippedInstance(RowFilter... subFilters) {
        return OrFilter.flatten(subFilters, 0, subFilters.length);
    }

    @Override
    final RowFilter emptyInstance() {
        return TrueFilter.THE;
    }

    @Override
    final RowFilter emptyFlippedInstance() {
        return FalseFilter.THE;
    }

    @Override
    final int reduceOperator(ColumnFilter a, ColumnFilter b) {
        return a.reduceOperatorForAnd(b);
    }
}