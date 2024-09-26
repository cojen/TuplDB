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

import java.math.BigDecimal;

import java.util.Arrays;

import java.util.function.Function;
import java.util.function.Predicate;

import org.cojen.tupl.table.ColumnInfo;

import static org.cojen.tupl.table.filter.ColumnFilter.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public sealed class AndFilter extends GroupFilter permits TrueFilter {
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
            } else if (sub instanceof FalseFilter) {
                return FalseFilter.THE;
            } else {
                count++;
            }
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
    public final void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    public final void appendTo(StringBuilder b) {
        if (mSubFilters.length == 0) {
            b.append("true");
        } else {
            super.appendTo(b);
        }
    }

    @Override
    public final boolean isDnf() {
        if ((mFlags & FLAG_DNF_SET) != 0) {
            return (mFlags & FLAG_IS_DNF) != 0;
        }
        for (RowFilter sub : mSubFilters) {
            if (sub instanceof OrFilter || !sub.isDnf()) {
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
            if (!sub.isCnf()) {
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
    public final RowFilter retain(Predicate<String> pred, boolean strict, RowFilter undecided) {
        RowFilter[] subFilters = mSubFilters;
        if (subFilters.length == 0) {
            return this;
        }

        subFilters = mSubFilters.clone();

        int len = 0;
        for (int i=0; i<subFilters.length; i++) {
            RowFilter sub = subFilters[i].retain(pred, strict, undecided);
            if (sub == FalseFilter.THE) {
                return sub;
            }
            if (sub != TrueFilter.THE) {
                subFilters[len++] = sub;
            }
        }

        return newInstance(subFilters, 0, len);
    }

    @Override
    public final void split(Function<ColumnFilter, RowFilter> check, RowFilter[] split) {
        split[0] = TrueFilter.THE;
        split[1] = TrueFilter.THE;
        splitCombine(check, split);
    }

    @Override
    protected final void splitCombine(Function<ColumnFilter, RowFilter> check, RowFilter[] split) {
        for (RowFilter sub : mSubFilters) {
            sub.splitCombine(check, split);
        }
    }

    @Override
    public final RowFilter[] rangeExtract(ColumnInfo... keyColumns) {
        if (mReduced == null) {
            return reduce().rangeExtract(keyColumns);
        }

        RowFilter[] subFilters = mSubFilters;

        final var lowTerms = new ColumnToArgFilter[keyColumns.length];
        final var highTerms = new ColumnToArgFilter[keyColumns.length];

        boolean fuzzy = false;

        keys: for (int k = 0; k < keyColumns.length; k++) {
            ColumnInfo keyColumn = keyColumns[k];
            String keyName = keyColumn.name;

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
                    } else if (!keyColumn.isDescending()) {
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

        return new RowFilter[] {lowRange, highRange, remaining, null};
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
    public final boolean matchesOne(RowFilter high, ColumnInfo... keyColumns) {
        if (!(high instanceof AndFilter highCol) ||
            mSubFilters.length != keyColumns.length ||
            mSubFilters.length != highCol.mSubFilters.length)
        {
            return false;
        }

        for (int i=0; i<mSubFilters.length; i++) {
            RowFilter lowSub = mSubFilters[i];
            if (!(lowSub instanceof ColumnToArgFilter lowSubCol)) {
                return false;
            }

            RowFilter highSub = highCol.mSubFilters[i];
            if (!(highSub instanceof ColumnToArgFilter highSubCol)) {
                return false;
            }

            if (lowSubCol.mArgNum != highSubCol.mArgNum ||
                !lowSubCol.mColumn.name.equals(highSubCol.mColumn.name))
            {
                return false;
            }

            int lowOp = OP_EQ, highOp = OP_EQ;
            if (i == mSubFilters.length - 1) {
                lowOp = OP_GE; highOp = OP_LE;
            }

            if (lowSubCol.mOperator != lowOp || highSubCol.mOperator != highOp) {
                return false;
            }
        }

        return true;
    }

    @Override
    public final boolean uniqueColumn(String columnName) {
        for (RowFilter sub : mSubFilters) {
            if (sub.uniqueColumn(columnName)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public final char opChar() {
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
