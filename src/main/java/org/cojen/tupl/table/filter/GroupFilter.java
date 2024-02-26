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

import java.math.BigInteger;

import java.util.Arrays;

import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.ToIntFunction;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public abstract sealed class GroupFilter extends RowFilter permits AndFilter, OrFilter {
    private static final long REDUCE_LIMIT, REDUCE_LIMIT_SQRT;
    private static final int SPLIT_LIMIT;

    static {
        // Limit the number of steps to perform when reducing via dnf/cnf. The reduce
        // operation time complexity is approximately O(n^2), so compare to sqrt.
        double limit = 10e6;
        String prop = System.getProperty("org.cojen.tupl.filter.ReduceLimit");
        if (prop != null) {
            try {
                limit = Double.parseDouble(prop);
            } catch (NumberFormatException e) {
            }
        }
        REDUCE_LIMIT = (long) limit;
        REDUCE_LIMIT_SQRT = (long) Math.sqrt(limit);
        SPLIT_LIMIT = (int) Math.min(1000L, REDUCE_LIMIT_SQRT);
    }

    final RowFilter[] mSubFilters;

    int mMatchHashCode;

    private MatchSet mMatchSet;

    static final int FLAG_DNF_SET = 1, FLAG_IS_DNF = 2, FLAG_CNF_SET = 4, FLAG_IS_CNF = 8;

    int mFlags;

    RowFilter mReduced, mReducedNoMerge, mDnf, mCnf;

    GroupFilter(int hash, RowFilter... subFilters) {
        super(hash);
        mSubFilters = subFilters;
    }

    @Override
    public final boolean equals(Object obj) {
        return obj == this || obj instanceof GroupFilter other
            && opChar() == other.opChar() && Arrays.equals(mSubFilters, other.mSubFilters);
    }

    @Override
    public final int compareTo(RowFilter filter) {
        if (filter instanceof GroupFilter other) {
            if (opChar() == other.opChar()) {
                return Arrays.compare(mSubFilters, other.mSubFilters);
            }
        }
        return super.compareTo(filter);
    }

    @Override
    public final RowFilter sort() {
        RowFilter[] subFilters = mSubFilters;
        if (subFilters.length == 0) {
            return this;
        }
        subFilters = subFilters.clone();
        for (int i=0; i<subFilters.length; i++) {
            subFilters[i] = subFilters[i].sort();
        }
        Arrays.sort(subFilters);
        return newInstance(subFilters);
    }

    @Override
    protected final RowFilter trySplit(Function<ColumnFilter, RowFilter> check) {
        RowFilter[] subFilters = mSubFilters;

        for (int i=0; i<subFilters.length; i++) {
            RowFilter sub = subFilters[i];
            RowFilter extracted = sub.trySplit(check);
            if (extracted == null) {
                return null;
            }
            if (extracted != sub) {
                if (subFilters == mSubFilters) {
                    subFilters = subFilters.clone();
                }
                subFilters[i] = extracted;
            }
        }

        return subFilters == mSubFilters ? this : newInstance(subFilters);
    }

    @Override
    public final RowFilter replaceArguments(IntUnaryOperator function) {
        return transform(sub -> sub.replaceArguments(function));
    }

    @Override
    public final RowFilter argumentAsNull(int argNum) {
        return transform(sub -> sub.argumentAsNull(argNum));
    }

    @Override
    public final RowFilter constantsToArguments(ToIntFunction<ColumnToConstantFilter> function) {
        return transform(sub -> sub.constantsToArguments(function));
    }

    private RowFilter transform(Function<RowFilter, RowFilter> transformer) {
        RowFilter[] subFilters = mSubFilters;

        for (int i=0; i<subFilters.length; i++) {
            RowFilter sub = subFilters[i];
            RowFilter modified = transformer.apply(sub);
            if (modified != sub) {
                if (subFilters == mSubFilters) {
                    subFilters = subFilters.clone();
                }
                subFilters[i] = modified;
            }
        }

        return subFilters == mSubFilters ? this : newInstance(subFilters);
    }

    @Override
    public void appendTo(StringBuilder b) {
        char opChar = opChar();
        for (int i=0; i<mSubFilters.length; i++) {
            if (i != 0) {
                b.append(' ').append(opChar).append(opChar).append(' ');
            }
            RowFilter sub = mSubFilters[i];
            if (sub instanceof GroupFilter) {
                b.append('(');
                sub.appendTo(b);
                b.append(')');
            } else {
                sub.appendTo(b);
            }
        }
    }

    @Override
    public final int numTerms() {
        int num = 0;
        for (RowFilter sub : mSubFilters) {
            num += sub.numTerms();
        }
        return num;
    }

    @Override
    protected final int maxArgument(int max) {
        for (RowFilter sub : mSubFilters) {
            max = sub.maxArgument(max);
        }
        return max;
    }

    public final RowFilter[] subFilters() {
        return mSubFilters;
    }

    public abstract char opChar();

    public final RowFilter newInstance(RowFilter... subFilters) {
        return newInstance(subFilters, 0, subFilters.length);
    }

    abstract RowFilter newInstance(RowFilter[] subFilters, int off, int len);

    /**
     * Returns a new instance with the operator flipped: "or" <==> "and"
     */
    abstract RowFilter newFlippedInstance(RowFilter... subFilters);

    abstract RowFilter emptyInstance();

    abstract RowFilter emptyFlippedInstance();

    abstract int reduceOperator(ColumnFilter a, ColumnFilter b);

    final RowFilter[] subNot() {
        RowFilter[] subFilters = mSubFilters.clone();
        for (int i=0; i<subFilters.length; i++) {
            subFilters[i] = subFilters[i].not();
        }
        return subFilters;
    }

    final MatchSet matchSet() {
        MatchSet set = mMatchSet;
        if (set == null) {
            mMatchSet = set = new MatchSet(this);
        }
        return set;
    }

    @Override
    public final int isSubMatch(RowFilter filter) {
        int result = isMatch(filter);
        if (result == 0) {
            MatchSet set = matchSet();
            result = set.hasMatch(filter);
            if (result == 0 && filter instanceof GroupFilter group) {
                for (RowFilter sub : group.mSubFilters) {
                    int subResult = set.hasMatch(sub);
                    if (subResult == 0) {
                        return 0;
                    }
                    if (result == 0) {
                        result = subResult;
                    } else if (result != subResult) {
                        return 0;
                    }
                }
            }
        }
        return result;
    }

    @Override
    public final int matchHashCode() {
        int hash = mMatchHashCode;
        if (hash == 0) {
            for (RowFilter sub : mSubFilters) {
                hash += sub.matchHashCode();
            }
            mMatchHashCode = hash;
        }
        return hash;
    }

    @Override
    public final RowFilter reduce() {
        if (mReduced == null) {
            mReduced = doReduce(0, true);
        }
        return mReduced;
    }

    /**
     * @param limit complexity limit
     * @param merge when true, also perform operator reduction
     */
    final RowFilter reduce(long limit, boolean merge) {
        if (merge) {
            if (mReduced == null) {
                mReduced = doReduce(limit, true);
            }
            return mReduced;
        } else {
            if (mReducedNoMerge == null) {
                mReducedNoMerge = doReduce(limit, false);
            }
            return mReducedNoMerge;
        }
    }

    private RowFilter doReduce(long limit, boolean merge) {
        /*
          Reduce the group filter by eliminating redundant sub-filters. The following
          transformations are applied:

          Absorption:
            A || (A && B) => A
            A && (A || B) => A

          Idempotence: (this is a special form of absorption when B is missing)
            A || A => A
            A && A => A

          Negative absorption:
            A || (!A && B) => A || B
            A && (!A || B) => A && B

          Complementation: (this is a special form of negative absorption when B is missing)
            A || !A => true
            A && !A => false

          Elimination:
            (A && B) || (A && !B) => A
            (A || B) && (A || !B) => A

          When merge is true, operator reduction is also applied, which can result in
          complementation, idempotence, or elimination. The elimination case occurs when two
          operators combine into a different one. A full reduction pass is required, for
          performing compound reduction.
         */

        RowFilter[] subFilters = mSubFilters;

        limit += (long) subFilters.length * subFilters.length;
        if (limit > REDUCE_LIMIT) {
            throw new ComplexFilterException(null, limit);
        }

        int numRemoved = 0;

        boolean repeat;
        do {
            // Is set to true when compound reduction is a possibility.
            repeat = false;

            loopi: for (int i=0; i<subFilters.length; i++) {
                RowFilter sub = subFilters[i];

                if (sub == null) {
                    continue loopi;
                }

                RowFilter subReduced = sub.reduce(limit, merge);
                if (sub != subReduced) {
                    if (subReduced == emptyFlippedInstance()) {
                        return subReduced;
                    }
                    sub = subReduced;
                    if (sub.getClass() != getClass()) {
                        if (subFilters == mSubFilters) {
                            subFilters = subFilters.clone();
                        }
                        subFilters[i] = sub;
                    } else {
                        // Must flatten the sub-filter in.
                        var subSubFilters = ((GroupFilter) sub).mSubFilters;
                        var newSubFilters = new RowFilter
                            [subFilters.length - 1 + subSubFilters.length];
                        System.arraycopy(subFilters, 0, newSubFilters, 0, i);
                        System.arraycopy(subSubFilters, 0, newSubFilters, i, subSubFilters.length);
                        System.arraycopy(subFilters, i + 1, newSubFilters,
                                         i + subSubFilters.length, subFilters.length - i - 1);
                        subFilters = newSubFilters;
                        repeat = true;
                        sub = subFilters[i];
                    }
                }

                loopj: for (int j=0; j<subFilters.length; j++) {
                    if (i == j) {
                        // Skip self matchings.
                        continue loopj;
                    }

                    RowFilter check = subFilters[j];
                    if (check == null) {
                        // Skip removed empty instances.
                        continue loopj;
                    }

                    int result = sub.isSubMatch(check);
                    if (result == 0) {
                        // Doesn't match, but try elimination.
                        if (sub.getClass() == check.getClass() && sub instanceof GroupFilter) {
                            var groupSub = (GroupFilter) sub;
                            RowFilter resultSub = groupSub.eliminate((GroupFilter) check);
                            if (resultSub != sub) {
                                // Elimination.
                                if (resultSub == emptyFlippedInstance()) {
                                    return resultSub;
                                }
                                repeat = true;
                                if (subFilters == mSubFilters) {
                                    subFilters = subFilters.clone();
                                }
                                subFilters[i] = resultSub;
                                continue loopj;
                            }
                        }

                        // Try operator reduction. Check is commutative, so only needs to be
                        // checked once (hence the j > i test).
                        if (merge && j > i
                            && sub.getClass() == check.getClass() && sub instanceof ColumnFilter)
                        {
                            var columnSub = (ColumnFilter) sub;
                            int op = reduceOperator(columnSub, ((ColumnFilter) check));
                            if (op != Integer.MIN_VALUE) {
                                if (op == Integer.MAX_VALUE) {
                                    // Complementation.
                                    return emptyFlippedInstance();
                                }
                                if (subFilters == mSubFilters) {
                                    subFilters = subFilters.clone();
                                }
                                // Elimination when negative, else idempotence.
                                if (op < 0) {
                                    repeat = true;
                                    op = ~op;
                                }
                                if (op != columnSub.operator()) {
                                    columnSub = columnSub.withOperator(op);
                                }
                                numRemoved++;
                                subFilters[i] = null;
                                subFilters[j] = columnSub;
                                continue loopi;
                            }
                        }

                        // Keep the sub-filter.
                        continue loopj;
                    }

                    if (result > 0) {
                        // Absorption or idempotence. Replace with null, which is treated as an
                        // empty instance. It will get removed later.
                        numRemoved++;
                        if (subFilters == mSubFilters) {
                            subFilters = subFilters.clone();
                        }
                        subFilters[i] = null;
                        continue loopi;
                    }

                    // Negative absorption. Remove one or more sub-filters from the sub-group.

                    if (!(sub instanceof GroupFilter groupSub)) {
                        // Complementation, actually.
                        return emptyFlippedInstance();
                    }

                    RowFilter[] subSubFilters = groupSub.mSubFilters;
                    RowFilter[] newSubSubFilters = new RowFilter[subSubFilters.length - 1];

                    newSub: {
                        int k = 0;
                        for (RowFilter subSub : subSubFilters) {
                            if (subSub.isMatch(check) >= 0) {
                                if (k >= newSubSubFilters.length) {
                                    break newSub;
                                }
                                newSubSubFilters[k++] = subSub;
                            }
                        }
                        sub = groupSub.newInstance(newSubSubFilters, 0, k);
                        if (subFilters == mSubFilters) {
                            subFilters = subFilters.clone();
                        }
                        subFilters[i] = sub;
                    }
                }
            }
        } while (repeat);

        if (numRemoved > 0) {
            int newLength = subFilters.length - numRemoved;

            if (newLength <= 1) {
                if (newLength == 0) {
                    return emptyInstance();
                }
                for (RowFilter sub : subFilters) {
                    if (sub != null) {
                        return sub;
                    }
                }
            }

            var newSubFilters = new RowFilter[newLength];
            for (int i=0, j=0; i<subFilters.length; i++) {
                RowFilter sub = subFilters[i];
                if (sub != null) {
                    newSubFilters[j++] = sub;
                }
            }

            subFilters = newSubFilters;
        } else if (subFilters == mSubFilters) {
            return this;
        }

        RowFilter reduced = newInstance(subFilters);

        if (reduced instanceof GroupFilter group) {
            group.mReduced = group;
        }

        return reduced;
    }

    @Override
    RowFilter expandOperators(boolean force) {
        if (!force) check: {
            // Check if has multiple levels.
            for (RowFilter sub : mSubFilters) {
                if (!(sub instanceof ColumnFilter)) {
                    break check;
                }
            }
            return this;
        }

        RowFilter[] subFilters = mSubFilters;

        for (int i=0; i<subFilters.length; i++) {
            RowFilter sub = subFilters[i];
            RowFilter expanded = sub.expandOperators(true);
            if (expanded != sub) {
                if (subFilters == mSubFilters) {
                    subFilters = subFilters.clone();
                }
                subFilters[i] = expanded;
            }
        }

        return subFilters == mSubFilters ? this : newInstance(subFilters);
    }

    @Override
    public final RowFilter dnf() {
        RowFilter dnf = mDnf;
        if (dnf == null) {
            dnf = dnf(0, true);

            // Try to reduce further by eliminating overlapping ranges.
            try {
                RowFilter expanded = dnf.expandOperators(false);
                if (expanded != dnf) {
                    RowFilter reduced = expanded.dnf(0, false).reduce();
                    if (!reduced.equals(dnf) && reduced.numTerms() <= dnf.numTerms()) {
                        if (!reduced.isDnf()) {
                            throw new AssertionError();
                        }
                        dnf = reduced;
                    }
                }
            } catch (ComplexFilterException e) {
            }

            mDnf = dnf;
        }

        return dnf;
    }

    @Override
    public final RowFilter dnf(long limit, boolean merge) {
        RowFilter filter = this;
        while (true) {
            filter = filter.reduce(limit, merge);
            if (filter.isDnf()) {
                return filter;
            }
            if (!(filter instanceof GroupFilter group)) {
                return filter.dnf(limit, merge);
            }
            filter = group.distribute(limit, merge, true);
        }
    }

    @Override
    public final RowFilter cnf() {
        RowFilter cnf = mCnf;
        if (cnf == null) {
            cnf = cnf(0, true);

            // Try to reduce further by eliminating overlapping ranges.
            try {
                RowFilter expanded = cnf.expandOperators(false);
                if (expanded != cnf) {
                    RowFilter reduced = expanded.cnf(0, false).reduce();
                    if (!reduced.equals(cnf) && reduced.numTerms() <= cnf.numTerms()) {
                        if (!reduced.isCnf()) {
                            throw new AssertionError();
                        }
                        cnf = reduced;
                    }
                }
            } catch (ComplexFilterException e) {
            }

            mCnf = cnf;
        }

        return cnf;
    }

    @Override
    public final RowFilter cnf(long limit, boolean merge) {
        RowFilter filter = this;
        while (true) {
            filter = filter.reduce(limit, merge);
            if (filter.isCnf()) {
                return filter;
            }
            if (!(filter instanceof GroupFilter group)) {
                return filter.cnf(limit, merge);
            }
            filter = group.distribute(limit, merge, false);
        }
    }

    /**
     * Applies the distributive law to this filter such it becomes flattened into disjunctive
     * or conjunctive normal form.
     *
     * @param limit complexity limit
     * @param merge when true, also perform operator reduction
     * @param dnf true if called by dnf method, false if called by cnf method
     */
    private RowFilter distribute(long limit, boolean merge, boolean dnf) {
        RowFilter[] subFilters = mSubFilters;

        long count = 1;
        for (RowFilter sub : subFilters) {
            if (sub instanceof GroupFilter group) {
                count *= group.mSubFilters.length;
                if (count > SPLIT_LIMIT && subFilters.length > 2) {
                    RowFilter filter = trySplitDistribute(limit, merge, dnf);
                    if (filter != null) {
                        return filter;
                    }
                }
                if (count > REDUCE_LIMIT_SQRT) {
                    throw complex();
                }
            }
        }

        var newSubFilters = new RowFilter[(int) count];

        for (int i=0; i<newSubFilters.length; i++) {
            var newGroupSubFilters = new RowFilter[subFilters.length];

            for (int select=i, j=0; j<newGroupSubFilters.length; j++) {
                RowFilter sub = subFilters[j];
                if (sub instanceof GroupFilter group) {
                    int num = group.mSubFilters.length;
                    int subSelect = select / num;
                    newGroupSubFilters[j] = group.mSubFilters[select - subSelect * num];
                    select = subSelect;
                } else {
                    newGroupSubFilters[j] = sub;
                }
            }

            var newSub = newInstance(newGroupSubFilters).reduce(limit, merge);

            newSubFilters[i] = newSub;
        }

        return newFlippedInstance(newSubFilters);
    }

    /**
     * @return null if no change
     */
    private RowFilter trySplitDistribute(long limit, boolean merge, boolean dnf) {
        int mid = mSubFilters.length >> 1;
        RowFilter filter = splitDistribute(limit, merge, dnf, mid);
        if (!equals(filter)) {
            return filter;
        }
        for (int pos = 1; pos < mSubFilters.length; pos++) {
            if (pos != mid) {
                filter = splitDistribute(limit, merge, dnf, pos);
                if (!equals(filter)) {
                    return filter;
                }
            }
        }
        return null;
    }

    private RowFilter splitDistribute(long limit, boolean merge, boolean dnf, int pos) {
        RowFilter left = newInstance(mSubFilters, 0, pos);
        RowFilter right = newInstance(mSubFilters, pos, mSubFilters.length - pos);
        if (dnf) {
            left = left.dnf(limit, merge);
            right = right.dnf(limit, merge);
        } else {
            left = left.cnf(limit, merge);
            right = right.cnf(limit, merge);
        }
        return newInstance(left, right);
    }

    private ComplexFilterException complex() {
        var numTerms = BigInteger.ONE;
        for (RowFilter sub : mSubFilters) {
            if (sub instanceof GroupFilter group) {
                numTerms = numTerms.multiply(BigInteger.valueOf(group.mSubFilters.length));
            }
        }
        return new ComplexFilterException(numTerms, 0);
    }

    /**
     * @return this if nothing was eliminated
     */
    private RowFilter eliminate(GroupFilter other) {
        RowFilter[] newSubFilters = null;
        int newSubFiltersSize = 0;

        for (int i=0; i<mSubFilters.length; i++) {
            RowFilter sub = mSubFilters[i];
            if (other.isSubMatch(sub) < 0 && matchSet().equalMatches(other.matchSet(), sub) != 0) {
                // Eliminate the sub-filter.
                if (newSubFilters == null) {
                    newSubFilters = new RowFilter[mSubFilters.length];
                    System.arraycopy(mSubFilters, 0, newSubFilters, 0, i);
                    newSubFiltersSize = i;
                }
            } else {
                // Keep the sub-filter.
                if (newSubFilters != null) {
                    newSubFilters[newSubFiltersSize++] = sub;
                }
            }
        }

        return newSubFilters == null ? this : newInstance(newSubFilters, 0, newSubFiltersSize);
    }
}
