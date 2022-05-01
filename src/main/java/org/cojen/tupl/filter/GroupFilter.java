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

import java.math.BigInteger;

import java.util.Arrays;
import java.util.Map;

import org.cojen.tupl.rows.ColumnInfo;


/**
 * 
 *
 * @author Brian S O'Neill
 */
public abstract class GroupFilter extends RowFilter {
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

    static final int FLAG_REDUCED = 1,
        FLAG_DNF_SET = 2, FLAG_IS_DNF = 4, FLAG_CNF_SET = 8, FLAG_IS_CNF = 16;

    int mFlags;

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
    public int compareTo(RowFilter filter) {
        if (filter instanceof GroupFilter other) {
            if (opChar() == other.opChar()) {
                return Arrays.compare(mSubFilters, other.mSubFilters);
            }
        }
        return super.compareTo(filter);
    }

    @Override
    public RowFilter sort() {
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
    public RowFilter prioritize(Map<String, ColumnInfo> columns) {
        RowFilter[] subFilters = mSubFilters;
        if (subFilters.length == 0) {
            return this;
        }

        subFilters = subFilters.clone();
        for (int i=0; i<subFilters.length; i++) {
            subFilters[i] = subFilters[i].prioritize(columns);
        }

        Arrays.sort(subFilters,
                    (a, b) -> Double.compare(matchStrength(b, columns), matchStrength(a, columns)));

        return newInstance(subFilters);
    }

    @Override
    public boolean isSufficient(Map<String, ColumnInfo> columns) {
        for (RowFilter sub : mSubFilters) {
            if (!sub.isSufficient(columns)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Assumes that the given filter has been prioritized already.
     *
     * @return [0..1]
     */
    private static double matchStrength(RowFilter filter, Map<String, ColumnInfo> columns) {
        if (filter instanceof ColumnToArgFilter cf) {
            return columns.containsKey(cf.column().name) ? 1 : 0;
        }

        if (filter instanceof GroupFilter gf) {
            double sum = 0;
            for (RowFilter sub : gf.mSubFilters) {
                double strength = matchStrength(sub, columns);
                if (strength == 0) {
                    // By assuming that filter is already prioritized, can stop early.
                    break;
                }
                sum += strength;
            }
            return sum / gf.mSubFilters.length;
        }

        if (filter instanceof ColumnToColumnFilter cf) {
            double strength = columns.containsKey(cf.column().name) ? 0.5 : 0;
            if (columns.containsKey(cf.otherColumn().name)) {
                strength += 0.5;
            }
            return strength;
        }

        return 0;
    }

    @Override
    void appendTo(StringBuilder b) {
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
    public int numTerms() {
        int num = 0;
        for (RowFilter sub : mSubFilters) {
            num += sub.numTerms();
        }
        return num;
    }

    public final RowFilter[] subFilters() {
        return mSubFilters;
    }

    public abstract char opChar();

    RowFilter newInstance(RowFilter... subFilters) {
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

    // Used to track cumulative work performed by the reduce method. When exceeded, a
    // ComplexFilterException is thrown.
    static final ThreadLocal<long[]> cReduceLimit = new ThreadLocal<>();

    @Override
    public final RowFilter reduce() {
        if ((mFlags & FLAG_REDUCED) != 0) {
            return this;
        }

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

          Operator reduction is also applied, which can result in complementation, idempotence,
          or elimination. The elimination case occurs when two operators combine into a
          different one. A full reduction pass is required, for performing compound reduction.

         */

        RowFilter[] subFilters = mSubFilters;

        if (subFilters.length > REDUCE_LIMIT_SQRT) {
            // Too complex, might take forever to compute the result.
            return this;
        }

        long[] limitRef = cReduceLimit.get();
        if (limitRef != null) {
            long limit = limitRef[0];
            limit += (long) subFilters.length * subFilters.length;
            if (limit > REDUCE_LIMIT) {
                throw new ComplexFilterException(null, limit);
            }
            limitRef[0] = limit;
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

                RowFilter subReduced = sub.reduce();
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

                        // Try operator reduction.
                        if (j > i // check is commutative, so only needs to be checked once 
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
            mFlags |= FLAG_REDUCED;
            return this;
        }

        RowFilter reduced = newInstance(subFilters);

        if (reduced instanceof GroupFilter group) {
            group.mFlags |= FLAG_REDUCED;
        }

        return reduced;
    }

    @Override
    public final RowFilter dnf() {
        RowFilter filter = this;
        while (true) {
            filter = filter.reduce();
            if (filter.isDnf()) {
                return filter;
            }
            if (!(filter instanceof GroupFilter group)) {
                return filter.dnf();
            }
            filter = group.distribute(true);
        }
    }

    @Override
    public final RowFilter cnf() {
        RowFilter filter = this;
        while (true) {
            filter = filter.reduce();
            if (filter.isCnf()) {
                return filter;
            }
            if (!(filter instanceof GroupFilter group)) {
                return filter.cnf();
            }
            filter = group.distribute(false);
        }
    }

    /**
     * Applies the distributive law to this filter such it becomes flattened into disjunctive
     * or conjunctive normal form.
     *
     * @param dnf true if called by dnf method, false if called by cnf method
     */
    private RowFilter distribute(boolean dnf) {
        RowFilter[] subFilters = mSubFilters;

        long count = 1;
        for (RowFilter sub : subFilters) {
            if (sub instanceof GroupFilter group) {
                count *= group.mSubFilters.length;
                if (count > SPLIT_LIMIT && subFilters.length > 2) {
                    RowFilter filter = trySplitDistribute(dnf);
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

            newSubFilters[i] = newInstance(newGroupSubFilters).reduce();
        }

        return newFlippedInstance(newSubFilters);
    }

    /**
     * @return null if no change
     */
    private RowFilter trySplitDistribute(boolean dnf) {
        int mid = mSubFilters.length >> 1;
        RowFilter filter = splitDistribute(dnf, mid);
        if (!equals(filter)) {
            return filter;
        }
        for (int pos = 1; pos < mSubFilters.length; pos++) {
            if (pos != mid) {
                filter = splitDistribute(dnf, pos);
                if (!equals(filter)) {
                    return filter;
                }
            }
        }
        return null;
    }

    private RowFilter splitDistribute(boolean dnf, int pos) {
        RowFilter left = newInstance(mSubFilters, 0, pos);
        RowFilter right = newInstance(mSubFilters, pos, mSubFilters.length - pos);
        if (dnf) {
            left = left.dnf();
            right = right.dnf();
        } else {
            left = left.cnf();
            right = right.cnf();
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
