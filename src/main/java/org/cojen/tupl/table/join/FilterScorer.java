/*
 *  Copyright (C) 2023 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.table.join;

import java.util.Set;

import org.cojen.tupl.table.filter.AndFilter;
import org.cojen.tupl.table.filter.ColumnFilter;
import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.OrFilter;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.Visitor;

import static org.cojen.tupl.table.filter.ColumnFilter.*;

/**
 * Estimates a filter's reduction power. Instances aren't thread safe.
 *
 * @author Brian S O'Neill
 * @see JoinSpec
 */
class FilterScorer implements Visitor {
    FilterScorer() {
    }

    // The fields of this class are only set temporarily, as needed by the visitor methods.

    /*
      Filter reduction score. Higher is better. The score is broken down into three regions:

      Bits 63..42: strong matches (max 4 million)
      Bits 41..21: medium matches (max 2 million)
      Bits 20.. 0: weak matches (max 2 million)

      Strong and medium matches reduce well and can be utilized by indexes. Weak matches
      are unlikely to be utilized by indexes.
    */
    private long mScore;

    private Set<String> mAvailable;

    public final long calculate(RowFilter filter, Set<String> available) {
        mScore = 0;
        mAvailable = available;
        filter.accept(this);
        mAvailable = null;
        return mScore;
    }

    /**
     * @return -1 if this score1 is worse than score2, 0 if same, 1 if this score1 is better
     * than score2
     */
    public static int compare(long score1, long score2) {
        return Long.compareUnsigned(score1, score2);
    }

    @Override
    public final void visit(OrFilter filter) {
        long score = ~0L; // max unsigned long
        for (RowFilter sub : filter.subFilters()) {
            sub.accept(this);
            long newScore = mScore;
            if (Long.compareUnsigned(newScore, score) < 0) {
                score = newScore; // select the worst score
            }
        }
        mScore = score;
    }

    @Override
    public final void visit(AndFilter filter) {
        long score = 0;
        for (RowFilter sub : filter.subFilters()) {
            sub.accept(this);
            long sum = score + mScore; // add up the scores
            if (Long.compareUnsigned(sum, score) < 0) {
                score = ~0L; // clamp if overflowed (unlikely)
                break;
            }
            score = sum;
        }
        mScore = score;
    }

    @Override
    public final void visit(ColumnToArgFilter filter) {
        if (mAvailable.contains(filter.column().name)) {
            score(filter);
        }
    }

    @Override
    public final void visit(ColumnToColumnFilter filter) {
        if (mAvailable.contains(filter.column().name)
            || mAvailable.contains(filter.otherColumn().name))
        {
            score(filter);
        }
    }

    private void score(ColumnFilter filter) {
        mScore = switch (filter.operator()) {
            case OP_EQ -> 1L << 42; // strong
            case OP_GE, OP_LT, OP_LE, OP_GT -> 1L << 21; // medium
            default -> 1L; // weak
        };
    }
}
