/*
 *  Copyright (C) 2024 Cojen.org
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

package org.cojen.tupl.table.expr;

import java.util.Objects;

import org.cojen.tupl.util.Canonicalizer;

/**
 * Defines a generic value range.
 *
 * @author Brian S. O'Neill
 * @see RangeExpr
 */
public final class Range {
    private static final Canonicalizer cCanonicalizer = new Canonicalizer();

    /**
     * Make an new or shared range instance.
     *
     * @param start inclusive start boundary; can be null for open range
     * @param end inclusive end boundary; can be null for open range
     */
    public static Range make(Object start, Object end) {
        return cCanonicalizer.apply(new Range(start, end));
    }

    private final Object mStart, mEnd;

    private Range(Object start, Object end) {
        mStart = start;
        mEnd = end;
    }

    @Override
    public int hashCode() {
        int hash = 839613349;
        if (mStart != null) {
            hash += mStart.hashCode();
        }
        if (mEnd != null) {
            hash = hash * 31 + mEnd.hashCode();
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this ||
            obj instanceof Range range
            && Objects.deepEquals(mStart, range.mStart) && Objects.deepEquals(mEnd, range.mEnd);
    }

    @Override
    public String toString() {
        if (mStart == null) {
            return mEnd == null ? ".." : (".." + mEnd);
        }
        return mEnd == null ? (mStart + "..") : (mStart + ".." + mEnd);
    }
}
