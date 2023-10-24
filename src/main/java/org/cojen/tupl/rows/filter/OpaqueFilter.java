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

package org.cojen.tupl.rows.filter;

import java.util.Objects;

import java.util.function.Function;
import java.util.function.IntUnaryOperator;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;

/**
 * Defines a filter term which cannot be deeply analyzed.
 *
 * @author Brian S. O'Neill
 */
public final class OpaqueFilter extends TermFilter {
    private final boolean mNot;
    private final Object mAttachment;

    public OpaqueFilter(boolean not, Object attachment) {
        // Note that the hashCode ignores the "not" state. If it was incorporated, then the
        // matchHashCode method would need to exclude it.
        this(Objects.hashCode(attachment), not, attachment);
    }

    private OpaqueFilter(int hash, boolean not, Object attachment) {
        super(hash);
        mNot = not;
        mAttachment = attachment;
    }

    public boolean isNot() {
        return mNot;
    }

    public Object attachment() {
        return mAttachment;
    }

    @Override
    public void accept(Visitor visitor) {
        visitor.visit(this);
    }

    @Override
    protected int maxArgument(int max) {
        return 0;
    }

    @Override
    RowFilter expandOperators(boolean force) {
        return this;
    }

    @Override
    public int isMatch(RowFilter filter) {
        if (filter == this) {
            return 1; // equal
        }
        if (filter instanceof OpaqueFilter other) {
            if (Objects.equals(mAttachment, other.mAttachment)) {
                return mNot == other.mNot ? 1 : -1;
            }
        }
        return 0; // doesn't match
    }

    @Override
    public int matchHashCode() {
        return hashCode();
    }

    @Override
    public RowFilter not() {
        return new OpaqueFilter(hashCode(), !mNot, this);
    }

    @Override
    public RowFilter replaceArguments(IntUnaryOperator function) {
        return this;
    }

    @Override
    public RowFilter argumentAsNull(int argNum) {
        return this;
    }

    @Override
    public RowFilter constantsToArguments(ToIntFunction<ColumnToConstantFilter> function) {
        return this;
    }

    @Override
    public RowFilter retain(Predicate<String> pred, boolean strict, RowFilter undecided) {
        return undecided;
    }

    @Override
    protected RowFilter trySplit(Function<ColumnFilter, RowFilter> check) {
        return null;
    }

    @Override
    public boolean uniqueColumn(String columnName) {
        return false;
    }

    @Override
    public final boolean equals(Object obj) {
        return obj == this || obj instanceof OpaqueFilter other
            && Objects.equals(mAttachment, other.mAttachment);
    }

    @Override
    public void appendTo(StringBuilder b) {
        if (mNot) {
            b.append('!');
        }
        b.append('[').append(mAttachment).append(']');
    }
}
