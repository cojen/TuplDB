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

package org.cojen.tupl.model;

import java.util.Objects;
import java.util.Set;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Defines a node which accesses a named column found in a RelationNode.
 *
 * @author Brian S. O'Neill
 * @see RelationNode#findColumn
 */
public final class ColumnNode extends Node {
    /**
     * @param name qualified or unqualified name which was requested
     * @param column must refer to a column in a RelationNode; name is a fully qualified field
     */
    public static ColumnNode make(RelationNode from, String name, Column column) {
        return new ColumnNode(from, name, column);
    }

    private final RelationNode mFrom;
    private final String mName;
    private final Column mColumn;
    private final Sub mLastSub;

    private ColumnNode(RelationNode from, String name, Column column) {
        mFrom = from;
        mName = name;
        mColumn = column;

        TupleType tt = from.type().tupleType();
        boolean nullable = false;
        Sub sub = null;

        for (String subName : mColumn.subNames()) {
            Column subColumn = tt.fieldColumn(subName);
            if (subColumn != null) {
                Type subType = subColumn.type();
                if (subType.isNullable()) {
                    nullable = true;
                }
                if (subType instanceof TupleType ctt) {
                    tt = ctt;
                }
            }
            sub = new Sub(sub, subName, nullable);
        }

        mLastSub = sub;
    }

    @Override
    public Type type() {
        return mColumn.type();
    }

    @Override
    public Node asType(Type type) {
        if (type == BasicType.BOOLEAN) {
            Type thisType = type();
            if (thisType == BasicType.BOOLEAN) {
                return this;
            }
            throw new IllegalStateException("Cannot convert " + thisType + " to " + type);
        } else {
            return ConversionNode.make(this, type);
        }
    }

    public RelationNode from() {
        return mFrom;
    }

    /**
     * Returns the qualified or unqualified name which was requested.
     */
    @Override
    public String name() {
        return mName;
    }

    @Override
    public ColumnNode withName(String name) {
        return name.equals(mName) ? this : new ColumnNode(mFrom, name, mColumn);
    }

    @Override
    public int maxArgument() {
        return 0;
    }

    @Override
    public boolean isPureFunction() {
        return true;
    }

    @Override
    public boolean isNullable() {
        return mLastSub.mNullable;
    }

    @Override
    public void evalColumns(Set<String> columns) {
        columns.add(mColumn.name());
    }

    @Override
    public Variable makeEval(EvalContext context) {
        return mLastSub.makeEval(context);
    }

    @Override
    public boolean canThrowRuntimeException() {
        return false;
    }

    /**
     * Returns a RelationNode column with a fully qualified field name.
     */
    public Column column() {
        return mColumn;
    }

    @Override
    public int hashCode() {
        int hash = mFrom.hashCode();
        hash = hash * 31 + mColumn.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ColumnNode cn && mFrom.equals(cn.mFrom) && mColumn.equals(cn.mColumn);
    }

    private static final class Sub {
        final Sub mParent;
        final String mName;
        final boolean mNullable;
        final int mHashCode;
        Sub mNext;

        Sub(Sub parent, String name, boolean nullable) {
            mParent = parent;
            mName = name;
            mNullable = nullable;
            if (parent == null) {
                mHashCode = name.hashCode() * 31;
            } else {
                mHashCode = parent.hashCode() * 31 + name.hashCode();
                parent.mNext = this;
            }
        }

        Variable makeEval(EvalContext context) {
            var resultRef = context.refFor(this);
            var result = resultRef.get();
            if (result != null) {
                return result;
            }

            final var baseVar = mParent == null ? context.rowVar : mParent.makeEval(context);

            if (!mNullable) {
                return resultRef.set(baseVar.invoke(mName));
            }

            if (mParent == null || !mParent.mNullable) {
                resultRef.set(baseVar.invoke(mName));
            } else {
                // Rather than checking if the parent was null or not each time, assign a
                // not-null or null result within the parent code blocks. See below.
                var labels = (Label[]) context.refFor(mParent).attachment;
                labels[0].insert(() -> resultRef.set(baseVar.invoke(mName).box()));
                labels[1].insert(() -> resultRef.get().set(null));
            }

            if (mNext != null) {
                // Define notNull/isNull labels for the next sub column to insert code into.
                MethodMaker mm = context.methodMaker();
                Label notNull = mm.label();
                resultRef.get().ifNe(null, notNull);
                Label isNull = mm.label().here();
                Label cont = mm.label().goto_();
                notNull.here();
                cont.here();
                resultRef.attachment = new Label[] {notNull, isNull};
            }

            return resultRef.get();
        }

        @Override
        public int hashCode() {
            return mHashCode;
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Sub sub
                && Objects.equals(mParent, sub.mParent)
                && mName.equals(sub.mName)
                && mNullable == sub.mNullable;
        }
    }
}
