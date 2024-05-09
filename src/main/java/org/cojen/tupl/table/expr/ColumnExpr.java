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

import java.util.function.Consumer;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.RowMethodsMaker;

/**
 * Defines an expression which reads a named column from a row.
 *
 * @author Brian S. O'Neill
 */
public final class ColumnExpr extends Expr implements Named {
    public static ColumnExpr make(int startPos, int endPos, TupleType rowType, Column column) {
        return new ColumnExpr(startPos, endPos, rowType, column);
    }

    private final Column mColumn;
    private final Sub mLastSub;

    private ColumnExpr(int startPos, int endPos, TupleType rowType, Column column) {
        super(startPos, endPos);
        mColumn = column;

        boolean nullable = false;
        Sub sub = null;

        for (String subName : mColumn.subNames()) {
            Column subColumn = rowType.columnFor(subName);
            Type subType = subColumn.type();
            if (subType.isNullable()) {
                nullable = true;
            }
            if (subType instanceof TupleType tt) {
                rowType = tt;
            }
            sub = new Sub(sub, subColumn.name(), nullable);
        }

        mLastSub = sub;
    }

    @Override
    public String name() {
        return mColumn.name();
    }

    @Override
    public Type type() {
        return mColumn.type();
    }

    @Override
    public Expr asType(Type type) {
        return ConversionExpr.make(startPos(), endPos(), this, type);
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
    public ColumnExpr sourceColumn() {
        return this;
    }

    @Override
    public void gatherEvalColumns(Consumer<Column> c) {
        c.accept(mColumn);
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
     * Returns a column with a fully qualified name.
     */
    public Column column() {
        return mColumn;
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            mColumn.encodeKey(enc);
        }
    }

    @Override
    public int hashCode() {
        return mColumn.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj instanceof ColumnExpr ce && mColumn.equals(ce.mColumn);
    }

    @Override
    public String toString() {
        return RowMethodsMaker.unescape(mColumn.name());
    }

    private static final class Sub {
        final Sub mParent;
        final String mName;
        final boolean mNullable;
        Sub mNext;

        Sub(Sub parent, String name, boolean nullable) {
            mParent = parent;
            mName = name;
            mNullable = nullable;
            if (parent != null) {
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
    }
}
