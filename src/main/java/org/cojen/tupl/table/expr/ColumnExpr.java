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

import java.util.Map;
import java.util.Objects;

import java.util.function.Consumer;

import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.RowInfo;

import static org.cojen.tupl.table.RowMethodsMaker.escape;
import static org.cojen.tupl.table.RowMethodsMaker.unescape;

/**
 * Defines an expression which reads a named column from a row.
 *
 * @author Brian S. O'Neill
 */
public abstract sealed class ColumnExpr extends Expr implements Named {
    /**
     * @param column can be null for wildcard
     */
    public static ColumnExpr make(int startPos, int endPos, TupleType rowType, Column column) {
        return new Base(startPos, endPos, rowType, column);
    }

    /**
     * @param column can be null for wildcard
     */
    public static ColumnExpr make(int startPos, int endPos, ColumnExpr parent, Column column) {
        return new Sub(startPos, endPos, parent, column);
    }

    protected final Column mColumn;

    protected ColumnExpr(int startPos, int endPos, Column column) {
        super(startPos, endPos);
        mColumn = column;
    }

    @Override
    public final Type type() {
        return mColumn == null ? AnyType.THE : mColumn.type();
    }

    @Override
    public final Expr asType(Type type) {
        return ConversionExpr.make(startPos(), endPos(), this, type);
    }

    @Override
    public final int maxArgument() {
        return 0;
    }

    @Override
    public final boolean isPureFunction() {
        return true;
    }

    @Override
    public final ColumnExpr sourceColumn() {
        return this;
    }

    @Override
    public final boolean canThrowRuntimeException() {
        return false;
    }

    public final boolean isWildcard() {
        return mColumn == null;
    }

    /**
     * Puts columns into the given map, keyed by full path. Method does nothing if this
     * expression isn't a wildcard.
     */
    public abstract void expandWildcard(Map<String, ColumnExpr> wildcards);

    protected abstract ColumnExpr parent();

    public abstract boolean isPath();

    /**
     * Returns true if any column along the path is hidden.
     */
    public abstract boolean isHidden();

    /**
     * Returns the first column along the path, which is null if this is a wildcard.
     */
    public abstract Column firstColumn();

    /**
     * Tries to find a column in the given RowInfo which matches the path of this column. If
     * found, the returned column has a fully qualified name (it might have dots).
     */
    public abstract ColumnInfo tryFindColumn(RowInfo info);

    /**
     * Returns true if the column path matches a path starting from the given tuple columns.
     * The names and types of each path element is examined, and they must match exactly.
     */
    public final boolean matches(Map<String, Column> columns) {
        return findMatch(columns) != null;
    }

    /**
     * @return null if not found
     */
    protected abstract Column findMatch(Map<String, Column> columns);

    private static final class Base extends ColumnExpr {
        private final TupleType mRowType;

        Base(int startPos, int endPos, TupleType rowType, Column column) {
            super(startPos, endPos, column);
            mRowType = rowType;
        }

        @Override
        public String name() {
            return mColumn == null ? "*" : mColumn.name();
        }

        @Override
        public boolean isNullable() {
            return mRowType.isNullable() || (mColumn != null && mColumn.type().isNullable());
        }

        @Override
        public void gatherEvalColumns(Consumer<Column> c) {
            if (mColumn != null) {
                c.accept(mColumn);
            }
        }

        @Override
        public Variable makeEval(EvalContext context) {
            var resultRef = context.refFor(this);
            var result = resultRef.get();
            return result != null ? result : resultRef.set(context.rowVar.invoke(name()));
        }

        @Override
        protected ColumnExpr parent() {
            return null;
        }

        @Override
        public void expandWildcard(Map<String, ColumnExpr> wildcards) {
            if (mColumn == null) {
                for (Column c : mRowType) {
                    wildcards.put(c.name(), make(startPos(), endPos(), mRowType, c));
                }
            }
        }

        @Override
        public boolean isPath() {
            return false;
        }

        @Override
        public boolean isHidden() {
            return mColumn != null && mColumn.isHidden();
        }

        @Override
        public Column firstColumn() {
            return mColumn;
        }

        @Override
        public ColumnInfo tryFindColumn(RowInfo info) {
            return info.allColumns.get(name());
        }

        @Override
        protected Column findMatch(Map<String, Column> columns) {
            if (mColumn == null) {
                return null;
            }
            Column match = columns.get(mColumn.name());
            return match != null && match.type().equals(type()) ? match : null;
        }

        private static final byte K_TYPE = KeyEncoder.allocType(), K_WILD = KeyEncoder.allocType();

        @Override
        protected void encodeKey(KeyEncoder enc) {
            if (mColumn == null) {
                enc.encode(this, K_WILD);
            } else if (enc.encode(this, K_TYPE)) {
                mColumn.encodeKey(enc);
            }
        }

        @Override
        public int hashCode() {
            return mRowType.hashCode() * 31 + Objects.hashCode(mColumn);
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this || obj instanceof Base other
                && mRowType.equals(other.mRowType) && Objects.equals(mColumn, other.mColumn);
        }

        @Override
        public String toString() {
            return mColumn == null ? "*" : unescape(mColumn.name());
        }
    }

    private static final class Sub extends ColumnExpr {
        private final ColumnExpr mParent;

        private String mFullName;

        Sub(int startPos, int endPos, ColumnExpr parent, Column column) {
            super(startPos, endPos, column);
            mParent = parent;
        }

        @Override
        public String name() {
            if (mFullName == null) {
                String pname = mParent.name();
                if (mColumn == null) {
                    mFullName = pname + '.' + '*';
                } else {
                    // FIXME: consider unescaping by path elements separately
                    mFullName = escape(unescape(pname) + '.' + mColumn.name());
                }
            }
            return mFullName;
        }

        @Override
        public boolean isNullable() {
            return mParent.isNullable() || (mColumn != null && mColumn.type().isNullable());
        }

        @Override
        public void gatherEvalColumns(Consumer<Column> c) {
            mParent.gatherEvalColumns(c);
        }

        @Override
        public Variable makeEval(EvalContext context) {
            if (mColumn == null) {
                throw new UnsupportedOperationException();
            }

            // FIXME
            throw null;
        }

        @Override
        protected ColumnExpr parent() {
            return mParent;
        }

        @Override
        public void expandWildcard(Map<String, ColumnExpr> wildcards) {
            if (mColumn == null && mParent.type() instanceof TupleType rowType) {
                // FIXME: consider unescaping by path elements separately
                String pname = unescape(mParent.name());
                for (Column c : rowType) {
                    String name = escape(pname + '.' + c.name());
                    wildcards.put(name, make(startPos(), endPos(), mParent, c));
                }
            }
        }

        @Override
        public boolean isPath() {
            return true;
        }

        @Override
        public boolean isHidden() {
            return mParent.isHidden() || (mColumn != null && mColumn.isHidden());
        }

        @Override
        public Column firstColumn() {
            return mParent.firstColumn();
        }

        @Override
        public ColumnInfo tryFindColumn(RowInfo info) {
            if (mColumn == null) {
                return null;
            }

            ColumnInfo head = mParent.tryFindColumn(info);

            if (head == null || head.isScalarType()) {
                return null;
            }

            try {
                info = RowInfo.find(head.type);
            } catch (IllegalArgumentException e) {
                return null;
            }

            String name = mColumn.name();
            ColumnInfo tail = info.allColumns.get(name);

            if (tail == null) {
                return null;
            }

            ColumnInfo ci = tail.copy();
            ci.name = head.name + '.' + name;

            return ci;
        }

        @Override
        protected Column findMatch(Map<String, Column> columns) {
            if (mColumn == null) {
                return null;
            }
            Column match = mParent.findMatch(columns);
            if (match == null || !(match.type() instanceof TupleType tt)) {
                return null;
            }
            match = tt.findColumn(mColumn.name());
            return match != null && match.type().equals(type()) ? match : null;
        }

        private static final byte K_TYPE = KeyEncoder.allocType(), K_WILD = KeyEncoder.allocType();

        @Override
        protected void encodeKey(KeyEncoder enc) {
            if (mColumn == null) {
                if (enc.encode(this, K_WILD)) {
                    mParent.encodeKey(enc);
                }
            } else if (enc.encode(this, K_TYPE)) {
                mParent.encodeKey(enc);
                mColumn.encodeKey(enc);
            }
        }

        @Override
        public int hashCode() {
            return mParent.hashCode() * 31 + Objects.hashCode(mColumn);
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this || obj instanceof Sub other
                && mParent.equals(other.mParent) && Objects.equals(mColumn, other.mColumn);
        }

        @Override
        public String toString() {
            return defaultToString();
        }

        @Override
        public void appendTo(StringBuilder b) {
            mParent.appendTo(b);
            b.append('.');
            if (mColumn == null) {
                b.append('*');
            } else {
                b.append(unescape(mColumn.name()));
            }
        }
    }

    /* FIXME: remove / fold into the new Sub class


        boolean nullable = false;
        Sub sub = null;

        for (String subName : mColumn.subNames()) {
            Column subColumn = rowType.findColumn(subName);
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
    */
}
