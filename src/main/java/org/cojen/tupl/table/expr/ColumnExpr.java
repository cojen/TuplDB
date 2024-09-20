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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import java.util.function.Consumer;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
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
public abstract sealed class ColumnExpr extends Expr implements Attr {
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
        Type type;
        if (column == null) {
            type = AnyType.THE;
        } else {
            type = column.type();
            if (parent.isNullable()) {
                type = type.nullable();
            }
        }

        return new Sub(startPos, endPos, parent, type, column);
    }

    protected final Column mColumn;

    protected boolean mHasSubColumns;

    protected ColumnExpr(int startPos, int endPos, Column column) {
        super(startPos, endPos);
        mColumn = column;
    }

    @Override
    public Type type() {
        return originalType();
    }

    /**
     * The type of this expression can differ from the original type if it needs to support
     * nulls. If the original type is primitive, then the nullable type is the boxed peer.
     */
    public final Type originalType() {
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
    public final boolean isConstant() {
        return false;
    }

    @Override
    public final ColumnExpr sourceColumn() {
        return this;
    }

    @Override
    public final boolean isOrderDependent() {
        return false;
    }

    @Override
    public final boolean isGrouping() {
        return false;
    }

    @Override
    public final boolean isAccumulating() {
        return false;
    }

    @Override
    public final boolean isAggregating() {
        return false;
    }

    @Override
    public Expr asAggregate(Set<String> group) {
        if (!group.contains(name())) {
            throw queryException("Column isn't part of the aggregation group or " +
                                 "wrapped by an aggregation function");
        }
        return CallExpr.make(startPos(), endPos(), "first", List.of(this), Map.of(),
                             new StandardFunctionFinder.first(null));
    }

    @Override
    public Expr asWindow(Map<ColumnExpr, AssignExpr> newAssignments) {
        AssignExpr assign = newAssignments.get(this);

        if (assign != null) {
            return VarExpr.make(startPos(), endPos(), assign);
        }

        Expr expr = CallExpr.make(startPos(), endPos(), "first", List.of(this),
                                  Map.of("rows", RangeExpr.constant(Range.zero())),
                                  new StandardFunctionFinder.first(null));

        // Wrap the call in an AssignExpr expression for two reasons. First, the column might
        // be directly referenced by ProjExpr, and so it needs a name. Second, by storing the
        // AssignExpr in the newAssignments map future references to the column can be replaced
        // with a VarExpr (see above), reducing duplicate window function work.
        
        assign = AssignExpr.make(startPos(), endPos(), name(), expr);
        newAssignments.put(this, assign);
        return assign;
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
            if (mColumn == null) {
                throw new UnsupportedOperationException();
            }

            var resultRef = context.refFor(this);
            var result = resultRef.get();
            if (result != null) {
                return result;
            }

            result = resultRef.set(context.sourceRowVar().invoke(mColumn.name()));

            if (mHasSubColumns && isNullable()) {
                // Define notNull/isNull labels for the sub columns to insert code into.
                // See Sub.makeEval.
                MethodMaker mm = context.methodMaker();
                Label notNull = mm.label();
                result.ifNe(null, notNull);
                Label isNull = mm.label().here();
                Label cont = mm.label().goto_();
                notNull.here();
                cont.here();
                resultRef.attachment = new Label[] {notNull, isNull};
            }

            return result;
        }

        @Override
        protected Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
            throw new AssertionError();
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
        private final Type mType;
        private final ColumnExpr mParent;

        private String mFullName;

        Sub(int startPos, int endPos, ColumnExpr parent, Type type, Column column) {
            super(startPos, endPos, column);
            mType = type;
            mParent = parent;
            parent.mHasSubColumns = true;
        }

        @Override
        public Type type() {
            return mType;
        }

        @Override
        public String name() {
            if (mFullName == null) {
                String pname = mParent.name();
                if (mColumn == null) {
                    mFullName = pname + '.' + '*';
                } else {
                    // TODO: consider unescaping by path elements separately
                    mFullName = escape(unescape(pname) + '.' + mColumn.name());
                }
            }
            return mFullName;
        }

        @Override
        public boolean isNullable() {
            return mType.isNullable();
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

            var resultRef = context.refFor(this);
            var result = resultRef.get();
            if (result != null) {
                return result;
            }

            var parentVar = mParent.makeEval(context);
            String subName = mColumn.name();

            if (!mParent.isNullable()) {
                return resultRef.set(parentVar.invoke(subName));
            }

            // Rather than checking if the parent was null or not each time, assign a
            // not-null or null result within the parent code blocks. See Base.makeEval.
            var labels = (Label[]) context.refFor(mParent).attachment;
            labels[0].insert(() -> resultRef.set(parentVar.invoke(subName).box()));
            labels[1].insert(() -> resultRef.get().set(null));

            return resultRef.get();
        }

        @Override
        protected Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
            throw new AssertionError();
        }

        @Override
        protected ColumnExpr parent() {
            return mParent;
        }

        @Override
        public void expandWildcard(Map<String, ColumnExpr> wildcards) {
            if (mColumn == null && mParent.type() instanceof TupleType rowType) {
                // TODO: consider unescaping by path elements separately
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
}
