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
import java.util.Set;

import java.util.function.Consumer;

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Define an expression for representing a range of values.
 *
 * @author Brian S. O'Neill
 * @see Range
 */
public final class RangeExpr extends Expr {
    /**
     * @param start optional
     * @param end optional
     */
    public static Expr make(int startPos, int endPos, Expr start, Expr end) {
        if (start != null && end != null) {
            Type type = start.type().commonType(end.type(), -1);
            if (type == null) {
                throw new QueryException("No common type for range", startPos, endPos);
            }
            start = start.asType(type);
            end = end.asType(type);
        }

        Type type = BasicType.make(Range.class, Type.TYPE_REFERENCE);

        constant: {
            Range value;

            if (start == null) {
                if (end == null) {
                    value = new Range(null, null);
                } else if (end instanceof ConstantExpr cend) {
                    value = new Range(null, cend.value());
                } else {
                    break constant;
                }
            } else if (start instanceof ConstantExpr cstart) {
                if (end == null) {
                    value = new Range(cstart.value(), null);
                } else if (end instanceof ConstantExpr cend) {
                    value = new Range(cstart.value(), cend.value());
                } else {
                    break constant;
                }
            } else {
                break constant;
            }

            return ConstantExpr.make(startPos, endPos, type, value);
        }

        return new RangeExpr(startPos, endPos, type, start, end);
    }

    private final Type mType;
    private final Expr mStart, mEnd;

    private RangeExpr(int startPos, int endPos, Type type, Expr start, Expr end) {
        super(startPos, endPos);
        mType = type;
        mStart = start;
        mEnd = end;
    }

    @Override
    public Type type() {
        return mType;
    }

    @Override
    public Expr asType(Type type) {
        if (mType.equals(type)) {
            return this;
        }
        throw new QueryException("Cannot convert " + mType + " to " + type, startPos(), endPos());
    }

    @Override
    public int maxArgument() {
        int max = 0;
        if (mStart != null) {
            max = mStart.maxArgument();
        }
        if (mEnd != null) {
            max = Math.max(max, mEnd.maxArgument());
        }
        return max;
    }

    @Override
    public boolean isPureFunction() {
        return (mStart == null || mStart.isPureFunction())
            && (mEnd == null || mEnd.isPureFunction());
    }

    @Override
    public boolean isTrivial() {
        return false;
    }

    @Override
    public boolean isNullable() {
        return false;
    }

    @Override
    public boolean isConstant() {
        return (mStart == null || mStart.isConstant())
            && (mEnd == null || mEnd.isConstant());
    }

    @Override
    public boolean isGrouping() {
        return (mStart != null && mStart.isGrouping())
            || (mEnd != null && mEnd.isGrouping());
    }

    @Override
    public boolean isAccumulating() {
        return (mStart != null && mStart.isAccumulating())
            || (mEnd != null && mEnd.isAccumulating());
    }

    @Override
    public boolean isAggregating() {
        return (mStart != null && mStart.isAggregating())
            || (mEnd != null && mEnd.isAggregating());
    }

    @Override
    public Expr asAggregate(Set<String> group) {
        Expr start = mStart == null ? null : mStart.asAggregate(group);
        Expr end = mEnd == null ? null : mEnd.asAggregate(group);
        if (start == mStart && end == mEnd) {
            return this;
        }
        return new RangeExpr(startPos(), endPos(), mType, start, end);
    }

    @Override
    public Expr replace(Map<Expr, ? extends Expr> replacements) {
        Expr replaced = replacements.get(this);
        if (replaced != null) {
            return replaced;
        }
        Expr start = mStart == null ? null : mStart.replace(replacements);
        Expr end = mEnd == null ? null : mEnd.replace(replacements);
        if (start == mStart && end == mEnd) {
            return this;
        }
        return new RangeExpr(startPos(), endPos(), mType, start, end);
    }

    @Override
    public void gatherEvalColumns(Consumer<Column> c) {
        if (mStart != null) {
            mStart.gatherEvalColumns(c);
        }
        if (mEnd != null) {
            mEnd.gatherEvalColumns(c);
        }
    }

    @Override
    protected Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef) {
        Object startVar = mStart == null ? null : mStart.makeEval(context);
        Object endVar = mEnd == null ? null : mEnd.makeEval(context);
        return context.methodMaker().new_(Range.class, startVar, endVar);
    }

    @Override
    public boolean canThrowRuntimeException() {
        return (mStart != null && mStart.canThrowRuntimeException())
            || (mEnd != null && mEnd.canThrowRuntimeException());
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            enc.encodeByte((mStart == null ? 0 : 2) | (mEnd == null ? 0 : 1));
            if (mStart != null) {
                mStart.encodeKey(enc);
            }
            if (mEnd != null) {
                mEnd.encodeKey(enc);
            }
        }
    }

    @Override
    public int hashCode() {
        int hash = 2099425649;
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
            obj instanceof RangeExpr re
            && Objects.equals(mStart, re.mStart) && Objects.equals(mEnd, re.mEnd);
    }

    @Override
    public String toString() {
        return defaultToString();
    }

    @Override
    public void appendTo(StringBuilder b) {
        if (mStart != null) {
            mStart.appendTo(b);
        }
        b.append("..");
        if (mEnd != null) {
            mEnd.appendTo(b);
        }
    }
}
