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

import java.util.Collection;
import java.util.Map;

import java.util.function.Consumer;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.table.RowInfo;

import org.cojen.tupl.table.filter.OpaqueFilter;
import org.cojen.tupl.table.filter.RowFilter;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Represents an AST expression element of a query.
 *
 * @author Brian S. O'Neill
 */
public abstract sealed class Expr
    permits BinaryOpExpr, ColumnExpr, ConstantExpr, InExpr,
    ParamExpr, RelationExpr, VarExpr, WrappedExpr
{
    private final int mStartPos, mEndPos;

    /**
     * @param startPos source code start position, zero-based, inclusive; is -1 if not applicable
     * @param endPos source code end position, zero-based, exclusive; is -1 if not applicable
     */
    Expr(int startPos, int endPos) {
        mStartPos = startPos;
        mEndPos = endPos;
    }

    /**
     * @return source code start position, zero-based, inclusive; is -1 if not applicable
     */
    public final int startPos() {
        return mStartPos;
    }

    /**
     * @return source code end position, zero-based, exclusive; is -1 if not applicable
     */
    public final int endPos() {
        return mEndPos;
    }

    public abstract Type type();

    /**
     * Return this or a replacement expression. If a conversion is required, an exact
     * conversion is performed, which can throw an exception at runtime.
     */
    public abstract Expr asType(Type type);

    /**
     * Apply an arithmetic negation operation.
     *
     * @param widen pass true to allow a widening conversion (applicable to int/long min values)
     */
    public final Expr negate(boolean widen) {
        return negate(mStartPos, widen);
    }

    public Expr negate(int startPos, boolean widen) {
        return BinaryOpExpr.make
            (startPos, mEndPos,
             Token.T_MINUS, ConstantExpr.make(startPos, mEndPos, 0), this);
    }

    /**
     * Apply a boolean not operation.
     */
    public final Expr not() {
        return not(mStartPos);
    }

    public Expr not(int startPos) {
        if (type().isInteger()) {
            return NotExpr.make(startPos, mEndPos, this);
        }

        return BinaryOpExpr.make
            (startPos, mEndPos,
             Token.T_EQ, asType(BasicType.BOOLEAN), ConstantExpr.make(startPos, mEndPos, false));
    }

    /**
     * Returns the highest query argument needed by this expression, which is zero if none are
     * needed.
     */
    public abstract int maxArgument();

    /**
     * Returns true if this expression represents a pure function with respect to the current
     * row, returning the same result upon each invocation.
     */
    public abstract boolean isPureFunction();

    /**
     * Performs a best effort conversion of this expression into a RowFilter. Any expressions
     * which cannot be converted are represented by OpaqueFilters which have the expression
     * attached.
     *
     * @param columns all converted columns are put into this map
     */
    public RowFilter toRowFilter(RowInfo info, Map<String, ColumnExpr> columns) {
        return new OpaqueFilter(false, this);
    }

    /**
     * Returns true if any path element is null or the evaluated result can be null.
     */
    public abstract boolean isNullable();

    /**
     * Returns a ColumnExpr if this expression just evaluates a column with no transformations.
     * Null is returned otherwise.
     */
    public ColumnExpr directColumn() {
        return null;
    }

    /**
     * Add to the given collection all the columns that makeEval will directly use.
     */
    public void gatherEvalColumns(Collection<Column> c) {
        gatherEvalColumns(c::add);
    }

    /**
     * Pass to the given consumer all the columns that makeEval will directly use.
     */
    public abstract void gatherEvalColumns(Consumer<Column> c);

    /**
     * Generates code which evaluates the expression. The context tracks expressions which have
     * already been evaluated and is updated by this method.
     */
    public abstract Variable makeEval(EvalContext context);

    /**
     * Provides an implementation of the makeEval method which calls makeFilter and returns
     * true or false.
     */
    protected Variable makeEvalForFilter(EvalContext context) {
        EvalContext.ResultRef resultRef;

        if (isPureFunction()) {
            resultRef = context.refFor(this);
            var result = resultRef.get();
            if (result != null) {
                return result;
            }
        } else {
            resultRef = null;
        }

        MethodMaker mm = context.methodMaker();

        Label pass = mm.label();
        Label fail = mm.label();

        makeFilter(context, pass, fail);

        var result = resultRef == null ? mm.var(boolean.class) : resultRef.toSet(boolean.class);

        fail.here();
        result.set(false);
        Label cont = mm.label().goto_();
        pass.here();
        result.set(true);
        cont.here();

        return result;
    }

    /**
     * Generates code which evaluates an expression for branching to a pass or fail label.
     * Short-circuit logic is used, and so the expression might only be partially evaluated.
     *
     * @throws IllegalStateException if unsupported
     */
    public void makeFilter(EvalContext context, Label pass, Label fail) {
        makeEval(context).ifTrue(pass);
        fail.goto_();
    }

    /**
     * Generates filter code which returns a boolean Variable.
     *
     * @throws IllegalStateException if unsupported
     */
    public Variable makeFilterEval(EvalContext context) {
        Variable result = makeEval(context);
        if (result.classType() != boolean.class) {
            throw new IllegalStateException();
        }
        return result;
    }

    /**
     * Returns true if the code generated by this expression might throw a RuntimeException. A
     * return value of false doesn't indicate that a RuntimeException isn't possible, but
     * instead it indicates that it's unlikely.
     */
    public boolean canThrowRuntimeException() {
        return true;
    }

    /**
     * Calls Variable.set or Variable.setExact.
     */
    public static void setAny(Variable v, Object value) {
        Class<?> type = v.classType();
        if (type == null || mustSetExact(type, value)) {
            v.setExact(value);
        } else {
            v.set(value);
        }
    }

    public static boolean mustSetExact(Class<?> type, Object value) {
        return value != null && !(value instanceof Variable) && !type.isPrimitive()
            && !String.class.isAssignableFrom(type)
            && !Class.class.isAssignableFrom(type);
    }

    /**
     * Returns a cache key instance by calling encodeKey.
     */
    protected final TupleKey makeKey() {
        var enc = new KeyEncoder();
        encodeKey(enc);
        return enc.finish();
    }

    /**
     * Encodes this expression into such that it can be used as a cache key for generated code.
     * Although the Expr itself can be used as such, the cache key should have a smaller memory
     * footprint.
     *
     * Each encodable entity must define the following constant:
     *
     *     private static final byte K_TYPE = KeyEncoder.allocType();
     *
     * Simple entities can simply call encodeType(K_TYPE), but multipart entities should call
     * encode(this, K_TYPE) and then only proceed with the encoding when true is returned. This
     * reduces entity encoding duplication and prevents infinite cycles.
     * 
     * Although the key doesn't need to be decoded, the encoding format should be defined as if
     * it could be decoded, to prevent false key matches. Like the equals method, source code
     * positions should not be included.
     */
    protected abstract void encodeKey(KeyEncoder enc);

    /**
     * Note: ignores the source code positions
     */
    @Override
    public abstract int hashCode();

    /**
     * Note: ignores the source code positions
     */
    @Override
    public abstract boolean equals(Object obj);

    /**
     * Note: ignores the source code positions
     */
    @Override
    public abstract String toString();

    protected void appendTo(StringBuilder b) {
        b.append(toString());
    }

    protected final String defaultToString() {
        var b = new StringBuilder();
        appendTo(b);
        return b.toString();
    }
}
