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

import java.lang.constant.Constable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.cojen.tupl.QueryException;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.table.RowInfo;

import org.cojen.tupl.table.filter.ExprFilter;
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
    permits BinaryOpExpr, CallExpr, ColumnExpr, ConstantExpr, InExpr,
    ParamExpr, RangeExpr, RelationExpr, VarExpr, WrappedExpr
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

    final QueryException queryException(String message) {
        return new QueryException(message, mStartPos, mEndPos);
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

        Type boolType = BasicType.BOOLEAN;

        if (isNullable()) {
            boolType = boolType.nullable();
        }

        return BinaryOpExpr.make
            (startPos, mEndPos,
             Token.T_XOR, asType(boolType), ConstantExpr.make(startPos, mEndPos, true));
    }

    /**
     * Returns true if calling the not method doesn't introduce new terms.
     */
    public boolean supportsLogicalNot() {
        return false;
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
     * Returns true if evaluating this expression is cheap.
     */
    public boolean isTrivial() {
        return false;
    }

    /**
     * Performs a best effort conversion of this expression into a RowFilter. Any expressions
     * which cannot be converted are represented by ExprFilters which refer to the expression.
     *
     * @param columns all converted columns are put into this map
     */
    public RowFilter toRowFilter(RowInfo info, Map<String, ColumnExpr> columns) {
        return new ExprFilter(this);
    }

    /**
     * Returns true if any path element is null or the evaluated result can be null.
     */
    public abstract boolean isNullable();

    /**
     * Returns true if this expression evaluates to a constant value.
     */
    public abstract boolean isConstant();

    /**
     * Returns true if this expression evaluates to a constant value of null.
     */
    public boolean isNull() {
        return false;
    }

    /**
     * Returns true if this expression evaluates to a constant value of zero.
     */
    public boolean isZero() {
        return false;
    }

    /**
     * Returns true if this expression evaluates to a constant value of one.
     */
    public boolean isOne() {
        return false;
    }

    /**
     * Returns a ColumnExpr if this expression just evaluates a column with no transformations.
     * Null is returned otherwise.
     */
    public ColumnExpr sourceColumn() {
        return null;
    }

    /**
     * Returns true if this expression is calling an accumulating function and the generated
     * code depends on the input values arriving in a specific order. This generally implies
     * the use of a WindowFunction with a "range" frame mode.
     */
    public abstract boolean isOrderDependent();

    /**
     * Returns true if this expression depends on a function which needs a projection group.
     */
    public abstract boolean isGrouping();

    /**
     * Returns true if this expression directly or indirectly depends upon a function which
     * accumulates group results, which also implies that isGrouping returns true.
     */
    public abstract boolean isAccumulating();

    /**
     * Returns true if this expression directly or indirectly depends upon an aggregation
     * function, which also implies that isAccumulating returns true.
     */
    public abstract boolean isAggregating();

    /**
     * Return this or a replacement expression such that all direct column accesses are wrapped
     * by the "first" aggregation function. A QueryException is thrown if any column which needs
     * to be wrapped isn't in the specified group.
     */
    public abstract Expr asAggregate(Set<String> group);

    /**
     * Return this or a replacement expression such that all direct column accesses are wrapped
     * by the "first" window function over a frame consisting of just the current row.
     *
     * @param newAssignments is updated to store ColumnExpr to AssignExpr replacements; matching
     * ColumnExpr instances should be replaced with VarExpr instances instead
     */
    public abstract Expr asWindow(Map<ColumnExpr, AssignExpr> newAssignments);

    /**
     * Returns true if this expression represents a range and is guaranteed to include the
     * current row of a window frame, which is denoted with a constant value of zero.
     */
    public boolean isRangeWithCurrent() {
        return false;
    }

    /**
     * Returns this or a replacement expression such that expressions in the given map are
     * replaced.
     *
     * @param replacements maps expressions to replace with their replacements
     */
    public Expr replace(Map<Expr, ? extends Expr> replacements) {
        Expr replaced = replacements.get(this);
        return replaced == null ? this : replaced;
    }

    /**
     * Returns a new LazyValue instance backed by this expression.
     */
    public LazyValue lazyValue(EvalContext context) {
        return new LazyValue(context, this);
    }

    /**
     * Add to the given collection all the columns that makeEval will directly use. Only the
     * first column in a path is selected.
     */
    public final void gatherEvalColumns(Collection<Column> c) {
        gatherEvalColumns(c::add);
    }

    /**
     * Pass to the given consumer all the columns that makeEval will directly use. Only the
     * first column in a path is selected.
     */
    public abstract void gatherEvalColumns(Consumer<Column> c);

    /**
     * For each gathered column, define a ProjExpr which references it. Only the first column
     * in a path is selected. Existing map entries aren't replaced.
     *
     * @param projMap maps column names to ProjExpr instances with the same name
     * @param flags see ProjExpr
     * @param observer is called for each ProjExpr which is put into the map
     */
    public void gatherEvalColumns(TupleType fromType, Map<String, ProjExpr> projMap, int flags,
                                  Consumer<ProjExpr> observer)
    {
        gatherEvalColumns(c -> {
            projMap.computeIfAbsent(c.name(), k -> {
                ProjExpr pe = ProjExpr.make(fromType, c, flags);
                if (observer != null) {
                    observer.accept(pe);
                }
                return pe;
            });
        });
    }

    /**
     * Generates code which evaluates the expression. The context tracks expressions which have
     * already been evaluated and is updated by this method.
     */
    public Variable makeEval(EvalContext context) {
        EvalContext.ResultRef resultRef;

        if (isTrivial() || !isPureFunction()) {
            resultRef = null;
        } else {
            resultRef = context.refFor(this);
            var result = resultRef.get();
            if (result != null) {
                return result;
            }
        }

        Variable resultVar = doMakeEval(context, resultRef);

        if (resultRef != null) {
            resultVar = resultRef.set(resultVar);
        }

        return resultVar;
    }

    /**
     * Implementation of makeEval, which should only be called via the makeEval method.
     *
     * @param resultRef is non-null if this expression is a non-trivial pure function
     */
    protected abstract Variable doMakeEval(EvalContext context, EvalContext.ResultRef resultRef);

    /**
     * Provides an implementation of the doMakeEval method which calls makeFilter and returns
     * true or false.
     */
    protected Variable doMakeEvalForFilter(EvalContext context, EvalContext.ResultRef resultRef) {
        MethodMaker mm = context.methodMaker();

        Label pass = mm.label();
        Label fail = mm.label();

        makeFilter(context, pass, fail);

        var result = mm.var(boolean.class);

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
            && !Constable.class.isAssignableFrom(type);
    }

    @FunctionalInterface
    public static interface Replacer<E> {
        E apply(int i, E element);
    }

    /**
     * Apply a function to replace all the elements of list, returning a new list if any
     * replacements were actually made. The original list is returned otherwise.
     *
     * @param i first element to replace (usually 0)
     */
    public static <E> List<E> replaceElements(List<E> list, int i, Replacer<E> replacer) {
        if (!list.isEmpty()) {
            boolean copied = false;
            for (; i<list.size(); i++) {
                E element = list.get(i);
                E replacement = replacer.apply(i, element);
                if (replacement != element) {
                    if (!copied) {
                        list = new ArrayList<>(list);
                        copied = true;
                    }
                    list.set(i, replacement);
                }
            }
        }

        return list;
    }

    /**
     * Apply a function to replace all the values of map, returning a map list if any
     * replacements were actually made. The original map is returned otherwise.
     */
    public static <K, V> Map<K, V> replaceValues(Map<K, V> map, BiFunction<K, V, V> replacer) {
        if (!map.isEmpty()) {
            boolean copied = false;
            for (K key : map.keySet()) {
                V value = map.get(key);
                V replacement = replacer.apply(key, value);
                if (replacement != value) {
                    if (!copied) {
                        map = new LinkedHashMap<>(map);
                        copied = true;
                    }
                    map.put(key, replacement);
                }
            }
        }

        return map;
    }

    /**
     * Returns a cache key instance by calling encodeKey.
     */
    public final TupleKey makeKey() {
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

    public void appendTo(StringBuilder b) {
        b.append(this);
    }

    protected final String defaultToString() {
        var b = new StringBuilder();
        appendTo(b);
        return b.toString();
    }
}
