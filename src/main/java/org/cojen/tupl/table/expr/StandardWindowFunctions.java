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

import org.cojen.maker.Variable;

/**
 * Defines a collection of standard window functions. In general, they should only be
 * constructed by the standard aggregation functions, when the validate step sees that a named
 * window function parameter was provided. This effectively converts the aggregation function
 * into a window function.
 *
 * @author Brian S. O'Neill
 * @see StandardFunctionFinder
 */
final class StandardWindowFunctions {
    /**
     * Defines a window function which produces the first value in the moving window.
     */
    static final class first extends WindowFunction {
        first(Type resultType, Type valueType, Type originalType, Frame frame) {
            super(resultType, valueType, originalType, frame);
        }

        @Override
        protected Variable compute(Variable bufferVar, Object frameStart, Object frameEnd) {
            if (frameStart instanceof Long v && v == 0L) {
                return bufferVar.invoke("frameCurrent");
            } else {
                return bufferVar.invoke("frameGetOrFirst", frameStart);
            }
        }
    }

    /**
     * Defines a window function which produces the last value in the moving window.
     */
    static final class last extends WindowFunction {
        last(Type resultType, Type valueType, Type originalType, Frame frame) {
            super(resultType, valueType, originalType, frame);
        }

        @Override
        protected Variable compute(Variable bufferVar, Object frameStart, Object frameEnd) {
            if (frameEnd instanceof Long v && v == 0L) {
                return bufferVar.invoke("frameCurrent");
            } else {
                return bufferVar.invoke("frameGetOrLast", frameEnd);
            }
        }
    }

    /**
     * Defines a window function which computes a moving minimum.
     */
    static final class min extends WindowFunction {
        min(Type resultType, Type valueType, Type originalType, Frame frame) {
            super(resultType, valueType, originalType, frame);
        }

        @Override
        protected Variable compute(Variable bufferVar, Object frameStart, Object frameEnd) {
            String method;
            if (originalType().isNullLow()) {
                method = type().isNullable() ? "frameMinOrNullNL" : "frameMinNL";
            } else {
                method = type().isNullable() ? "frameMinOrNull" : "frameMin";
            }

            return bufferVar.invoke(method, frameStart, frameEnd);
        }
    }

    /**
     * Defines a window function which computes a moving maximum.
     */
    static final class max extends WindowFunction {
        max(Type resultType, Type valueType, Type originalType, Frame frame) {
            super(resultType, valueType, originalType, frame);
        }

        @Override
        protected Variable compute(Variable bufferVar, Object frameStart, Object frameEnd) {
            String method;
            if (originalType().isNullLow()) {
                method = type().isNullable() ? "frameMaxOrNullNL" : "frameMaxNL";
            } else {
                method = type().isNullable() ? "frameMaxOrNull" : "frameMax";
            }

            return bufferVar.invoke(method, frameStart, frameEnd);
        }
    }

    /**
     * Defines a window function which computes a moving count.
     */
    static final class count extends WindowFunction {
        count(Type resultType, Type valueType, Type originalType, Frame frame) {
            super(resultType, valueType, originalType, frame);
        }

        @Override
        protected Variable evalArg(GroupContext context) {
            if (context.args().isEmpty()) {
                // TODO: Filling a buffer with "true" values isn't terribly efficient. Instead,
                // use a simple counter value. It might be possible to implement a WindowBuffer
                // which does this.
                return context.methodMaker().var(boolean.class).set(true);
            } else {
                return super.evalArg(context);
            }
        }

        @Override
        protected Variable compute(Variable bufferVar, Object frameStart, Object frameEnd) {
            return bufferVar.invoke("frameCount", frameStart, frameEnd);
        }
    }

    /**
     * Defines a window function which computes a moving sum.
     */
    static final class sum extends WindowFunction {
        sum(Type resultType, Type valueType, Type originalType, Frame frame) {
            super(resultType, valueType, originalType, frame);
        }

        @Override
        protected Variable compute(Variable bufferVar, Object frameStart, Object frameEnd) {
            return bufferVar.invoke("frameSum", frameStart, frameEnd);
        }
    }

    /**
     * Defines a window function which computes a moving average.
     */
    static final class avg extends WindowFunction {
        avg(Type resultType, Type valueType, Type originalType, Frame frame) {
            super(resultType, valueType, originalType, frame);
        }

        @Override
        protected Variable compute(Variable bufferVar, Object frameStart, Object frameEnd) {
            String method = type().isNullable() ? "frameAverageOrNull" : "frameAverage";
            return bufferVar.invoke(method, frameStart, frameEnd);
        }
    }
}
