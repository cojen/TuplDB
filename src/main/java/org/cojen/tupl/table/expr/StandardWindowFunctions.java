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
     * Defines a window function which computes a moving count.
     */
    static final class count extends WindowFunction {
        count(Type resultType, Type valueType, Type originalType, Frame frame) {
            super(resultType, valueType, originalType, frame);
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
