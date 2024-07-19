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

import java.math.BigDecimal;

import org.cojen.maker.Variable;

import static org.cojen.tupl.table.expr.Type.*;

/**
 * Defines a collection of standard window functions. Many of them should only be constructed
 * by the standard aggregation functions, when the validate step sees that a named window
 * function parameter was provided. This effectively converts the aggregation function into a
 * window function.
 *
 * @author Brian S. O'Neill
 * @see StandardFunctionFinder
 */
final class StandardWindowFunctions {
    /**
     * Defines a window function which computes a moving sum.
     */
    static final class sum extends WindowFunction {
        sum(Type type, Type originalType, Frame frame) {
            super(type, originalType, frame);
        }

        @Override
        protected Class<?> bufferType() {
            Type type = originalType();
            Class<?> clazz = type.clazz();
            int typeCode = type.plainTypeCode();
            boolean nullable = type.isNullable();

            switch (typeCode) {
                case TYPE_UBYTE, TYPE_USHORT, TYPE_UINT -> {
                    return nullable ? WindowBuffer.OfULongObj.class : WindowBuffer.OfULong.class;
                }
                case TYPE_BYTE, TYPE_SHORT, TYPE_INT -> {
                    return nullable ? WindowBuffer.OfLongObj.class : WindowBuffer.OfLong.class;
                }
                case TYPE_FLOAT, TYPE_DOUBLE -> {
                    return nullable ? WindowBuffer.OfDoubleObj.class : WindowBuffer.OfDouble.class;
                }
                case TYPE_ULONG, TYPE_LONG, TYPE_BIG_INTEGER -> {
                    return WindowBuffer.OfBigInteger.class;
                }
                case TYPE_BIG_DECIMAL -> {
                    return WindowBuffer.OfBigDecimal.class;
                }
                default -> {
                    // See StandardFunctionFinder.sum.
                    throw new AssertionError();
                }
            }
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
        avg(Type type, Type originalType, Frame frame) {
            super(type, originalType, frame);
        }

        @Override
        protected Class<?> bufferType() {
            Class<?> clazz = type().clazz();
            if (clazz == double.class) {
                return WindowBuffer.OfDouble.class;
            } else if (clazz == Double.class) {
                return WindowBuffer.OfDoubleObj.class;
            } else if (clazz == BigDecimal.class) {
                return WindowBuffer.OfBigDecimal.class;
            } else {
                // See StandardFunctionFinder.avg.
                throw new AssertionError();
            }
        }

        @Override
        protected Variable compute(Variable bufferVar, Object frameStart, Object frameEnd) {
            return bufferVar.invoke("frameAverage", frameStart, frameEnd);
        }
    }
}
