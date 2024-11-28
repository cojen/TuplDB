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

import java.math.BigInteger;
import java.math.MathContext;

import org.cojen.maker.Variable;

import org.cojen.tupl.table.ConvertUtils;
import org.cojen.tupl.table.ColumnInfo;

import static org.cojen.tupl.table.expr.Token.*;
import static org.cojen.tupl.table.expr.Type.*;

import java.lang.Long;

/**
 * Collection of utilities for generating arithmetic code.
 *
 * @author Brian S. O'Neill
 */
final class Arithmetic {
    // Operations which aren't defined by the Token class, which must not collide with any
    // Token arithmetic operations, or else the switch statements won't compile.
    static final int OP_MIN = 17, OP_MAX = 18;

    /**
     * Evaluate an operation against two variables, of the same type, non-null, and return a
     * new variable of the same type. If the operation isn't supported, a null Variable
     * instance is returned. Exact arithmetic is performed, and so an exception can be thrown
     * at runtime, depending on the type and operation.
     */
    public static Variable eval(ColumnInfo type, int op, Variable left, Variable right) {
        return switch (type.plainTypeCode()) {
            case TYPE_BOOLEAN -> Arithmetic.Bool.eval(op, left, right);
            case TYPE_UBYTE -> Arithmetic.UByte.eval(op, left, right);
            case TYPE_USHORT -> Arithmetic.UShort.eval(op, left, right);
            case TYPE_UINT -> Arithmetic.UInteger.eval(op, left, right);
            case TYPE_ULONG -> Arithmetic.ULong.eval(op, left, right);
            case TYPE_BYTE -> Arithmetic.Byte.eval(op, left, right);
            case TYPE_SHORT -> Arithmetic.Short.eval(op, left, right);
            case TYPE_INT, TYPE_LONG -> Arithmetic.Integer.eval(op, left, right);
            case TYPE_FLOAT, TYPE_DOUBLE -> Arithmetic.Float.eval(op, left, right);
            case TYPE_BIG_INTEGER -> Arithmetic.Big.eval(op, left, right);
            case TYPE_BIG_DECIMAL -> Arithmetic.BigDecimal.eval(op, left, right);
            default -> null;
        };
    }

    /**
     * Generates code for computing the minimum value, neither of which can be null. If the
     * type isn't supported, a null Variable instance is returned.
     */
    public static Variable min(ColumnInfo type, Variable left, Variable right) {
        return eval(type, OP_MIN, left, right);
    }

    /**
     * Generates code for computing the maximum value, neither of which can be null. If the
     * type isn't supported, a null Variable instance is returned.
     */
    public static Variable max(ColumnInfo type, Variable left, Variable right) {
        return eval(type, OP_MAX, left, right);
    }

    /**
     * Returns the static minimum value for the given type. If the type isn't a number or
     * doesn't have a minimum value, null is returned.
     */
    public static Object min(ColumnInfo type) {
        return switch (type.plainTypeCode()) {
            case TYPE_UBYTE -> (byte) 0;
            case TYPE_USHORT -> (short) 0;
            case TYPE_UINT -> 0;
            case TYPE_ULONG -> 0L;
            case TYPE_BYTE -> java.lang.Byte.MIN_VALUE;
            case TYPE_SHORT -> java.lang.Short.MIN_VALUE;
            case TYPE_INT -> java.lang.Integer.MIN_VALUE;
            case TYPE_LONG -> java.lang.Long.MIN_VALUE;
            case TYPE_FLOAT -> java.lang.Float.MIN_VALUE;
            case TYPE_DOUBLE -> java.lang.Double.MIN_VALUE;
            default -> null;
        };
    }

    /**
     * Returns the static maximum value for the given type. If the type isn't a number or
     * doesn't have a maximum value, null is returned.
     */
    public static Object max(ColumnInfo type) {
        return switch (type.plainTypeCode()) {
            case TYPE_UBYTE -> (byte) ~0;
            case TYPE_USHORT -> (short) ~0;
            case TYPE_UINT -> ~0;
            case TYPE_ULONG -> ~0L;
            case TYPE_BYTE -> java.lang.Byte.MAX_VALUE;
            case TYPE_SHORT -> java.lang.Short.MAX_VALUE;
            case TYPE_INT -> java.lang.Integer.MAX_VALUE;
            case TYPE_LONG -> java.lang.Long.MAX_VALUE;
            case TYPE_FLOAT -> java.lang.Float.MAX_VALUE;
            case TYPE_DOUBLE -> java.lang.Double.MAX_VALUE;
            default -> null;
        };
    }

    /**
     * Generates code which sets a variable to zero or false. If the type isn't supported, the
     * variable is set to null, and false is returned.
     */
    public static boolean zero(Variable v) {
        Class<?> unboxed = v.unboxedType();
        if (unboxed != null) {
            v.set(unboxed == boolean.class ? false : 0);
            return true;
        }
        Class<?> clazz = v.classType();
        if (clazz == java.math.BigInteger.class || clazz == java.math.BigDecimal.class) {
            v.set(v.field("ZERO"));
            return true;
        }
        v.clear();
        return false;
    }

    /**
     * Returns true if the given class can be set to zero or false.
     */
    public static boolean canZero(Class<?> clazz) {
        return Variable.unboxedType(clazz) != null ||
            clazz == java.math.BigInteger.class || clazz == java.math.BigDecimal.class;
    }

    public static final class Bool {
        static Variable eval(int op, Variable left, Variable right) {
            return switch (op) {
                case T_AND -> left.and(right);
                case T_OR -> left.or(right);
                case T_XOR -> left.xor(right);
                default -> null;
            };
        }
    }

    public static final class UByte {
        static Variable eval(int op, Variable left, Variable right) {
            final String method;

            switch (op) {
            case T_PLUS:
                method = "addExact";
                break;
            case T_MINUS:
                method = "subtractExact";
                break;
            case T_STAR:
                method = "multiplyExact";
                break;
            case T_DIV:
                method = "divideExact";
                break;
            case T_REM:
                method = "remainderExact";
                break;
            case T_AND:
                return left.and(right);
            case T_OR:
                return left.or(right);
            case T_XOR:
                return left.xor(right);

            case OP_MIN:
                method = "min";
                break;
            case OP_MAX:
                method = "max";
                break;

            default:
                return null;
            }

            var uleft = left.cast(int.class).and(0xff);
            var uright = right.cast(int.class).and(0xff);

            return left.methodMaker().var(UByte.class).invoke(method, uleft, uright);
        }

        public static byte addExact(int x, int y) {
            return convert(x + y);
        }

        public static byte subtractExact(int x, int y) {
            return convert(x - y);
        }

        public static byte multiplyExact(int x, int y) {
            return convert(x * y);
        }

        public static byte divideExact(int x, int y) {
            return convert(x / y);
        }

        public static byte remainderExact(int x, int y) {
            return (byte) (x % y);
        }

        public static byte min(int x, int y) {
            return (byte) Math.min(x, y);
        }

        public static byte max(int x, int y) {
            return (byte) Math.max(x, y);
        }

        private static byte convert(int r) {
            if ((r & ~0xff) != 0) {
                throw new ArithmeticException("integer overflow");
            }
            return (byte) r;
        }
    }

    public static final class UShort {
        static Variable eval(int op, Variable left, Variable right) {
            final String method;

            switch (op) {
            case T_PLUS:
                method = "addExact";
                break;
            case T_MINUS:
                method = "subtractExact";
                break;
            case T_STAR:
                method = "multiplyExact";
                break;
            case T_DIV:
                method = "divideExact";
                break;
            case T_REM:
                method = "remainderExact";
                break;
            case T_AND:
                return left.and(right);
            case T_OR:
                return left.or(right);
            case T_XOR:
                return left.xor(right);

            case OP_MIN:
                method = "min";
                break;
            case OP_MAX:
                method = "max";
                break;

            default:
                return null;
            }

            var uleft = left.cast(int.class).and(0xffff);
            var uright = right.cast(int.class).and(0xffff);

            return left.methodMaker().var(UShort.class).invoke(method, uleft, uright);
        }

        public static short addExact(int x, int y) {
            return convert(x + y);
        }

        public static short subtractExact(int x, int y) {
            return convert(x - y);
        }

        public static short multiplyExact(int x, int y) {
            return convert(x * y);
        }

        public static short divideExact(int x, int y) {
            return convert(x / y);
        }

        public static short remainderExact(int x, int y) {
            return (short) (x % y);
        }

        public static short min(int x, int y) {
            return (short) Math.min(x, y);
        }

        public static short max(int x, int y) {
            return (short) Math.max(x, y);
        }

        private static short convert(int r) {
            if ((r & ~0xffff) != 0) {
                throw new ArithmeticException("integer overflow");
            }
            return (short) r;
        }
    }

    public static final class UInteger {
        static Variable eval(int op, Variable left, Variable right) {
            final String method;

            switch (op) {
            case T_PLUS:
                method = "addExact";
                break;
            case T_MINUS:
                method = "subtractExact";
                break;
            case T_STAR:
                method = "multiplyExact";
                break;
            case T_DIV:
                method = "divideExact";
                break;
            case T_REM:
                method = "remainderExact";
                break;
            case T_AND:
                return left.and(right);
            case T_OR:
                return left.or(right);
            case T_XOR:
                return left.xor(right);

            case OP_MIN:
                method = "min";
                break;
            case OP_MAX:
                method = "max";
                break;

            default:
                return null;
            }

            var uleft = left.invoke("toUnsignedLong", left);
            var uright = right.invoke("toUnsignedLong", right);

            return left.methodMaker().var(UInteger.class).invoke(method, uleft, uright);
        }

        public static int addExact(long x, long y) {
            return convert(x + y);
        }

        public static int subtractExact(long x, long y) {
            return convert(x - y);
        }

        public static int multiplyExact(long x, long y) {
            return convert(x * y);
        }

        public static int divideExact(long x, long y) {
            return convert(x / y);
        }

        public static int remainderExact(long x, long y) {
            return (int) (x % y);
        }

        public static int min(long x, long y) {
            return (int) Math.min(x, y);
        }

        public static int max(long x, long y) {
            return (int) Math.max(x, y);
        }

        private static int convert(long r) {
            if ((r & ~0xffff_ffffL) != 0) {
                throw new ArithmeticException("integer overflow");
            }
            return (int) r;
        }
    }

    public static final class ULong {
        private static final BigInteger OVERFLOW;

        static {
            OVERFLOW = BigInteger.valueOf(1L << 62).multiply(BigInteger.valueOf(4));
        }

        static Variable eval(int op, Variable left, Variable right) {
            final String method;

            switch (op) {
            case T_PLUS:
                method = "addExact";
                break;
            case T_MINUS:
                method = "subtractExact";
                break;
            case T_STAR:
                method = "multiplyExact";
                break;
            case T_DIV:
                method = "divideExact";
                break;
            case T_REM:
                method = "remainderExact";
                break;
            case T_AND:
                return left.and(right);
            case T_OR:
                return left.or(right);
            case T_XOR:
                return left.xor(right);

            case OP_MIN:
                method = "min";
                break;
            case OP_MAX:
                method = "max";
                break;

            default:
                return null;
            }

            return left.methodMaker().var(ULong.class).invoke(method, left, right);
        }

        public static long addExact(long x, long y) {
            long r = x + y;
            if (Long.compareUnsigned(r, x) < 0) {
                throw new ArithmeticException("integer overflow");
            }
            return r;
        }

        public static long subtractExact(long x, long y) {
            long r = x - y;
            if (Long.compareUnsigned(r, x) > 0) {
                throw new ArithmeticException("integer overflow");
            }
            return r;
        }

        public static long multiplyExact(long x, long y) {
            BigInteger bx = ConvertUtils.unsignedLongToBigIntegerExact(x);
            BigInteger by = ConvertUtils.unsignedLongToBigIntegerExact(y);
            return convert(bx.multiply(by));
        }

        public static long divideExact(long x, long y) {
            BigInteger bx = ConvertUtils.unsignedLongToBigIntegerExact(x);
            BigInteger by = ConvertUtils.unsignedLongToBigIntegerExact(y);
            return convert(bx.divide(by));
        }

        public static long remainderExact(long x, long y) {
            BigInteger bx = ConvertUtils.unsignedLongToBigIntegerExact(x);
            BigInteger by = ConvertUtils.unsignedLongToBigIntegerExact(y);
            return convert(bx.remainder(by));
        }

        public static long min(long x, long y) {
            return Long.compareUnsigned(x, y) <= 0 ? x : y;
        }

        public static long max(long x, long y) {
            return Long.compareUnsigned(x, y) >= 0 ? x : y;
        }

        private static long convert(BigInteger r) {
            if (r.compareTo(OVERFLOW) >= 0) {
                throw new ArithmeticException("integer overflow");
            }
            return r.longValue();
        }
    }

    public static final class Byte {
        static Variable eval(int op, Variable left, Variable right) {
            final String method;

            switch (op) {
            case T_PLUS:
                method = "addExact";
                break;
            case T_MINUS:
                method = "subtractExact";
                break;
            case T_STAR:
                method = "multiplyExact";
                break;
            case T_DIV:
                method = "divideExact";
                break;
            case T_REM:
                return left.rem(right);
            case T_AND:
                return left.and(right);
            case T_OR:
                return left.or(right);
            case T_XOR:
                return left.xor(right);

            case OP_MIN:
                method = "min";
                break;
            case OP_MAX:
                method = "max";
                break;

            default:
                return null;
            }

            return left.methodMaker().var(Byte.class).invoke(method, left, right);
        }

        public static byte addExact(byte x, byte y) {
            byte r = (byte) (x + y);
            if (((x ^ r) & (y ^ r)) < 0) {
                throw new ArithmeticException("integer overflow");
            }
            return r;
        }

        public static byte subtractExact(byte x, byte y) {
            byte r = (byte) (x - y);
            if (((x ^ y) & (x ^ r)) < 0) {
                throw new ArithmeticException("integer overflow");
            }
            return r;
        }

        public static byte multiplyExact(byte x, byte y) {
            int r = x * y;
            if ((byte) r != r) {
                throw new ArithmeticException("integer overflow");
            }
            return (byte) r;
        }

        public static byte divideExact(byte x, byte y) {
            byte q = (byte) (x / y);
            if ((x & y & q) >= 0) {
                return q;
            }
            throw new ArithmeticException("integer overflow");
        }

        public static byte min(byte x, byte y) {
            return (byte) Math.min(x, y);
        }

        public static byte max(byte x, byte y) {
            return (byte) Math.max(x, y);
        }
    }

    public static final class Short {
        static Variable eval(int op, Variable left, Variable right) {
            final String method;

            switch (op) {
            case T_PLUS:
                method = "addExact";
                break;
            case T_MINUS:
                method = "subtractExact";
                break;
            case T_STAR:
                method = "multiplyExact";
                break;
            case T_DIV:
                method = "divideExact";
                break;
            case T_REM:
                return left.rem(right);
            case T_AND:
                return left.and(right);
            case T_OR:
                return left.or(right);
            case T_XOR:
                return left.xor(right);

            case OP_MIN:
                method = "min";
                break;
            case OP_MAX:
                method = "max";
                break;

            default:
                return null;
            }

            return left.methodMaker().var(Short.class).invoke(method, left, right);
        }

        public static short addExact(short x, short y) {
            short r = (short) (x + y);
            if (((x ^ r) & (y ^ r)) < 0) {
                throw new ArithmeticException("integer overflow");
            }
            return r;
        }

        public static short subtractExact(short x, short y) {
            short r = (short) (x - y);
            if (((x ^ y) & (x ^ r)) < 0) {
                throw new ArithmeticException("integer overflow");
            }
            return r;
        }

        public static short multiplyExact(short x, short y) {
            int r = x * y;
            if ((short) r != r) {
                throw new ArithmeticException("integer overflow");
            }
            return (short) r;
        }

        public static short divideExact(short x, short y) {
            short q = (short) (x / y);
            if ((x & y & q) >= 0) {
                return q;
            }
            throw new ArithmeticException("integer overflow");
        }

        public static short min(short x, short y) {
            return (short) Math.min(x, y);
        }

        public static short max(short x, short y) {
            return (short) Math.max(x, y);
        }
    }

    public static final class Integer {
        static Variable eval(int op, Variable left, Variable right) {
            final String method;

            switch (op) {
            case T_PLUS:
                method = "addExact";
                break;
            case T_MINUS:
                method = "subtractExact";
                break;
            case T_STAR:
                method = "multiplyExact";
                break;
            case T_DIV:
                method = "divideExact";
                break;
            case T_REM:
                return left.rem(right);
            case T_AND:
                return left.and(right);
            case T_OR:
                return left.or(right);
            case T_XOR:
                return left.xor(right);

            case OP_MIN:
                method = "min";
                break;
            case OP_MAX:
                method = "max";
                break;

            default:
                return null;
            }

            return left.methodMaker().var(Math.class).invoke(method, left, right);
        }
    }

    public static final class Float {
        static Variable eval(int op, Variable left, Variable right) {
            final String method;

            switch (op) {
            case T_PLUS:
                return left.add(right);
            case T_MINUS:
                return left.sub(right);
            case T_STAR:
                return left.mul(right);
            case T_DIV:
                return left.div(right);
            case T_REM:
                return left.rem(right);

            case OP_MIN:
                method = "min";
                break;
            case OP_MAX:
                method = "max";
                break;

            default:
                return null;
            }

            return left.methodMaker().var(Math.class).invoke(method, left, right);
        }
    }

    public static final class Big {
        static Variable eval(int op, Variable left, Variable right) {
            final String method;

            switch (op) {
            case T_PLUS:
                method = "add";
                break;
            case T_MINUS:
                method = "subtract";
                break;
            case T_STAR:
                method = "multiply";
                break;
            case T_DIV:
                method = "divide";
                break;
            case T_REM:
                method = "remainder";
                break;
            case T_AND:
                method = "and";
                break;
            case T_OR:
                method = "or";
                break;
            case T_XOR:
                method = "xor";
                break;

            case OP_MIN:
                method = "min";
                break;
            case OP_MAX:
                method = "max";
                break;

             default:
                return null;
            }

            return left.invoke(method, right);
        }
    }

    public static final class BigDecimal {
        static Variable eval(int op, Variable left, Variable right) {
            final String method;

            switch (op) {
            case T_AND: case T_OR: case T_XOR: return null;

            case T_DIV:
                method = "divide";
                break;
            case T_REM:
                method = "remainder";
                break;
                
            default: return Big.eval(op, left, right);
            }

            var contextVar = left.methodMaker().var(MathContext.class).field("DECIMAL64");
            return left.invoke(method, right, contextVar);
        }
    }
}
