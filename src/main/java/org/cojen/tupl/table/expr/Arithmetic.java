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

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ConvertUtils;

import static org.cojen.tupl.table.expr.Token.*;

import java.lang.Long;

/**
 * The arithmetic operations act on two variables, of the same type, with non-null values, and
 * returns a new variable of the same type. If the operation isn't supported, a null Variable
 * instance is returned. Exact arithmetic is performed, and so an exception can be thrown at
 * runtime, depending on the type and operation.
 *
 * @author Brian S. O'Neill
 */
final class Arithmetic {
    public static final class Bool {
        static Variable eval(int op, Variable left, Variable right) {
            switch (op) {
            case T_AND:
                return left.and(right);
            case T_OR:
                return left.or(right);
            case T_XOR:
                return left.xor(right);
            }
            return null;
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
            case T_AND:
                return left.and(right);
            case T_OR:
                return left.or(right);
            case T_XOR:
                return left.xor(right);
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

        private static byte convert(int r) {
            if ((r & 0xff) != 0) {
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
            case T_AND:
                return left.and(right);
            case T_OR:
                return left.or(right);
            case T_XOR:
                return left.xor(right);
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

        private static short convert(int r) {
            if ((r & 0xffff) != 0) {
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
            case T_AND:
                return left.and(right);
            case T_OR:
                return left.or(right);
            case T_XOR:
                return left.xor(right);
            default:
                return null;
            }

            MethodMaker mm = left.methodMaker();
            var ivar = mm.var(Integer.class);

            var uleft = ivar.invoke("toUnsignedLong", left);
            var uright = ivar.invoke("toUnsignedLong", right);

            return mm.var(UInteger.class).invoke(method, uleft, uright);
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

        private static int convert(long r) {
            if ((r & 0xffff_ffffL) != 0) {
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
            case T_AND:
                return left.and(right);
            case T_OR:
                return left.or(right);
            case T_XOR:
                return left.xor(right);
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
            if (((x ^ r) & (y ^ r)) < 0) {
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
            if (((x ^ r) & (y ^ r)) < 0) {
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
            default:
                return null;
            }

            return left.methodMaker().var(Math.class).invoke(method, left, right);
        }
    }

    public static final class Float {
        static Variable eval(int op, Variable left, Variable right) {
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
            }
            return null;
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
            default:
                return null;
            }

            return left.invoke(method, right);
        }
    }

    public static final class BigDecimal {
        static Variable eval(int op, Variable left, Variable right) {
            return switch (op) {
            case T_AND, T_OR, T_XOR -> null;
                default -> Big.eval(op, left, right);
            };
        }
    }
}
