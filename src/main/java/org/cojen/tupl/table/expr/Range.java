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

/**
 * Defines a generic value range.
 *
 * @author Brian S. O'Neill
 * @see RangeExpr
 */
public interface Range {
    boolean isOpenStart();

    boolean isOpenEnd();

    /**
     * Returns true if the range includes zero, assuming that the start and end are both
     * inclusive.
     */
    boolean includesZero();

    /**
     * Returns the range start as an integer, clamped to MIN_VALUE and MAX_VALUE.
     */
    int start_int();

    /**
     * Returns the range start as a long, clamped to MIN_VALUE and MAX_VALUE.
     */
    long start_long();

    /**
     * Returns the range start as a double, using +/- infinity to represent an open range.
     */
    double start_double();

    /**
     * Returns the range end as an integer, clamped to MIN_VALUE and MAX_VALUE.
     */
    int end_int();

    /**
     * Returns the range end as a long, clamped to MIN_VALUE and MAX_VALUE.
     */
    long end_long();

    /**
     * Returns the range end as a double, using +/- infinity to represent an open range.
     */
    double end_double();

    private static int clamp_to_int(long v) {
        if (v < Integer.MIN_VALUE) {
            return Integer.MIN_VALUE;
        } else if (v > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        } else {
            return (int) v;
        }
    }

    private static long clamp_to_long(Number v) {
        if (v instanceof BigInteger b) {
            if (b.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) <= 0) {
                return Long.MIN_VALUE;
            }
            if (b.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) >= 0) {
                return Long.MAX_VALUE;
            }
        }

        return v.longValue();
    }

    private static boolean isInt(Number v) {
        return v instanceof Integer || v instanceof Short || v instanceof Byte;
    }

    private static boolean isLong(Number v) {
        return v instanceof Long || v instanceof BigInteger;
    }

    /**
     * Returns true if the given number is a BigInteger and is less than Long.MIN_VALUE.
     */
    private static boolean isOpenStart(Number v) {
        return v instanceof BigInteger b && b.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0;
    }

    /**
     * Returns true if the given number is a BigInteger and is greater than Long.MAX_VALUE.
     */
    private static boolean isOpenEnd(Number v) {
        return v instanceof BigInteger b && b.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0;
    }

    /**
     * Returns a fully open range instance.
     */
    public static Range open() {
        return Open.THE;
    }

    static final class Open implements Range {
        static final Open THE = new Open();

        @Override
        public boolean isOpenStart() {
            return true;
        }

        @Override
        public boolean isOpenEnd() {
            return true;
        }

        @Override
        public boolean includesZero() {
            return true;
        }

        @Override
        public int start_int() {
            return Integer.MIN_VALUE;
        }

        @Override
        public long start_long() {
            return Long.MIN_VALUE;
        }

        @Override
        public double start_double() {
            return Double.NEGATIVE_INFINITY;
        }

        @Override
        public int end_int() {
            return Integer.MAX_VALUE;
        }

        @Override
        public long end_long() {
            return Long.MAX_VALUE;
        }

        @Override
        public double end_double() {
            return Double.POSITIVE_INFINITY;
        }

        @Override
        public String toString() {
            return "..";
        }
    }

    /**
     * Returns a range where the start and end are both zero.
     */
    public static Range zero() {
        return Zero.THE;
    }

    static final class Zero implements Range {
        static final Zero THE = new Zero();

        @Override
        public boolean isOpenStart() {
            return false;
        }

        @Override
        public boolean isOpenEnd() {
            return false;
        }

        @Override
        public boolean includesZero() {
            return true;
        }

        @Override
        public int start_int() {
            return 0;
        }

        @Override
        public long start_long() {
            return 0;
        }

        @Override
        public double start_double() {
            return 0;
        }

        @Override
        public int end_int() {
            return 0;
        }

        @Override
        public long end_long() {
            return 0;
        }

        @Override
        public double end_double() {
            return 0;
        }

        @Override
        public String toString() {
            return "0..0";
        }
    }

    /**
     * @param start can be null for open range
     * @param end can be null for open range
     */
    public static Range make(Number start, Number end) {
        if (start == null || isOpenStart(start)) {
            if (end == null) {
                return open();
            } else if (isInt(end)) {
                return makeOpenStart(end.intValue());
            } else if (isLong(end)) {
                return makeOpenStart(clamp_to_long(end));
            } else {
                return makeOpenStart(end.doubleValue());
            }
        } else if (end == null || isOpenEnd(end)) {
            if (isInt(start)) {
                return makeOpenEnd(start.intValue());
            } else if (isLong(start)) {
                return makeOpenEnd(clamp_to_long(start));
            } else {
                return makeOpenEnd(start.doubleValue());
            }
        } else {
            boolean s_int = isInt(start);
            if (s_int && isInt(end)) {
                return make(start.intValue(), end.intValue());
            } else if ((s_int || isLong(start)) && (isInt(end) || isLong(end))) {
                return make(clamp_to_long(start), clamp_to_long(end));
            } else {
                return make(start.doubleValue(), end.doubleValue());
            }
        }
    }

    public static Range make(int start, int end) {
        if (start == 0 && end == 0) {
            return zero();
        }

        return new Range() {
            @Override
            public boolean isOpenStart() {
                return false;
            }

            @Override
            public boolean isOpenEnd() {
                return false;
            }

            @Override
            public boolean includesZero() {
                return start <= 0 && 0 <= end;
            }

            @Override
            public int start_int() {
                return start;
            }

            @Override
            public long start_long() {
                return start;
            }

            @Override
            public double start_double() {
                return start;
            }

            @Override
            public int end_int() {
                return end;
            }

            @Override
            public long end_long() {
                return end;
            }

            @Override
            public double end_double() {
                return end;
            }

            @Override
            public String toString() {
                return start + ".." + end;
            }
        };
    }

    public static Range makeOpenStart(int end) {
        return new Range() {
            @Override
            public boolean isOpenStart() {
                return true;
            }

            @Override
            public boolean isOpenEnd() {
                return false;
            }

            @Override
            public boolean includesZero() {
                return 0 <= end;
            }

            @Override
            public int start_int() {
                return Integer.MIN_VALUE;
            }

            @Override
            public long start_long() {
                return Long.MIN_VALUE;
            }

            @Override
            public double start_double() {
                return Double.NEGATIVE_INFINITY;
            }

            @Override
            public int end_int() {
                return end;
            }

            @Override
            public long end_long() {
                return end;
            }

            @Override
            public double end_double() {
                return end;
            }

            @Override
            public String toString() {
                return ".." + end;
            }
        };
    }

    public static Range makeOpenEnd(int start) {
        return new Range() {
            @Override
            public boolean isOpenStart() {
                return false;
            }

            @Override
            public boolean isOpenEnd() {
                return true;
            }

            @Override
            public boolean includesZero() {
                return start <= 0;
            }

            @Override
            public int start_int() {
                return start;
            }

            @Override
            public long start_long() {
                return start;
            }

            @Override
            public double start_double() {
                return start;
            }

            @Override
            public int end_int() {
                return Integer.MAX_VALUE;
            }

            @Override
            public long end_long() {
                return Long.MAX_VALUE;
            }

            @Override
            public double end_double() {
                return Double.POSITIVE_INFINITY;
            }

            @Override
            public String toString() {
                return start + "..";
            }
        };
    }

    public static Range make(long start, long end) {
        if (start == 0 && end == 0) {
            return zero();
        }

        return new Range() {
            @Override
            public boolean isOpenStart() {
                return false;
            }

            @Override
            public boolean isOpenEnd() {
                return false;
            }

            @Override
            public boolean includesZero() {
                return start <= 0 && 0 <= end;
            }

            @Override
            public int start_int() {
                return clamp_to_int(start);
            }

            @Override
            public long start_long() {
                return start;
            }

            @Override
            public double start_double() {
                return start;
            }

            @Override
            public int end_int() {
                return clamp_to_int(end);
            }

            @Override
            public long end_long() {
                return end;
            }

            @Override
            public double end_double() {
                return end;
            }

            @Override
            public String toString() {
                return start + ".." + end;
            }
        };
    }

    public static Range makeOpenStart(long end) {
        return new Range() {
            @Override
            public boolean isOpenStart() {
                return true;
            }

            @Override
            public boolean isOpenEnd() {
                return false;
            }

            @Override
            public boolean includesZero() {
                return 0 <= end;
            }

            @Override
            public int start_int() {
                return Integer.MIN_VALUE;
            }

            @Override
            public long start_long() {
                return Long.MIN_VALUE;
            }

            @Override
            public double start_double() {
                return Double.NEGATIVE_INFINITY;
            }

            @Override
            public int end_int() {
                return clamp_to_int(end);
            }

            @Override
            public long end_long() {
                return end;
            }

            @Override
            public double end_double() {
                return end;
            }

            @Override
            public String toString() {
                return ".." + end;
            }
        };
    }

    public static Range makeOpenEnd(long start) {
        return new Range() {
            @Override
            public boolean isOpenStart() {
                return false;
            }

            @Override
            public boolean isOpenEnd() {
                return true;
            }

            @Override
            public boolean includesZero() {
                return start <= 0;
            }

            @Override
            public int start_int() {
                return clamp_to_int(start);
            }

            @Override
            public long start_long() {
                return start;
            }

            @Override
            public double start_double() {
                return start;
            }

            @Override
            public int end_int() {
                return Integer.MAX_VALUE;
            }

            @Override
            public long end_long() {
                return Long.MAX_VALUE;
            }

            @Override
            public double end_double() {
                return Double.POSITIVE_INFINITY;
            }

            @Override
            public String toString() {
                return start + "..";
            }
        };
    }

    public static Range make(double start, double end) {
        if (start == Double.NEGATIVE_INFINITY) {
            return makeOpenStart(end);
        } else if (end == Double.POSITIVE_INFINITY) {
            return makeOpenEnd(end);
        } else if (start == 0 && end == 0) {
            return zero();
        }

        return new Range() {
            @Override
            public boolean isOpenStart() {
                return false;
            }

            @Override
            public boolean isOpenEnd() {
                return false;
            }

            @Override
            public boolean includesZero() {
                return start <= 0 && 0 <= end;
            }

            @Override
            public int start_int() {
                return (int) start;
            }

            @Override
            public long start_long() {
                return (long) start;
            }

            @Override
            public double start_double() {
                return start;
            }

            @Override
            public int end_int() {
                return (int) end;
            }

            @Override
            public long end_long() {
                return (long) end;
            }

            @Override
            public double end_double() {
                return end;
            }

            @Override
            public String toString() {
                return start + ".." + end;
            }
        };
    }

    public static Range makeOpenStart(double end) {
        if (end == Double.POSITIVE_INFINITY) {
            return open();
        }

        return new Range() {
            @Override
            public boolean isOpenStart() {
                return true;
            }

            @Override
            public boolean isOpenEnd() {
                return false;
            }

            @Override
            public boolean includesZero() {
                return 0 <= end;
            }

            @Override
            public int start_int() {
                return Integer.MIN_VALUE;
            }

            @Override
            public long start_long() {
                return Long.MIN_VALUE;
            }

            @Override
            public double start_double() {
                return Double.NEGATIVE_INFINITY;
            }

            @Override
            public int end_int() {
                return (int) end;
            }

            @Override
            public long end_long() {
                return (long) end;
            }

            @Override
            public double end_double() {
                return end;
            }

            @Override
            public String toString() {
                return ".." + end;
            }
        };
    }

    public static Range makeOpenEnd(double start) {
        if (start == Double.NEGATIVE_INFINITY) {
            return open();
        }

        return new Range() {
            @Override
            public boolean isOpenStart() {
                return false;
            }

            @Override
            public boolean isOpenEnd() {
                return true;
            }

            @Override
            public boolean includesZero() {
                return start <= 0;
            }

            @Override
            public int start_int() {
                return (int) start;
            }

            @Override
            public long start_long() {
                return (long) start;
            }

            @Override
            public double start_double() {
                return start;
            }

            @Override
            public int end_int() {
                return Integer.MAX_VALUE;
            }

            @Override
            public long end_long() {
                return Long.MAX_VALUE;
            }

            @Override
            public double end_double() {
                return Double.POSITIVE_INFINITY;
            }

            @Override
            public String toString() {
                return start + "..";
            }
        };
    }
}
