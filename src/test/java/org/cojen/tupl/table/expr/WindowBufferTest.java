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

import java.lang.reflect.Method;

import java.math.BigDecimal;
import java.math.BigInteger;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class WindowBufferTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(WindowBufferTest.class.getName());
    }

    private static final long MIN = Long.MIN_VALUE, MAX = Long.MAX_VALUE;

    private static BigInteger bi(long v) {
        return BigInteger.valueOf(v);
    }

    private static BigDecimal bd(long v) {
        return BigDecimal.valueOf(v);
    }

    private static BigDecimal bd(double v) {
        return BigDecimal.valueOf(v);
    }

    private Type mValueType;

    @Test
    public void testLong() throws Exception {
        mValueType = BasicType.make(long.class, Type.TYPE_LONG);

        String op = "frameCount";
        test(op, 0, 0, new Long[] {10L, 20L, 30L}, new Object[] {1, 1, 1});
        test(op, MIN, 0, new Long[] {10L, 20L, 30L}, new Object[] {1, 2, 3});
        test(op, 0, MAX, new Long[] {10L, 20L, 30L}, new Object[] {3, 2, 1});
        test(op, -1, 1, new Long[] {10L, 20L, 30L}, new Object[] {2, 3, 2});
        test(op, -1, -1, new Long[] {10L, 20L, 30L}, new Object[] {0, 1, 1});
        test(op, 1, 1, new Long[] {10L, 20L, 30L}, new Object[] {1, 1, 0});
        test(op, MIN, MAX, new Long[] {10L, 20L, 30L}, new Object[] {3, 3, 3});

        op = "frameSum";
        test(op, 0, 0, new Long[] {10L, 20L, 30L}, new Object[] {10L, 20L, 30L});
        test(op, MIN, 0, new Long[] {10L, 20L, 30L}, new Object[] {10L, 30L, 60L});
        test(op, 0, MAX, new Long[] {10L, 20L, 30L}, new Object[] {60L, 50L, 30L});
        test(op, -1, 1, new Long[] {10L, 20L, 30L}, new Object[] {30L, 60L, 50L});
        test(op, -1, -1, new Long[] {10L, 20L, 30L}, new Object[] {0L, 10L, 20L});
        test(op, 1, 1, new Long[] {10L, 20L, 30L}, new Object[] {20L, 30L, 0L});
        test(op, MIN, MAX, new Long[] {10L, 20L, 30L}, new Object[] {60L, 60L, 60L});

        op = "frameAverage";
        test(op, 0, 0, new Long[] {10L, 20L, 30L}, new Object[] {10L, 20L, 30L});
        test(op, MIN, 0, new Long[] {10L, 20L, 30L}, new Object[] {10L, 15L, 20L});
        test(op, 0, MAX, new Long[] {10L, 20L, 30L}, new Object[] {20L, 25L, 30L});
        test(op, -1, 1, new Long[] {10L, 20L, 30L}, new Object[] {15L, 20L, 25L});
        test(op, -1, -1, new Long[] {10L, 20L, 30L}, new Object[] {null, 10L, 20L});
        test(op, 1, 1, new Long[] {10L, 20L, 30L}, new Object[] {20L, 30L, null});
        test(op, MIN, MAX, new Long[] {10L, 20L, 30L}, new Object[] {20L, 20L, 20L});

        op = "frameAverageOrNull";
        test(op, 0, 0, new Long[] {10L, 20L, 30L}, new Object[] {10L, 20L, 30L});
        test(op, MIN, 0, new Long[] {10L, 20L, 30L}, new Object[] {10L, 15L, 20L});
        test(op, 0, MAX, new Long[] {10L, 20L, 30L}, new Object[] {20L, 25L, 30L});
        test(op, -1, 1, new Long[] {10L, 20L, 30L}, new Object[] {15L, 20L, 25L});
        test(op, -1, -1, new Long[] {10L, 20L, 30L}, new Object[] {null, 10L, 20L});
        test(op, 1, 1, new Long[] {10L, 20L, 30L}, new Object[] {20L, 30L, null});
        test(op, MIN, MAX, new Long[] {10L, 20L, 30L}, new Object[] {20L, 20L, 20L});

        op = "frameMin";
        test(op, 0, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 30L});
        test(op, MIN, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 10L});
        test(op, 0, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 30L});
        test(op, -1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});
        test(op, -1, -1, new Long[] {20L, 10L, 30L}, new Object[] {MAX, 20L, 10L});
        test(op, 1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 30L, MAX});
        test(op, MIN, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});

        op = "frameMinOrNull";
        test(op, 0, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 30L});
        test(op, MIN, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 10L});
        test(op, 0, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 30L});
        test(op, -1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});
        test(op, -1, -1, new Long[] {20L, 10L, 30L}, new Object[] {null, 20L, 10L});
        test(op, 1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 30L, null});
        test(op, MIN, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});

        op = "frameMinNL";
        test(op, 0, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 30L});
        test(op, MIN, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 10L});
        test(op, 0, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 30L});
        test(op, -1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});
        test(op, -1, -1, new Long[] {20L, 10L, 30L}, new Object[] {MAX, 20L, 10L});
        test(op, 1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 30L, MAX});
        test(op, MIN, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});

        op = "frameMinOrNullNL";
        test(op, 0, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 30L});
        test(op, MIN, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 10L});
        test(op, 0, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 30L});
        test(op, -1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});
        test(op, -1, -1, new Long[] {20L, 10L, 30L}, new Object[] {null, 20L, 10L});
        test(op, 1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 30L, null});
        test(op, MIN, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});

        op = "frameMax";
        test(op, 0, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 30L});
        test(op, MIN, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 40L});
        test(op, 0, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 30L});
        test(op, -1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});
        test(op, -1, -1, new Long[] {20L, 40L, 30L}, new Object[] {MIN, 20L, 40L});
        test(op, 1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 30L, MIN});
        test(op, MIN, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});

        op = "frameMaxOrNull";
        test(op, 0, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 30L});
        test(op, MIN, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 40L});
        test(op, 0, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 30L});
        test(op, -1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});
        test(op, -1, -1, new Long[] {20L, 40L, 30L}, new Object[] {null, 20L, 40L});
        test(op, 1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 30L, null});
        test(op, MIN, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});

        op = "frameMaxNL";
        test(op, 0, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 30L});
        test(op, MIN, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 40L});
        test(op, 0, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 30L});
        test(op, -1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});
        test(op, -1, -1, new Long[] {20L, 40L, 30L}, new Object[] {MIN, 20L, 40L});
        test(op, 1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 30L, MIN});
        test(op, MIN, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});

        op = "frameMaxOrNullNL";
        test(op, 0, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 30L});
        test(op, MIN, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 40L});
        test(op, 0, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 30L});
        test(op, -1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});
        test(op, -1, -1, new Long[] {20L, 40L, 30L}, new Object[] {null, 20L, 40L});
        test(op, 1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 30L, null});
        test(op, MIN, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});
    }

    @Test
    public void testLongObj() throws Exception {
        mValueType = BasicType.make(long.class, Type.TYPE_LONG).nullable();

        String op = "frameCount";
        test(op, 0, 0, new Long[] {10L, 20L, 30L}, new Object[] {1, 1, 1});
        test(op, MIN, 0, new Long[] {10L, 20L, 30L}, new Object[] {1, 2, 3});
        test(op, 0, MAX, new Long[] {10L, 20L, 30L}, new Object[] {3, 2, 1});
        test(op, -1, 1, new Long[] {10L, 20L, 30L}, new Object[] {2, 3, 2});
        test(op, -1, -1, new Long[] {10L, 20L, 30L}, new Object[] {0, 1, 1});
        test(op, 1, 1, new Long[] {10L, 20L, 30L}, new Object[] {1, 1, 0});
        test(op, MIN, MAX, new Long[] {10L, 20L, 30L}, new Object[] {3, 3, 3});

        op = "frameSum";
        test(op, 0, 0, new Long[] {10L, 20L, 30L}, new Object[] {10L, 20L, 30L});
        test(op, MIN, 0, new Long[] {10L, 20L, 30L}, new Object[] {10L, 30L, 60L});
        test(op, 0, MAX, new Long[] {10L, 20L, 30L}, new Object[] {60L, 50L, 30L});
        test(op, -1, 1, new Long[] {10L, 20L, 30L}, new Object[] {30L, 60L, 50L});
        test(op, -1, -1, new Long[] {10L, 20L, 30L}, new Object[] {0L, 10L, 20L});
        test(op, 1, 1, new Long[] {10L, 20L, 30L}, new Object[] {20L, 30L, 0L});
        test(op, MIN, MAX, new Long[] {10L, 20L, 30L}, new Object[] {60L, 60L, 60L});

        op = "frameAverage";
        test(op, 0, 0, new Long[] {10L, 20L, 30L}, new Object[] {10L, 20L, 30L});
        test(op, MIN, 0, new Long[] {10L, 20L, 30L}, new Object[] {10L, 15L, 20L});
        test(op, 0, MAX, new Long[] {10L, 20L, 30L}, new Object[] {20L, 25L, 30L});
        test(op, -1, 1, new Long[] {10L, 20L, 30L}, new Object[] {15L, 20L, 25L});
        test(op, -1, -1, new Long[] {10L, 20L, 30L}, new Object[] {null, 10L, 20L});
        test(op, 1, 1, new Long[] {10L, 20L, 30L}, new Object[] {20L, 30L, null});
        test(op, MIN, MAX, new Long[] {10L, 20L, 30L}, new Object[] {20L, 20L, 20L});

        op = "frameAverageOrNull";
        test(op, 0, 0, new Long[] {10L, 20L, 30L}, new Object[] {10L, 20L, 30L});
        test(op, MIN, 0, new Long[] {10L, 20L, 30L}, new Object[] {10L, 15L, 20L});
        test(op, 0, MAX, new Long[] {10L, 20L, 30L}, new Object[] {20L, 25L, 30L});
        test(op, -1, 1, new Long[] {10L, 20L, 30L}, new Object[] {15L, 20L, 25L});
        test(op, -1, -1, new Long[] {10L, 20L, 30L}, new Object[] {null, 10L, 20L});
        test(op, 1, 1, new Long[] {10L, 20L, 30L}, new Object[] {20L, 30L, null});
        test(op, MIN, MAX, new Long[] {10L, 20L, 30L}, new Object[] {20L, 20L, 20L});

        op = "frameMin";
        test(op, 0, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 30L});
        test(op, MIN, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 10L});
        test(op, 0, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 30L});
        test(op, -1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});
        test(op, -1, -1, new Long[] {20L, 10L, 30L}, new Object[] {null, 20L, 10L});
        test(op, 1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 30L, null});
        test(op, MIN, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});

        op = "frameMinOrNull";
        test(op, 0, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 30L});
        test(op, MIN, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 10L});
        test(op, 0, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 30L});
        test(op, -1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});
        test(op, -1, -1, new Long[] {20L, 10L, 30L}, new Object[] {null, 20L, 10L});
        test(op, 1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 30L, null});
        test(op, MIN, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});

        op = "frameMinNL";
        test(op, 0, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 30L});
        test(op, MIN, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 10L});
        test(op, 0, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 30L});
        test(op, -1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});
        test(op, -1, -1, new Long[] {20L, 10L, 30L}, new Object[] {null, 20L, 10L});
        test(op, 1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 30L, null});
        test(op, MIN, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});

        op = "frameMinOrNullNL";
        test(op, 0, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 30L});
        test(op, MIN, 0, new Long[] {20L, 10L, 30L}, new Object[] {20L, 10L, 10L});
        test(op, 0, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 30L});
        test(op, -1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});
        test(op, -1, -1, new Long[] {20L, 10L, 30L}, new Object[] {null, 20L, 10L});
        test(op, 1, 1, new Long[] {20L, 10L, 30L}, new Object[] {10L, 30L, null});
        test(op, MIN, MAX, new Long[] {20L, 10L, 30L}, new Object[] {10L, 10L, 10L});

        op = "frameMax";
        test(op, 0, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 30L});
        test(op, MIN, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 40L});
        test(op, 0, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 30L});
        test(op, -1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});
        test(op, -1, -1, new Long[] {20L, 40L, 30L}, new Object[] {null, 20L, 40L});
        test(op, 1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 30L, null});
        test(op, MIN, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});

        op = "frameMaxOrNull";
        test(op, 0, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 30L});
        test(op, MIN, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 40L});
        test(op, 0, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 30L});
        test(op, -1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});
        test(op, -1, -1, new Long[] {20L, 40L, 30L}, new Object[] {null, 20L, 40L});
        test(op, 1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 30L, null});
        test(op, MIN, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});

        op = "frameMaxNL";
        test(op, 0, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 30L});
        test(op, MIN, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 40L});
        test(op, 0, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 30L});
        test(op, -1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});
        test(op, -1, -1, new Long[] {20L, 40L, 30L}, new Object[] {null, 20L, 40L});
        test(op, 1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 30L, null});
        test(op, MIN, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});

        op = "frameMaxOrNullNL";
        test(op, 0, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 30L});
        test(op, MIN, 0, new Long[] {20L, 40L, 30L}, new Object[] {20L, 40L, 40L});
        test(op, 0, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 30L});
        test(op, -1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});
        test(op, -1, -1, new Long[] {20L, 40L, 30L}, new Object[] {null, 20L, 40L});
        test(op, 1, 1, new Long[] {20L, 40L, 30L}, new Object[] {40L, 30L, null});
        test(op, MIN, MAX, new Long[] {20L, 40L, 30L}, new Object[] {40L, 40L, 40L});
    }

    @Test
    public void testDouble() throws Exception {
        mValueType = BasicType.make(double.class, Type.TYPE_DOUBLE);

        String op = "frameCount";
        test(op, 0, 0, new Double[] {10d, 20d, 30d}, new Object[] {1, 1, 1});
        test(op, MIN, 0, new Double[] {10d, 20d, 30d}, new Object[] {1, 2, 3});
        test(op, 0, MAX, new Double[] {10d, 20d, 30d}, new Object[] {3, 2, 1});
        test(op, -1, 1, new Double[] {10d, 20d, 30d}, new Object[] {2, 3, 2});
        test(op, -1, -1, new Double[] {10d, 20d, 30d}, new Object[] {0, 1, 1});
        test(op, 1, 1, new Double[] {10d, 20d, 30d}, new Object[] {1, 1, 0});
        test(op, MIN, MAX, new Double[] {10d, 20d, 30d}, new Object[] {3, 3, 3});

        op = "frameSum";
        test(op, 0, 0, new Double[] {10d, 20d, 30d}, new Object[] {10d, 20d, 30d});
        test(op, MIN, 0, new Double[] {10d, 20d, 30d}, new Object[] {10d, 30d, 60d});
        test(op, 0, MAX, new Double[] {10d, 20d, 30d}, new Object[] {60d, 50d, 30d});
        test(op, -1, 1, new Double[] {10d, 20d, 30d}, new Object[] {30d, 60d, 50d});
        test(op, -1, -1, new Double[] {10d, 20d, 30d}, new Object[] {0d, 10d, 20d});
        test(op, 1, 1, new Double[] {10d, 20d, 30d}, new Object[] {20d, 30d, 0d});
        test(op, MIN, MAX, new Double[] {10d, 20d, 30d}, new Object[] {60d, 60d, 60d});

        op = "frameAverage";
        test(op, 0, 0, new Double[] {10d, 20d, 30d}, new Object[] {10d, 20d, 30d});
        test(op, MIN, 0, new Double[] {10d, 20d, 30d}, new Object[] {10d, 15d, 20d});
        test(op, 0, MAX, new Double[] {10d, 20d, 30d}, new Object[] {20d, 25d, 30d});
        test(op, -1, 1, new Double[] {10d, 20d, 30d}, new Object[] {15d, 20d, 25d});
        test(op, -1, -1, new Double[] {10d, 20d, 30d}, new Object[] {0d/0, 10d, 20d});
        test(op, 1, 1, new Double[] {10d, 20d, 30d}, new Object[] {20d, 30d, 0d/0});
        test(op, MIN, MAX, new Double[] {10d, 20d, 30d}, new Object[] {20d, 20d, 20d});
        test(op, MIN, MAX, new Double[] {10d, 11d}, new Object[] {10.5d, 10.5d});

        op = "frameAverageOrNull";
        test(op, 0, 0, new Double[] {10d, 20d, 30d}, new Object[] {10d, 20d, 30d});
        test(op, MIN, 0, new Double[] {10d, 20d, 30d}, new Object[] {10d, 15d, 20d});
        test(op, 0, MAX, new Double[] {10d, 20d, 30d}, new Object[] {20d, 25d, 30d});
        test(op, -1, 1, new Double[] {10d, 20d, 30d}, new Object[] {15d, 20d, 25d});
        test(op, -1, -1, new Double[] {10d, 20d, 30d}, new Object[] {null, 10d, 20d});
        test(op, 1, 1, new Double[] {10d, 20d, 30d}, new Object[] {20d, 30d, null});
        test(op, MIN, MAX, new Double[] {10d, 20d, 30d}, new Object[] {20d, 20d, 20d});
        test(op, MIN, MAX, new Double[] {10d, 11d}, new Object[] {10.5d, 10.5d});

        op = "frameMin";
        test(op, 0, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 30d});
        test(op, MIN, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 10d});
        test(op, 0, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 30d});
        test(op, -1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});
        test(op, -1, -1, new Double[] {20d, 10d, 30d}, new Object[] {Double.MAX_VALUE, 20d, 10d});
        test(op, 1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 30d, Double.MAX_VALUE});
        test(op, MIN, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});

        op = "frameMinOrNull";
        test(op, 0, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 30d});
        test(op, MIN, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 10d});
        test(op, 0, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 30d});
        test(op, -1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});
        test(op, -1, -1, new Double[] {20d, 10d, 30d}, new Object[] {null, 20d, 10d});
        test(op, 1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 30d, null});
        test(op, MIN, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});

        op = "frameMinNL";
        test(op, 0, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 30d});
        test(op, MIN, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 10d});
        test(op, 0, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 30d});
        test(op, -1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});
        test(op, -1, -1, new Double[] {20d, 10d, 30d}, new Object[] {Double.MAX_VALUE, 20d, 10d});
        test(op, 1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 30d, Double.MAX_VALUE});
        test(op, MIN, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});

        op = "frameMinOrNullNL";
        test(op, 0, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 30d});
        test(op, MIN, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 10d});
        test(op, 0, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 30d});
        test(op, -1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});
        test(op, -1, -1, new Double[] {20d, 10d, 30d}, new Object[] {null, 20d, 10d});
        test(op, 1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 30d, null});
        test(op, MIN, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});

        op = "frameMax";
        test(op, 0, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 30d});
        test(op, MIN, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 40d});
        test(op, 0, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 30d});
        test(op, -1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});
        test(op, -1, -1, new Double[] {20d, 40d, 30d}, new Object[] {Double.MIN_VALUE, 20d, 40d});
        test(op, 1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 30d, Double.MIN_VALUE});
        test(op, MIN, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});

        op = "frameMaxOrNull";
        test(op, 0, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 30d});
        test(op, MIN, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 40d});
        test(op, 0, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 30d});
        test(op, -1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});
        test(op, -1, -1, new Double[] {20d, 40d, 30d}, new Object[] {null, 20d, 40d});
        test(op, 1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 30d, null});
        test(op, MIN, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});

        op = "frameMaxNL";
        test(op, 0, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 30d});
        test(op, MIN, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 40d});
        test(op, 0, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 30d});
        test(op, -1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});
        test(op, -1, -1, new Double[] {20d, 40d, 30d}, new Object[] {Double.MIN_VALUE, 20d, 40d});
        test(op, 1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 30d, Double.MIN_VALUE});
        test(op, MIN, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});

        op = "frameMaxOrNullNL";
        test(op, 0, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 30d});
        test(op, MIN, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 40d});
        test(op, 0, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 30d});
        test(op, -1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});
        test(op, -1, -1, new Double[] {20d, 40d, 30d}, new Object[] {null, 20d, 40d});
        test(op, 1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 30d, null});
        test(op, MIN, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});
    }

    @Test
    public void testDoubleObj() throws Exception {
        mValueType = BasicType.make(double.class, Type.TYPE_DOUBLE).nullable();

        String op = "frameCount";
        test(op, 0, 0, new Double[] {10d, 20d, 30d}, new Object[] {1, 1, 1});
        test(op, MIN, 0, new Double[] {10d, 20d, 30d}, new Object[] {1, 2, 3});
        test(op, 0, MAX, new Double[] {10d, 20d, 30d}, new Object[] {3, 2, 1});
        test(op, -1, 1, new Double[] {10d, 20d, 30d}, new Object[] {2, 3, 2});
        test(op, -1, -1, new Double[] {10d, 20d, 30d}, new Object[] {0, 1, 1});
        test(op, 1, 1, new Double[] {10d, 20d, 30d}, new Object[] {1, 1, 0});
        test(op, MIN, MAX, new Double[] {10d, 20d, 30d}, new Object[] {3, 3, 3});

        op = "frameSum";
        test(op, 0, 0, new Double[] {10d, 20d, 30d}, new Object[] {10d, 20d, 30d});
        test(op, MIN, 0, new Double[] {10d, 20d, 30d}, new Object[] {10d, 30d, 60d});
        test(op, 0, MAX, new Double[] {10d, 20d, 30d}, new Object[] {60d, 50d, 30d});
        test(op, -1, 1, new Double[] {10d, 20d, 30d}, new Object[] {30d, 60d, 50d});
        test(op, -1, -1, new Double[] {10d, 20d, 30d}, new Object[] {0d, 10d, 20d});
        test(op, 1, 1, new Double[] {10d, 20d, 30d}, new Object[] {20d, 30d, 0d});
        test(op, MIN, MAX, new Double[] {10d, 20d, 30d}, new Object[] {60d, 60d, 60d});

        op = "frameAverage";
        test(op, 0, 0, new Double[] {10d, 20d, 30d}, new Object[] {10d, 20d, 30d});
        test(op, MIN, 0, new Double[] {10d, 20d, 30d}, new Object[] {10d, 15d, 20d});
        test(op, 0, MAX, new Double[] {10d, 20d, 30d}, new Object[] {20d, 25d, 30d});
        test(op, -1, 1, new Double[] {10d, 20d, 30d}, new Object[] {15d, 20d, 25d});
        test(op, -1, -1, new Double[] {10d, 20d, 30d}, new Object[] {null, 10d, 20d});
        test(op, 1, 1, new Double[] {10d, 20d, 30d}, new Object[] {20d, 30d, null});
        test(op, MIN, MAX, new Double[] {10d, 20d, 30d}, new Object[] {20d, 20d, 20d});
        test(op, MIN, MAX, new Double[] {10d, 11d}, new Object[] {10.5d, 10.5d});

        op = "frameAverageOrNull";
        test(op, 0, 0, new Double[] {10d, 20d, 30d}, new Object[] {10d, 20d, 30d});
        test(op, MIN, 0, new Double[] {10d, 20d, 30d}, new Object[] {10d, 15d, 20d});
        test(op, 0, MAX, new Double[] {10d, 20d, 30d}, new Object[] {20d, 25d, 30d});
        test(op, -1, 1, new Double[] {10d, 20d, 30d}, new Object[] {15d, 20d, 25d});
        test(op, -1, -1, new Double[] {10d, 20d, 30d}, new Object[] {null, 10d, 20d});
        test(op, 1, 1, new Double[] {10d, 20d, 30d}, new Object[] {20d, 30d, null});
        test(op, MIN, MAX, new Double[] {10d, 20d, 30d}, new Object[] {20d, 20d, 20d});
        test(op, MIN, MAX, new Double[] {10d, 11d}, new Object[] {10.5d, 10.5d});

        op = "frameMin";
        test(op, 0, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 30d});
        test(op, MIN, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 10d});
        test(op, 0, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 30d});
        test(op, -1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});
        test(op, -1, -1, new Double[] {20d, 10d, 30d}, new Object[] {null, 20d, 10d});
        test(op, 1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 30d, null});
        test(op, MIN, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});

        op = "frameMinOrNull";
        test(op, 0, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 30d});
        test(op, MIN, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 10d});
        test(op, 0, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 30d});
        test(op, -1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});
        test(op, -1, -1, new Double[] {20d, 10d, 30d}, new Object[] {null, 20d, 10d});
        test(op, 1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 30d, null});
        test(op, MIN, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});

        op = "frameMinNL";
        test(op, 0, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 30d});
        test(op, MIN, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 10d});
        test(op, 0, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 30d});
        test(op, -1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});
        test(op, -1, -1, new Double[] {20d, 10d, 30d}, new Object[] {null, 20d, 10d});
        test(op, 1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 30d, null});
        test(op, MIN, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});

        op = "frameMinOrNullNL";
        test(op, 0, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 30d});
        test(op, MIN, 0, new Double[] {20d, 10d, 30d}, new Object[] {20d, 10d, 10d});
        test(op, 0, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 30d});
        test(op, -1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});
        test(op, -1, -1, new Double[] {20d, 10d, 30d}, new Object[] {null, 20d, 10d});
        test(op, 1, 1, new Double[] {20d, 10d, 30d}, new Object[] {10d, 30d, null});
        test(op, MIN, MAX, new Double[] {20d, 10d, 30d}, new Object[] {10d, 10d, 10d});

        op = "frameMax";
        test(op, 0, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 30d});
        test(op, MIN, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 40d});
        test(op, 0, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 30d});
        test(op, -1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});
        test(op, -1, -1, new Double[] {20d, 40d, 30d}, new Object[] {null, 20d, 40d});
        test(op, 1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 30d, null});
        test(op, MIN, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});

        op = "frameMaxOrNull";
        test(op, 0, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 30d});
        test(op, MIN, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 40d});
        test(op, 0, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 30d});
        test(op, -1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});
        test(op, -1, -1, new Double[] {20d, 40d, 30d}, new Object[] {null, 20d, 40d});
        test(op, 1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 30d, null});
        test(op, MIN, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});

        op = "frameMaxNL";
        test(op, 0, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 30d});
        test(op, MIN, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 40d});
        test(op, 0, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 30d});
        test(op, -1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});
        test(op, -1, -1, new Double[] {20d, 40d, 30d}, new Object[] {null, 20d, 40d});
        test(op, 1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 30d, null});
        test(op, MIN, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});

        op = "frameMaxOrNullNL";
        test(op, 0, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 30d});
        test(op, MIN, 0, new Double[] {20d, 40d, 30d}, new Object[] {20d, 40d, 40d});
        test(op, 0, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 30d});
        test(op, -1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});
        test(op, -1, -1, new Double[] {20d, 40d, 30d}, new Object[] {null, 20d, 40d});
        test(op, 1, 1, new Double[] {20d, 40d, 30d}, new Object[] {40d, 30d, null});
        test(op, MIN, MAX, new Double[] {20d, 40d, 30d}, new Object[] {40d, 40d, 40d});
    }

    @Test
    public void testBigInteger() throws Exception {
        mValueType = BasicType.make(BigInteger.class, Type.TYPE_BIG_INTEGER);

        String op = "frameCount";
        test(op, 0, 0, new Object[] {bi(10), bi(20), bi(30)}, new Object[] {1, 1, 1});
        test(op, MIN, 0, new Object[] {bi(10), bi(20), bi(30)}, new Object[] {1, 2, 3});
        test(op, 0, MAX, new Object[] {bi(10), bi(20), bi(30)}, new Object[] {3, 2, 1});
        test(op, -1, 1, new Object[] {bi(10), bi(20), bi(30)}, new Object[] {2, 3, 2});
        test(op, -1, -1, new Object[] {bi(10), bi(20), bi(30)}, new Object[] {0, 1, 1});
        test(op, 1, 1, new Object[] {bi(10), bi(20), bi(30)}, new Object[] {1, 1, 0});
        test(op, MIN, MAX, new Object[] {bi(10), bi(20), bi(30)}, new Object[] {3, 3, 3});

        op = "frameSum";
        test(op, 0, 0, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(10), bi(20), bi(30)});
        test(op, MIN, 0, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(10), bi(30), bi(60)});
        test(op, 0, MAX, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(60), bi(50), bi(30)});
        test(op, -1, 1, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(30), bi(60), bi(50)});
        test(op, -1, -1, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(0), bi(10), bi(20)});
        test(op, 1, 1, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(20), bi(30), bi(0)});
        test(op, MIN, MAX, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(60), bi(60), bi(60)});

        op = "frameAverage";
        test(op, 0, 0, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(10), bi(20), bi(30)});
        test(op, MIN, 0, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(10), bi(15), bi(20)});
        test(op, 0, MAX, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(20), bi(25), bi(30)});
        test(op, -1, 1, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(15), bi(20), bi(25)});
        test(op, -1, -1, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {null, bi(10), bi(20)});
        test(op, 1, 1, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(20), bi(30), null});
        test(op, MIN, MAX, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(20), bi(20), bi(20)});

        op = "frameAverageOrNull";
        test(op, 0, 0, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(10), bi(20), bi(30)});
        test(op, MIN, 0, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(10), bi(15), bi(20)});
        test(op, 0, MAX, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(20), bi(25), bi(30)});
        test(op, -1, 1, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(15), bi(20), bi(25)});
        test(op, -1, -1, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {null, bi(10), bi(20)});
        test(op, 1, 1, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(20), bi(30), null});
        test(op, MIN, MAX, new Object[] {bi(10), bi(20), bi(30)},
             new Object[] {bi(20), bi(20), bi(20)});

        op = "frameMin";
        test(op, 0, 0, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(20), bi(10), bi(30)});
        test(op, MIN, 0, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(20), bi(10), bi(10)});
        test(op, 0, MAX, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(10), bi(30)});
        test(op, -1, 1, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(10), bi(10)});
        test(op, -1, -1, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {null, bi(20), bi(10)});
        test(op, 1, 1, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(30), null});
        test(op, MIN, MAX, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(10), bi(10)});

        op = "frameMinOrNull";
        test(op, 0, 0, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(20), bi(10), bi(30)});
        test(op, MIN, 0, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(20), bi(10), bi(10)});
        test(op, 0, MAX, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(10), bi(30)});
        test(op, -1, 1, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(10), bi(10)});
        test(op, -1, -1, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {null, bi(20), bi(10)});
        test(op, 1, 1, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(30), null});
        test(op, MIN, MAX, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(10), bi(10)});

        op = "frameMinNL";
        test(op, 0, 0, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(20), bi(10), bi(30)});
        test(op, MIN, 0, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(20), bi(10), bi(10)});
        test(op, 0, MAX, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(10), bi(30)});
        test(op, -1, 1, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(10), bi(10)});
        test(op, -1, -1, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {null, bi(20), bi(10)});
        test(op, 1, 1, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(30), null});
        test(op, MIN, MAX, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(10), bi(10)});

        op = "frameMinOrNullNL";
        test(op, 0, 0, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(20), bi(10), bi(30)});
        test(op, MIN, 0, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(20), bi(10), bi(10)});
        test(op, 0, MAX, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(10), bi(30)});
        test(op, -1, 1, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(10), bi(10)});
        test(op, -1, -1, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {null, bi(20), bi(10)});
        test(op, 1, 1, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(30), null});
        test(op, MIN, MAX, new Object[] {bi(20), bi(10), bi(30)},
             new Object[] {bi(10), bi(10), bi(10)});

        op = "frameMax";
        test(op, 0, 0, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(20), bi(40), bi(30)});
        test(op, MIN, 0, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(20), bi(40), bi(40)});
        test(op, 0, MAX, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(40), bi(30)});
        test(op, -1, 1, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(40), bi(40)});
        test(op, -1, -1, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {null, bi(20), bi(40)});
        test(op, 1, 1, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(30), null});
        test(op, MIN, MAX, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(40), bi(40)});

        op = "frameMaxOrNull";
        test(op, 0, 0, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(20), bi(40), bi(30)});
        test(op, MIN, 0, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(20), bi(40), bi(40)});
        test(op, 0, MAX, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(40), bi(30)});
        test(op, -1, 1, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(40), bi(40)});
        test(op, -1, -1, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {null, bi(20), bi(40)});
        test(op, 1, 1, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(30), null});
        test(op, MIN, MAX, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(40), bi(40)});

        op = "frameMaxNL";
        test(op, 0, 0, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(20), bi(40), bi(30)});
        test(op, MIN, 0, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(20), bi(40), bi(40)});
        test(op, 0, MAX, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(40), bi(30)});
        test(op, -1, 1, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(40), bi(40)});
        test(op, -1, -1, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {null, bi(20), bi(40)});
        test(op, 1, 1, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(30), null});
        test(op, MIN, MAX, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(40), bi(40)});

        op = "frameMaxOrNullNL";
        test(op, 0, 0, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(20), bi(40), bi(30)});
        test(op, MIN, 0, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(20), bi(40), bi(40)});
        test(op, 0, MAX, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(40), bi(30)});
        test(op, -1, 1, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(40), bi(40)});
        test(op, -1, -1, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {null, bi(20), bi(40)});
        test(op, 1, 1, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(30), null});
        test(op, MIN, MAX, new Object[] {bi(20), bi(40), bi(30)},
             new Object[] {bi(40), bi(40), bi(40)});
    }

    @Test
    public void testBigDecimal() throws Exception {
        mValueType = BasicType.make(BigDecimal.class, Type.TYPE_BIG_DECIMAL);

        String op = "frameCount";
        test(op, 0, 0, new Object[] {bd(10), bd(20), bd(30)}, new Object[] {1, 1, 1});
        test(op, MIN, 0, new Object[] {bd(10), bd(20), bd(30)}, new Object[] {1, 2, 3});
        test(op, 0, MAX, new Object[] {bd(10), bd(20), bd(30)}, new Object[] {3, 2, 1});
        test(op, -1, 1, new Object[] {bd(10), bd(20), bd(30)}, new Object[] {2, 3, 2});
        test(op, -1, -1, new Object[] {bd(10), bd(20), bd(30)}, new Object[] {0, 1, 1});
        test(op, 1, 1, new Object[] {bd(10), bd(20), bd(30)}, new Object[] {1, 1, 0});
        test(op, MIN, MAX, new Object[] {bd(10), bd(20), bd(30)}, new Object[] {3, 3, 3});

        op = "frameSum";
        test(op, 0, 0, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(10), bd(20), bd(30)});
        test(op, MIN, 0, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(10), bd(30), bd(60)});
        test(op, 0, MAX, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(60), bd(50), bd(30)});
        test(op, -1, 1, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(30), bd(60), bd(50)});
        test(op, -1, -1, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(0), bd(10), bd(20)});
        test(op, 1, 1, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(20), bd(30), bd(0)});
        test(op, MIN, MAX, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(60), bd(60), bd(60)});

        op = "frameAverage";
        test(op, 0, 0, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(10), bd(20), bd(30)});
        test(op, MIN, 0, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(10), bd(15), bd(20)});
        test(op, 0, MAX, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(20), bd(25), bd(30)});
        test(op, -1, 1, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(15), bd(20), bd(25)});
        test(op, -1, -1, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {null, bd(10), bd(20)});
        test(op, 1, 1, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(20), bd(30), null});
        test(op, MIN, MAX, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(20), bd(20), bd(20)});
        test(op, MIN, MAX, new Object[] {bd(10), bd(11)}, new Object[] {bd(10.5), bd(10.5)});

        op = "frameAverageOrNull";
        test(op, 0, 0, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(10), bd(20), bd(30)});
        test(op, MIN, 0, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(10), bd(15), bd(20)});
        test(op, 0, MAX, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(20), bd(25), bd(30)});
        test(op, -1, 1, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(15), bd(20), bd(25)});
        test(op, -1, -1, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {null, bd(10), bd(20)});
        test(op, 1, 1, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(20), bd(30), null});
        test(op, MIN, MAX, new Object[] {bd(10), bd(20), bd(30)},
             new Object[] {bd(20), bd(20), bd(20)});
        test(op, MIN, MAX, new Object[] {bd(10), bd(11)}, new Object[] {bd(10.5), bd(10.5)});

        op = "frameMin";
        test(op, 0, 0, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(20), bd(10), bd(30)});
        test(op, MIN, 0, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(20), bd(10), bd(10)});
        test(op, 0, MAX, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(10), bd(30)});
        test(op, -1, 1, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(10), bd(10)});
        test(op, -1, -1, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {null, bd(20), bd(10)});
        test(op, 1, 1, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(30), null});
        test(op, MIN, MAX, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(10), bd(10)});

        op = "frameMinOrNull";
        test(op, 0, 0, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(20), bd(10), bd(30)});
        test(op, MIN, 0, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(20), bd(10), bd(10)});
        test(op, 0, MAX, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(10), bd(30)});
        test(op, -1, 1, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(10), bd(10)});
        test(op, -1, -1, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {null, bd(20), bd(10)});
        test(op, 1, 1, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(30), null});
        test(op, MIN, MAX, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(10), bd(10)});

        op = "frameMinNL";
        test(op, 0, 0, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(20), bd(10), bd(30)});
        test(op, MIN, 0, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(20), bd(10), bd(10)});
        test(op, 0, MAX, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(10), bd(30)});
        test(op, -1, 1, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(10), bd(10)});
        test(op, -1, -1, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {null, bd(20), bd(10)});
        test(op, 1, 1, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(30), null});
        test(op, MIN, MAX, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(10), bd(10)});

        op = "frameMinOrNullNL";
        test(op, 0, 0, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(20), bd(10), bd(30)});
        test(op, MIN, 0, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(20), bd(10), bd(10)});
        test(op, 0, MAX, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(10), bd(30)});
        test(op, -1, 1, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(10), bd(10)});
        test(op, -1, -1, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {null, bd(20), bd(10)});
        test(op, 1, 1, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(30), null});
        test(op, MIN, MAX, new Object[] {bd(20), bd(10), bd(30)},
             new Object[] {bd(10), bd(10), bd(10)});

        op = "frameMax";
        test(op, 0, 0, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(20), bd(40), bd(30)});
        test(op, MIN, 0, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(20), bd(40), bd(40)});
        test(op, 0, MAX, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(40), bd(30)});
        test(op, -1, 1, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(40), bd(40)});
        test(op, -1, -1, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {null, bd(20), bd(40)});
        test(op, 1, 1, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(30), null});
        test(op, MIN, MAX, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(40), bd(40)});

        op = "frameMaxOrNull";
        test(op, 0, 0, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(20), bd(40), bd(30)});
        test(op, MIN, 0, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(20), bd(40), bd(40)});
        test(op, 0, MAX, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(40), bd(30)});
        test(op, -1, 1, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(40), bd(40)});
        test(op, -1, -1, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {null, bd(20), bd(40)});
        test(op, 1, 1, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(30), null});
        test(op, MIN, MAX, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(40), bd(40)});

        op = "frameMaxNL";
        test(op, 0, 0, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(20), bd(40), bd(30)});
        test(op, MIN, 0, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(20), bd(40), bd(40)});
        test(op, 0, MAX, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(40), bd(30)});
        test(op, -1, 1, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(40), bd(40)});
        test(op, -1, -1, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {null, bd(20), bd(40)});
        test(op, 1, 1, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(30), null});
        test(op, MIN, MAX, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(40), bd(40)});

        op = "frameMaxOrNullNL";
        test(op, 0, 0, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(20), bd(40), bd(30)});
        test(op, MIN, 0, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(20), bd(40), bd(40)});
        test(op, 0, MAX, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(40), bd(30)});
        test(op, -1, 1, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(40), bd(40)});
        test(op, -1, -1, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {null, bd(20), bd(40)});
        test(op, 1, 1, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(30), null});
        test(op, MIN, MAX, new Object[] {bd(20), bd(40), bd(30)},
             new Object[] {bd(40), bd(40), bd(40)});
    }

    private void test(String op, long frameStart, long frameEnd, Object[] values, Object[] results)
        throws Exception
    {
        assertEquals(values.length, results.length);

        Class<?> bufferClass = WindowBuffer.forType(mValueType);
        Class<?> valueClass = mValueType.clazz();

        Object buffer = bufferClass.getConstructor(int.class).newInstance(8);

        var beginMethod = bufferClass.getMethod("begin", valueClass);
        var appendMethod = bufferClass.getMethod("append", valueClass);
        var endMethod = bufferClass.getMethod("end");
        var advanceMethod = bufferClass.getMethod("advance");
        var advanceAndRemoveMethod = bufferClass.getMethod("advanceAndRemove");
        var advanceAndRemoveMethodFS = bufferClass.getMethod("advanceAndRemove", long.class);

        var opMethod = bufferClass.getMethod(op, long.class, long.class);

        int valuePos = 0;
        int resultPos = 0;

        boolean finished = false;

        while (true) {
            if (!finished) {
                if (valuePos == 0) {
                    beginMethod.invoke(buffer, values[valuePos]);
                } else {
                    appendMethod.invoke(buffer, values[valuePos]);
                }

                if (++valuePos >= values.length) {
                    finished = true;
                }
            }

            if (finished || frameEnd <= (int) endMethod.invoke(buffer)) {
                Object result = opMethod.invoke(buffer, frameStart, frameEnd);
                assertEquals(results[resultPos], result);

                if (frameStart == MIN) {
                    advanceMethod.invoke(buffer);
                } else if (frameStart >= 0) {
                    advanceAndRemoveMethod.invoke(buffer);
                } else {
                    advanceAndRemoveMethodFS.invoke(buffer, frameStart);
                }

                if (++resultPos >= results.length) {
                    break;
                }
            }
        }

        assertEquals(values.length, valuePos);
        assertEquals(results.length, resultPos);
    }

    @Test
    public void findGroup() throws Exception {
        findGroup(false);
    }

    @Test
    public void findGroupAndTrim() throws Exception {
        findGroup(true);
    }

    private void findGroup(boolean trim) throws Exception {
        mValueType = BasicType.make(double.class, Type.TYPE_DOUBLE);
        Class<?> bufferClass = WindowBuffer.forType(mValueType);
        Class<?> valueClass = mValueType.clazz();

        Object buffer = bufferClass.getConstructor(int.class).newInstance(8);

        var sizeMethod = bufferClass.getMethod("size");
        var beginMethod = bufferClass.getMethod("begin", valueClass);
        var appendMethod = bufferClass.getMethod("append", valueClass);
        var advanceMethod = bufferClass.getMethod("advance");
        var trimStartMethod = bufferClass.getMethod("trimStart", long.class);
        var findStartMethod = bufferClass.getMethod("findGroupStart", long.class);
        var findEndMethod = bufferClass.getMethod("findGroupEnd", long.class);
        var frameGetMethod = bufferClass.getMethod("frameGet", long.class);

        {
            double[] values = {
                1.5, 2.0, 2.0, 2.5, 3.0, 3.0, 3.5, 4.0, 4.0, 4.5
            };

            beginMethod.invoke(buffer, values[0]);
            for (int i=1; i<values.length; i++) {
                appendMethod.invoke(buffer, values[i]);
            }
        }

        for (int i=0; i<4; i++) {
            advanceMethod.invoke(buffer);
        }

        assertEquals(10, sizeMethod.invoke(buffer));

        assertEquals(3.0, frameGetMethod.invoke(buffer, 0L));
        assertEquals(3.0, frameGetMethod.invoke(buffer, 1L));

        {
            long[] starts = {-4, -4, -3, -1, 0, 2, 3, 5, Long.MAX_VALUE};
            double[] values = {1.5, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 4.5};

            for (int i = -4; i <= 4; i++) {
                long pos = (long) findStartMethod.invoke(buffer, (long) i);
                long start = starts[i + 4];
                assertEquals(start, pos);

                if (start == Long.MAX_VALUE) {
                    continue;
                }

                double value = (double) frameGetMethod.invoke(buffer, pos);
                assertTrue(values[i + 4] == value);

                if (trim) {
                    trimStartMethod.invoke(buffer, pos);

                    int expect = switch((int) start) {
                        case -4 -> 10;
                        case -3 -> 9;
                        case -1 -> 7;
                        default -> 6;
                    };

                    int size = (int) sizeMethod.invoke(buffer);
                    assertEquals(expect, size);
                }
            }
        }

        {
            long[] ends = {5, 5, 4, 2, 1, -1, -2, -4, Long.MIN_VALUE};
            double[] values = {4.5, 4.5, 4.0, 3.5, 3.0, 2.5, 2.0, 1.5, 1.5};

            for (int i = 4; i >= (trim ? 0 : -4); i--) {
                long pos = (long) findEndMethod.invoke(buffer, (long) i);
                long end = ends[4 - i];
                assertEquals(ends[4 - i], pos);

                if (end == Long.MIN_VALUE) {
                    continue;
                }

                double value = (double) frameGetMethod.invoke(buffer, pos);
                assertTrue(values[4 - i] == value);
            }
        }
    }

    @Test
    public void findRangeAscending() throws Exception {
        mValueType = BasicType.make(double.class, Type.TYPE_DOUBLE);
        Class<?> bufferClass = WindowBuffer.forType(mValueType);
        Class<?> valueClass = mValueType.clazz();

        Object buffer = bufferClass.getConstructor(int.class).newInstance(8);

        var beginMethod = bufferClass.getMethod("begin", valueClass);
        var appendMethod = bufferClass.getMethod("append", valueClass);
        var advanceMethod = bufferClass.getMethod("advance");
        var findStartMethod = bufferClass.getMethod("findRangeStartAsc", double.class);
        var findEndMethod = bufferClass.getMethod("findRangeEndAsc", double.class);
        var frameGetMethod = bufferClass.getMethod("frameGet", long.class);

        double[] values = {
            1.5, 2.0, 2.0, 2.5, 3.0, 3.0, 3.5, 4.0, 4.0, 4.5
        };

        beginMethod.invoke(buffer, values[0]);
        for (int i=1; i<values.length; i++) {
            appendMethod.invoke(buffer, values[i]);
        }

        for (int i=0; i<4; i++) {
            advanceMethod.invoke(buffer);
        }

        assertEquals(3.0, frameGetMethod.invoke(buffer, 0L));
        assertEquals(3.0, frameGetMethod.invoke(buffer, 1L));

        for (double delta = -2.0; delta <= 2.0; delta += 0.25) {
            double find = 3.0 + delta;
            long pos = (long) findStartMethod.invoke(buffer, delta);
            if (pos == Long.MAX_VALUE) {
                assertTrue(find > values[values.length - 1]);
            } else {
                double value = (double) frameGetMethod.invoke(buffer, pos);
                assertTrue(value >= find);
                if (pos >= -3) {
                    double prev = (double) frameGetMethod.invoke(buffer, pos - 1);
                    assertTrue(prev < find);
                }
            }
        }

        for (double delta = -2.0; delta <= 2.0; delta += 0.25) {
            double find = 3.0 + delta;
            long pos = (long) findEndMethod.invoke(buffer, delta);
            if (pos == Long.MIN_VALUE) {
                assertTrue(find < values[0]);
            } else {
                double value = (double) frameGetMethod.invoke(buffer, pos);
                assertTrue(value <= find);
                if (pos <= 4) {
                    double next = (double) frameGetMethod.invoke(buffer, pos + 1);
                    assertTrue(next > find);
                }
            }
        }

        advanceMethod.invoke(buffer);
        assertEquals(3.0, frameGetMethod.invoke(buffer, -1L));
        assertEquals(3.0, frameGetMethod.invoke(buffer, 0L));

        {
            long pos = (long) findStartMethod.invoke(buffer, 0.0);
            assertEquals(-1, pos);
        }

        {
            long pos = (long) findEndMethod.invoke(buffer, 0.0);
            assertEquals(0, pos);
        }
    }

    @Test
    public void findRangeDescending() throws Exception {
        mValueType = BasicType.make(double.class, Type.TYPE_DOUBLE);
        Class<?> bufferClass = WindowBuffer.forType(mValueType);
        Class<?> valueClass = mValueType.clazz();

        Object buffer = bufferClass.getConstructor(int.class).newInstance(8);

        var beginMethod = bufferClass.getMethod("begin", valueClass);
        var appendMethod = bufferClass.getMethod("append", valueClass);
        var advanceMethod = bufferClass.getMethod("advance");
        var findStartMethod = bufferClass.getMethod("findRangeStartDesc", double.class);
        var findEndMethod = bufferClass.getMethod("findRangeEndDesc", double.class);
        var frameGetMethod = bufferClass.getMethod("frameGet", long.class);

        double[] values = {
            4.5, 4.0, 4.0, 3.5, 3.0, 3.0, 2.5, 2.0, 2.0, 1.5
        };

        beginMethod.invoke(buffer, values[0]);
        for (int i=1; i<values.length; i++) {
            appendMethod.invoke(buffer, values[i]);
        }

        for (int i=0; i<4; i++) {
            advanceMethod.invoke(buffer);
        }

        assertEquals(3.0, frameGetMethod.invoke(buffer, 0L));
        assertEquals(3.0, frameGetMethod.invoke(buffer, 1L));

        for (double delta = -2.0; delta <= 2.0; delta += 0.25) {
            double find = 3.0 + delta;
            long pos = (long) findStartMethod.invoke(buffer, delta);
            if (pos == Long.MIN_VALUE) {
                assertTrue(find > values[0]);
            } else {
                double value = (double) frameGetMethod.invoke(buffer, pos);
                assertTrue(value >= find);
                if (pos <= 4) {
                    double next = (double) frameGetMethod.invoke(buffer, pos + 1);
                    assertTrue(next < find);
                }
            }
        }

        for (double delta = -2.0; delta <= 2.0; delta += 0.25) {
            double find = 3.0 + delta;
            long pos = (long) findEndMethod.invoke(buffer, delta);
            if (pos == Long.MAX_VALUE) {
                assertTrue(find < values[values.length - 1]);
            } else {
                double value = (double) frameGetMethod.invoke(buffer, pos);
                assertTrue(value <= find);
                if (pos >= -3) {
                    double prev = (double) frameGetMethod.invoke(buffer, pos - 1);
                    assertTrue(prev > find);
                }
            }
        }

        advanceMethod.invoke(buffer);
        assertEquals(3.0, frameGetMethod.invoke(buffer, -1L));
        assertEquals(3.0, frameGetMethod.invoke(buffer, 0L));

        {
            long pos = (long) findStartMethod.invoke(buffer, 0.0);
            assertEquals(0, pos);
        }

        {
            long pos = (long) findEndMethod.invoke(buffer, 0.0);
            assertEquals(-1, pos);
        }
    }
}
