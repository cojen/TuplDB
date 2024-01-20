/*
 *  Copyright (C) 2021 Cojen.org
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

package org.cojen.tupl.table;

import java.lang.invoke.MethodHandles;

import java.math.BigInteger;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import static org.cojen.tupl.table.filter.ColumnFilter.*;

import static org.cojen.tupl.table.ColumnInfo.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CompareUtilsTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CompareUtilsTest.class.getName());
    }

    private static ColumnInfo columnInfo(int typeCode) {
        var c = new ColumnInfo();
        c.typeCode = typeCode;
        c.assignType();
        c.name = c.type.getSimpleName();
        return c;
    }

    @Test
    public void compareIntegers() throws Throwable {
        // Tests comparing signed and unsigned integer types, of various sizes.

        int[] typeCodes = {
            TYPE_UBYTE, TYPE_USHORT, TYPE_UINT, TYPE_ULONG,
            TYPE_BYTE, TYPE_SHORT, TYPE_INT, TYPE_LONG
        };

        int[] ops = { OP_EQ, OP_NE, OP_LT, OP_GE, OP_GT, OP_LE };

        long[] values = {
            0, 10, 127, 128, 200, 255, 256, 10_000, 65535, 65536, 100_000,
            1_000_000_000L, 2_147_483_647L, 2_147_483_648L, 100_000_000_000L, Long.MAX_VALUE,

            -1, -10, -127, -128, -200, -255, -256, -10_000, -65535, -65536, -100_000,
            -1_000_000_000L, -2_147_483_647L, -2_147_483_648L, -100_000_000_000L, Long.MIN_VALUE,
        };

        for (int op : ops) {
            for (int leftTypeCode : typeCodes) {
                ColumnInfo leftInfo = columnInfo(leftTypeCode);
                for (long leftValue : values) {
                    for (int rightTypeCode : typeCodes) {
                        ColumnInfo rightInfo = columnInfo(rightTypeCode);
                        for (long rightValue : values) {
                            doCompareIntegers(leftInfo, leftValue, op, rightInfo, rightValue);
                        }
                    }
                }
            }
        }
    }

    private void doCompareIntegers(ColumnInfo leftInfo, long leftValue,
                                   int op,
                                   ColumnInfo rightInfo, long rightValue)
        throws Throwable
    {
        MethodMaker mm = MethodMaker.begin(MethodHandles.lookup(), null, "_");

        Variable leftVar = mm.var(long.class).set(leftValue).cast(leftInfo.type);
        Variable rightVar = mm.var(long.class).set(rightValue).cast(rightInfo.type);

        BigInteger leftBig = toBigInteger(leftInfo, leftValue);
        BigInteger rightBig = toBigInteger(rightInfo, rightValue);

        int cmp = leftBig.compareTo(rightBig);

        Label passLabel = mm.label();
        Label failLabel = mm.label();

        boolean flip;

        switch (op) {
        default: fail();
        case OP_EQ: flip = cmp != 0; break;
        case OP_NE: flip = cmp == 0; break;
        case OP_LT: flip = cmp >= 0; break;
        case OP_GE: flip = cmp < 0; break;
        case OP_GT: flip = cmp <= 0; break;
        case OP_LE: flip = cmp > 0; break;
        }

        if (!flip) {
            CompareUtils.comparePrimitives(mm, leftInfo, leftVar, rightInfo, rightVar,
                                           op, passLabel, failLabel);
        } else {
            CompareUtils.comparePrimitives(mm, leftInfo, leftVar, rightInfo, rightVar,
                                           op, failLabel, passLabel);
        }

        failLabel.here();
        mm.var(Assert.class).invoke("fail", "" + leftInfo.typeCode + ", " + leftValue + ", " +
                                    op + ", " + rightInfo.typeCode + ", " + rightValue);

        passLabel.here();

        mm.finish().invoke();
    }

    private static BigInteger toBigInteger(ColumnInfo info, long value) {
        switch (info.typeCode) {
        case TYPE_UBYTE:
            return BigInteger.valueOf(value & 0xff);
        case TYPE_USHORT:
            return BigInteger.valueOf(value & 0xffff);
        case TYPE_UINT:
            return BigInteger.valueOf(value & 0xffff_ffffL);
        case TYPE_ULONG:
            var bytes = new byte[8];
            RowUtils.encodeLongBE(bytes, 0, value);
            return new BigInteger(1, bytes);
        case TYPE_BYTE:
            return BigInteger.valueOf((byte) value);
        case TYPE_SHORT:
            return BigInteger.valueOf((short) value);
        case TYPE_INT:
            return BigInteger.valueOf((int) value);
        case TYPE_LONG:
            return BigInteger.valueOf(value);
        }

        throw new AssertionError();
    }
}
