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
import java.math.BigInteger;
import java.math.MathContext;

import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.PrimaryKey;
import org.cojen.tupl.QueryException;
import org.cojen.tupl.Row;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Unsigned;

import static org.cojen.tupl.table.expr.Token.*;
import static org.cojen.tupl.table.expr.Type.*;

import java.lang.Double;
import java.lang.Float;
import java.lang.Long;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class BinaryOpTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(BinaryOpTest.class.getName());
    }

    @PrimaryKey("id")
    public static interface TestRow extends Row {
        long id();
        void id(long id);

        @Unsigned
        byte c3();
        void c3(byte v);

        @Unsigned
        short c4();
        void c4(short v);

        @Unsigned
        int c5();
        void c5(int v);

        @Unsigned
        long c6();
        void c6(long v);

        byte c11();
        void c11(byte v);

        short c12();
        void c12(short v);

        int c13();
        void c13(int v);

        long c14();
        void c14(long v);

        float c17();
        void c17(float v);

        double c18();
        void c18(double v);

        BigInteger c28();
        void c28(BigInteger v);

        BigDecimal c29();
        void c29(BigDecimal v);
    }

    private Random mRnd;
    private Database mDb;
    private Table<TestRow> mTable;

    @After
    public void teardown() throws Exception {
        if (mDb != null) {
            mDb.close();
        }
    }

    @Test
    public void fuzz() throws Exception {
        mRnd = new Random(5925012809348304926L);
        mDb = Database.open(new DatabaseConfig());
        mTable = mDb.openTable(TestRow.class);

        {
            TestRow row = mTable.newRow();
            row.id(1);
            updateColumn(row, TYPE_UBYTE, 0);
            updateColumn(row, TYPE_USHORT, 0);
            updateColumn(row, TYPE_UINT, 0);
            updateColumn(row, TYPE_ULONG, 0);
            updateColumn(row, TYPE_BYTE, 0);
            updateColumn(row, TYPE_SHORT, 0);
            updateColumn(row, TYPE_INT, 0);
            updateColumn(row, TYPE_LONG, 0);
            updateColumn(row, TYPE_FLOAT, 0);
            updateColumn(row, TYPE_DOUBLE, 0);
            updateColumn(row, TYPE_BIG_INTEGER, 0);
            updateColumn(row, TYPE_BIG_DECIMAL, 0);
            mTable.store(null, row);
        }

        for (int i=0; i<2000; i++) {
            doFuzz(false);
        }

        for (int i=0; i<1000; i++) {
            doFuzz(true);
        }
    }

    private void doFuzz(boolean small) throws Exception {
        int type1, type2;
        Object value1, value2;
        int mode1, mode2;

        while (true) {
            type1 = randomType();
            value1 = small ? randomValueSmall(type1) : randomValue(type1);
            mode1 = mRnd.nextInt(3); // 0: constant, 1: column, 2: param

            type2 = randomType();
            value2 = small ? randomValueSmall(type2) : randomValue(type2);
            // Restrict mode2 such that "? <op> ?" forms aren't produced. They aren't supported
            // because a common type cannot be inferred, other than the "any" type.
            mode2 = mRnd.nextInt(mode1 == 2 ? 2 : 3);

            if (mode1 == 2 && isUnsignedInteger(type2) && value1.toString().startsWith("-")) {
                // Don't bother testing a negative parameter as being compared against an
                // unsigned column or constant.
                continue;
            }

            if (mode2 == 2 && isUnsignedInteger(type1) && value2.toString().startsWith("-")) {
                // Don't bother testing a negative parameter as being compared against an
                // unsigned column or constant.
                continue;
            }

            if (mode1 == 1 && mode2 == 1 && type1 == type2) {
                // Cannot update the same column.
                continue;
            }

            break;
        }

        if (mode1 == 1) {
            updateRow(type1, value1);
        }

        if (mode2 == 1) {
            updateRow(type2, value2);
        }

        int op = randomOp(type1, type2);

        Object expect;
        ArithmeticException expectException;

        try {
            expect = calc(type1, value1, mode1, op, type2, value2, mode2);
            expectException = null;
        } catch (ArithmeticException e) {
            expect = null;
            expectException = e;
        }

        var b = new StringBuilder();

        b.append('{').append("v = ");
        appendValue(b, type1, value1, mode1);
        b.append(' ');
        appendOp(b, op);
        b.append(' ');
        appendValue(b, type2, value2, mode2);
        b.append('}');

        String queryStr = b.toString();

        Table<Row> derived;
        if (mode1 == 2) {
            derived = mTable.derive(queryStr, value1);
        } else if (mode2 == 2) {
            derived = mTable.derive(queryStr, value2);
        } else {
            derived = mTable.derive(queryStr);
        }

        Row row;
        try {
            try (Scanner<Row> s = derived.newScanner(null)) {
                row = s.row();
            }
        } catch (ArithmeticException e) {
            if ((mode1 == 2) != (mode2 == 2)) {
                // Parameter conversion error, which is only possible when given just one
                // parameter.
                return;
            }

            if (expect == null) {
                assertNotNull(expectException);
                return;
            }

            if ((isUnsignedInteger(type1) || isUnsignedInteger(type2))
                && expect.toString().startsWith("-"))
            {
                // Unsigned integer overflow.
                return;
            }

            if (expectException == null) {
                throw e;
            }

            return;
        }

        if (expect instanceof BigInteger bi) {
            BigInteger result = row.getBigInteger("v");
            assertEquals(expect, result);
        } else if (expect instanceof BigDecimal bd) {
            BigDecimal result = row.getBigDecimal("v");
            if (!expect.equals(result)) {
                BigDecimal diff = result.subtract((BigDecimal) expect);
                BigDecimal ratio = diff.divide(result, MathContext.DECIMAL64).abs();
                assertTrue("" + ratio, ratio.compareTo(BigDecimal.valueOf(0.00001)) < 0);
            }
        } else if (expect == null) {
            assertNotNull(expectException);
            String str = row.getString("v");
            assertTrue(str.equals("NaN") || str.contains("Infinity"));
        } else {
            fail("" + expect.getClass());
        }
    }

    private int randomType() {
        return switch (mRnd.nextInt(12)) {
            case 0 -> TYPE_UBYTE; case 1 -> TYPE_USHORT; case 2 -> TYPE_UINT; case 3 -> TYPE_ULONG;
            case 4 -> TYPE_BYTE; case 5 -> TYPE_SHORT; case 6 -> TYPE_INT; case 7 -> TYPE_LONG;
            case 8 -> TYPE_FLOAT; case 9 -> TYPE_DOUBLE;
            case 10 -> TYPE_BIG_INTEGER; case 11 -> TYPE_BIG_DECIMAL;
            default -> throw new AssertionError();
        };
    }

    private Object randomValue(int type) {
        return switch (type) {
            case TYPE_UBYTE -> (byte) mRnd.nextInt(10);
            case TYPE_USHORT -> (short) mRnd.nextInt(100);
            case TYPE_UINT -> mRnd.nextInt(1000);
            case TYPE_ULONG -> mRnd.nextLong(10000);
            case TYPE_BYTE -> (byte) (mRnd.nextInt(20) - 10);
            case TYPE_SHORT -> (short) (mRnd.nextInt(200) - 100);
            case TYPE_INT -> mRnd.nextInt(2000) - 1000;
            case TYPE_LONG -> mRnd.nextLong(20000) - 10000;
            case TYPE_FLOAT -> (float) mRnd.nextInt(1000);
            case TYPE_DOUBLE -> (double) mRnd.nextInt(10000);
            case TYPE_BIG_INTEGER -> BigInteger.valueOf(mRnd.nextInt(100000));
            case TYPE_BIG_DECIMAL -> BigDecimal.valueOf(mRnd.nextInt(100000));
            default -> throw new AssertionError();
        };
    }

    private Object randomValueSmall(int type) {
        return switch (type) {
            case TYPE_UBYTE -> (byte) mRnd.nextInt(10);
            case TYPE_USHORT -> (short) mRnd.nextInt(10);
            case TYPE_UINT -> mRnd.nextInt(10);
            case TYPE_ULONG -> mRnd.nextLong(10);
            case TYPE_BYTE -> (byte) (mRnd.nextInt(20) - 10);
            case TYPE_SHORT -> (short) (mRnd.nextInt(20) - 10);
            case TYPE_INT -> mRnd.nextInt(20) - 10;
            case TYPE_LONG -> mRnd.nextLong(20) - 10;
            case TYPE_FLOAT -> (float) mRnd.nextInt(10);
            case TYPE_DOUBLE -> (double) mRnd.nextInt(10);
            case TYPE_BIG_INTEGER -> BigInteger.valueOf(mRnd.nextInt(10));
            case TYPE_BIG_DECIMAL -> BigDecimal.valueOf(mRnd.nextInt(10));
            default -> throw new AssertionError();
        };
    }

    private int randomOp(int type1, int type2) {
        if (!isInteger(type1) || !isInteger(type2)) {
            return T_PLUS + mRnd.nextInt(T_REM - T_PLUS + 1);
        }

        while (true) {
            int op = T_AND + mRnd.nextInt(T_REM - T_AND + 1);
            if (op != T_NOT) {
                return op;
            }
        }
    }

    private void updateRow(int type, Object value) throws Exception {
        TestRow row = mTable.newRow();
        row.id(1);
        updateColumn(row, type, value);
        mTable.update(null, row);
    }

    private static void updateColumn(TestRow row, int type, Object value) throws Exception {
        row.set("c" + type, value);
    }

    /**
     * @param mode 0: constant, 1: column, 2: param
     */
    private static void appendValue(StringBuilder b, int type, Object value, int mode) {
        switch (mode) {
        case 0 -> appendConstant(b, value);
        case 1 -> b.append("c" + type);
        case 2 -> b.append('?');
        case 3 -> throw new AssertionError();
        }
    }

    private static void appendConstant(StringBuilder b, Object value) {
        b.append(value);

        if (value instanceof BigInteger) {
            b.append('g');
        } else if (value instanceof BigDecimal) {
            if (!value.toString().contains(".")) {
                b.append('.');
            }
            b.append('g');
        } else if (value instanceof Double) {
            b.append('d');
        } else if (value instanceof Float) {
            b.append('f');
        } else if (value instanceof Long) {
            b.append('L');
        }
    }
        
    private static void appendOp(StringBuilder b, int op) {
        b.append(BinaryOpExpr.opString(op));
    }

    /**
     * @return BigInteger or BigDecimal
     */
    private static Object calc(int type1, Object value1, int mode1, int op,
                               int type2, Object value2, int mode2)
    {
        switch (op) {
        case T_AND: return toBigInteger(value1).and(toBigInteger(value2));
        case T_OR: return toBigInteger(value1).or(toBigInteger(value2));
        case T_XOR: return toBigInteger(value1).xor(toBigInteger(value2));
        }

        if (mode1 == 2) {
            assertNotEquals(2, mode2);
            type1 = type2;
        } else if (mode2 == 2) {
            type2 = type1;
        }

        if (isInteger(type1) && isInteger(type2)) {
            return switch (op) {
                case T_PLUS -> toBigInteger(value1).add(toBigInteger(value2));
                case T_MINUS -> toBigInteger(value1).subtract(toBigInteger(value2));
                case T_STAR -> toBigInteger(value1).multiply(toBigInteger(value2));
                case T_DIV -> toBigInteger(value1).divide(toBigInteger(value2));
                case T_REM -> toBigInteger(value1).remainder(toBigInteger(value2));
                default -> throw new AssertionError();
            };
        }

        return switch (op) {
            case T_PLUS -> toBigDecimal(value1).add(toBigDecimal(value2));
            case T_MINUS -> toBigDecimal(value1).subtract(toBigDecimal(value2));
            case T_STAR -> toBigDecimal(value1).multiply(toBigDecimal(value2));
        case T_DIV -> toBigDecimal(value1).divide(toBigDecimal(value2), MathContext.DECIMAL64);
        case T_REM -> toBigDecimal(value1).remainder(toBigDecimal(value2), MathContext.DECIMAL64);
            default -> throw new AssertionError();
        };
    }

    private static BigInteger toBigInteger(Object value) {
        if (value instanceof BigInteger bi) {
            return bi;
        }
        return BigInteger.valueOf(((Number) value).longValue());
    }

    private static BigDecimal toBigDecimal(Object value) {
        if (value instanceof BigDecimal bd) {
            return bd;
        }
        return BigDecimal.valueOf(((Number) value).longValue());
    }

    @Test
    public void broken() throws Exception {
        try {
            Parser.parse("true < 0");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("No common type"));
        }

        try {
            Parser.parse("0 && 1");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("Boolean operation not allowed"));
        }

        try {
            Parser.parse("'a' & 'b'");
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage().contains("Bitwise operation not allowed"));
        }
    }

    @PrimaryKey({"a", "b"})
    public static interface LogicRow extends Row {
        int a();
        void a(int a);

        boolean b();
        void b(boolean b);
    }

    @Test
    public void logic() throws Exception {
        mDb = Database.open(new DatabaseConfig());
        Table<LogicRow> table = mDb.openTable(LogicRow.class);

        {
            LogicRow row = table.newRow();
            row.a(0); row.b(false);
            table.insert(null, row);
            row.a(0); row.b(true);
            table.insert(null, row);
            row.a(1); row.b(false);
            table.insert(null, row);
            row.a(1); row.b(true);
            table.insert(null, row);
        }

        verify(table, "{v = (a != 0) && b}", false, false, false, true);
        verify(table, "{v = (a != 0) && (b == true)}", false, false, false, true);
        verify(table, "{v = (a > 0) && (b != false)}", false, false, false, true);

        verify(table, "{v = (a != 0) & b}", false, false, false, true);
        verify(table, "{v = (a != 0) & (b == true)}", false, false, false, true);
        verify(table, "{v = (a > 0) & (b != false)}", false, false, false, true);

        verify(table, "{v = (a != 0) || b}", false, true, true, true);
        verify(table, "{v = (a != 0) || (b == true)}", false, true, true, true);
        verify(table, "{v = (a > 0) || (b != false)}", false, true, true, true);

        verify(table, "{v = (a != 0) | b}", false, true, true, true);
        verify(table, "{v = (a != 0) | (b == true)}", false, true, true, true);
        verify(table, "{v = (a > 0) | (b != false)}", false, true, true, true);

        verify(table, "{v = (a != 0) ^ b}", false, true, true, false);
        verify(table, "{v = (a != 0) ^ (b == true)}", false, true, true, false);
        verify(table, "{v = (a > 0) ^ (b != false)}", false, true, true, false);

        verify(table, "{v = (a != 0) != b}", false, true, true, false);
        verify(table, "{v = (a != 0) != (b == true)}", false, true, true, false);
        verify(table, "{v = (a > 0) != (b != false)}", false, true, true, false);

        verify(table, "{v = (a != 0) == b}", true, false, false, true);
        verify(table, "{v = (a != 0) == (b == true)}", true, false, false, true);
        verify(table, "{v = (a > 0) == (b != false)}", true, false, false, true);

        verify(table, "{v = b < true}", true, false, true, false);
        verify(table, "{v = b <= true}", true, true, true, true);
        verify(table, "{v = b > true}", false, false, false, false);
        verify(table, "{v = b >= true}", false, true, false, true);

        verify(table, "{v = true < b}", false, false, false, false);
        verify(table, "{v = true <= b}", false, true, false, true);
        verify(table, "{v = true > b}", true, false, true, false);
        verify(table, "{v = true >= b}", true, true, true, true);
    }

    @Test
    public void constant() throws Exception {
        mDb = Database.open(new DatabaseConfig());
        Table<LogicRow> table = mDb.openTable(LogicRow.class);

        {
            LogicRow row = table.newRow();
            row.a(0); row.b(false);
            table.insert(null, row);
        }

        verify(table, "{v = a == a}", true);
        verify(table, "{v = a >= a}", true);
        verify(table, "{v = a <= a}", true);
        verify(table, "{v = a != a}", false);
        verify(table, "{v = a > a}", false);
        verify(table, "{v = a < a}", false);

        verify(table, "{v = 0 == 1}", false);
        verify(table, "{v = 0 != 1}", true);

        verify(table, "{v = a == null}", false);
        verify(table, "{v = null == a}", false);
        verify(table, "{v = true == (a == null)}", false);
        verify(table, "{v = true == (null == a)}", false);
    }

    private static void verify(Table<? extends Row> table, String query, boolean... results)
        throws Exception
    {
        int i = 0;

        try (Scanner<Row> s = table.derive(query).newScanner(null)) {
            for (Row row = s.row(); row != null; row = s.step(row)) {
                boolean v = row.get_boolean("v");
                assertEquals(results[i++], v);
            }
        }

        assertEquals(results.length, i);
    }

    @SuppressWarnings("unchecked")
    private static void dump(Table table, String query) throws Exception {
        try (Scanner s = table.derive(query).newScanner(null)) {
            for (Object row = s.row(); row != null; row = s.step(row)) {
                System.out.println(row);
            }
        }
    }
}
