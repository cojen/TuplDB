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

import java.lang.Long;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.HashMap;
import java.util.Map;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;

import org.cojen.tupl.io.Utils;

import static org.cojen.tupl.table.expr.Token.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class ArithmeticTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ArithmeticTest.class.getName());
    }

    private BasicType mType;
    private ClassMaker mClassMaker;
    private Class<?> mTestClass;
    private Map<String, Method> mTestMethods;

    private void makeTestClass(Class<?> clazz, int typeCode) throws Exception {
        makeTestClass(BasicType.make(clazz, typeCode));
    }

    private void makeTestClass(BasicType type) throws Exception {
        mType = type;
        mClassMaker = ClassMaker.begin().public_();

        addTestMethod("add", T_PLUS);
        addTestMethod("sub", T_MINUS);
        addTestMethod("mul", T_STAR);
        addTestMethod("div", T_DIV);
        addTestMethod("rem", T_REM);
        addTestMethod("and", T_AND);
        addTestMethod("or", T_OR);
        addTestMethod("xor", T_XOR);
        addTestMethod("min", Arithmetic.OP_MIN);
        addTestMethod("max", Arithmetic.OP_MAX);

        {
            Class<?> tc = mType.clazz();
            MethodMaker mm = mClassMaker.addMethod(tc, "zero").public_().static_();
            var resultVar = mm.var(tc);
            assertTrue(Arithmetic.zero(resultVar));
            mm.return_(resultVar);
        }

        {
            MethodMaker mm = mClassMaker.addMethod(String.class, "clear").public_().static_();
            var resultVar = mm.var(String.class);
            assertFalse(Arithmetic.zero(resultVar));
            mm.return_(resultVar);
        }

        mTestClass = mClassMaker.finishHidden().lookupClass();

        mTestMethods = new HashMap<>();

        for (Method m : mTestClass.getMethods()) {
            if (Modifier.isStatic(m.getModifiers())) {
                mTestMethods.putIfAbsent(m.getName(), m);
            }
        }

        assertNull(mTestClass.getMethod("clear").invoke(null));
    }

    private void addTestMethod(String name, int op) {
        Class<?> tc = mType.clazz();
        MethodMaker mm = mClassMaker.addMethod(tc, name, tc, tc).public_().static_();
        var resultVar = Arithmetic.eval(mType, op, mm.param(0), mm.param(1));
        if (resultVar == null) {
            mm.new_(UnsupportedOperationException.class).throw_();
        } else {
            mm.return_(resultVar);
        }
    }

    private void verify(String name, Object param1, Object param2, Object result) throws Exception {
        assertEquals(result, mTestMethods.get(name).invoke(null, param1, param2));
    }

    private void verifyOverflow(String name, Object param1, Object param2) throws Exception {
        try {
            mTestMethods.get(name).invoke(null, param1, param2);
            fail();
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (!(cause instanceof ArithmeticException)) {
                throw Utils.rethrow(cause);
            }
        }
    }

    private void verifyUnsupported(String name, Object param1, Object param2) throws Exception {
        try {
            mTestMethods.get(name).invoke(null, param1, param2);
            fail();
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause();
            if (!(cause instanceof UnsupportedOperationException)) {
                throw Utils.rethrow(cause);
            }
        }
    }

    @Test
    public void bool() throws Exception {
        makeTestClass(BasicType.BOOLEAN);

        assertFalse((boolean) mTestMethods.get("zero").invoke(null));

        verifyUnsupported("add", false, false);
        verifyUnsupported("sub", false, false);
        verifyUnsupported("mul", false, false);
        verifyUnsupported("div", false, false);
        verifyUnsupported("rem", false, false);

        verify("and", false, false, false);
        verify("and", true, false, false);
        verify("and", true, true, true);

        verify("or", false, false, false);
        verify("or", true, false, true);
        verify("or", true, true, true);

        verify("xor", false, false, false);
        verify("xor", true, false, true);
        verify("xor", true, true, false);

        verifyUnsupported("min", false, false);
        verifyUnsupported("max", false, false);
    }

    @Test
    public void u_byte() throws Exception {
        makeTestClass(byte.class, Type.TYPE_UBYTE);

        assertEquals((byte) 0, (byte) mTestMethods.get("zero").invoke(null));

        verify("add", (byte) 10, (byte) 20, (byte) 30);
        verify("add", (byte) 100, (byte) 100, (byte) 200);
        verifyOverflow("add", (byte) 200, (byte) 56);

        verify("sub", (byte) 20, (byte) 10, (byte) 10);
        verify("sub", (byte) 200, (byte) 100, (byte) 100);
        verifyOverflow("sub", (byte) 10, (byte) 20);

        verify("mul", (byte) 10, (byte) 20, (byte) 200);
        verifyOverflow("mul", (byte) 200, (byte) 2);

        verify("div", (byte) 200, (byte) 3, (byte) 66);
        verifyOverflow("div", (byte) 200, (byte) 0);

        verify("rem", (byte) 200, (byte) 3, (byte) 2);
        verifyOverflow("rem", (byte) 200, (byte) 0);

        verify("and", (byte) 0b00011011, (byte) 0b01101100, (byte) 0b00001000);
        verify("or", (byte) 0b00011011, (byte) 0b01101100, (byte) 0b01111111);
        verify("xor", (byte) 0b00011011, (byte) 0b01101100, (byte) 0b01110111);

        verify("min", (byte) 20, (byte) 10, (byte) 10);
        verify("min", (byte) 20, (byte) 255, (byte) 20);

        verify("max", (byte) 20, (byte) 10, (byte) 20);
        verify("max", (byte) 20, (byte) 255, (byte) 255);
    }

    @Test
    public void u_short() throws Exception {
        makeTestClass(short.class, Type.TYPE_USHORT);

        assertEquals((short) 0, (short) mTestMethods.get("zero").invoke(null));

        verify("add", (short) 10, (short) 20, (short) 30);
        verify("add", (short) 30000, (short) 20000, (short) 50000);
        verifyOverflow("add", (short) 30000, (short) 40000);

        verify("sub", (short) 20, (short) 10, (short) 10);
        verify("sub", (short) 50000, (short) 20000, (short) 30000);
        verifyOverflow("sub", (short) 10, (short) 20);

        verify("mul", (short) 300, (short) 200, (short) 60000);
        verifyOverflow("mul", (short) 50000, (short) 2);

        verify("div", (short) 60000, (short) 301, (short) 199);
        verifyOverflow("div", (short) 10000, (short) 0);

        verify("rem", (short) 60000, (short) 301, (short) 101);
        verifyOverflow("rem", (short) 200, (short) 0);

        verify("and", (short) 48137, (short) 18337, (short) 1025);
        verify("or", (short) 48137, (short) 18337, (short) 65449);
        verify("xor", (short) 48137, (short) 18337, (short) 64424);

        verify("min", (short) 20, (short) 10, (short) 10);
        verify("min", (short) 20, (short) 65535, (short) 20);

        verify("max", (short) 20, (short) 10, (short) 20);
        verify("max", (short) 20, (short) 65535, (short) 65535);
    }

    @Test
    public void u_int() throws Exception {
        makeTestClass(int.class, Type.TYPE_UINT);

        assertEquals(0, (int) mTestMethods.get("zero").invoke(null));

        verify("add", 10, 20, 30);
        verify("add", (int) 2_100_000_000L, 2_000_000_000, (int) 4_100_000_000L);
        verifyOverflow("add", (int) 3_000_000_000L, 2_000_000_000);

        verify("sub", 20, 10, 10);
        verify("sub", (int) 4_100_000_000L, 2_000_000_000, (int) 2_100_000_000L);
        verifyOverflow("sub", 10, 20);

        verify("mul", 64031, 64031, (int) 4_099_968_961L);
        verifyOverflow("mul", (int) 2_300_000_000L, 2);

        verify("div", (int) 4_099_968_961L, 64031, 64031);
        verifyOverflow("div", 10000, 0);

        verify("rem", (int) 4_099_968_961L, 64031, 0);
        verifyOverflow("rem", 200, 0);

        verify("and", (int) 4141963291L, 373335376, 373293072);
        verify("or", (int) 4141963291L, 373335376, (int) 4142005595L);
        verify("xor", (int) 4141963291L, 373335376, (int) 3768712523L);

        verify("min", 20, 10, 10);
        verify("min", 20, (int) 4_100_000_000L, 20);

        verify("max", 20, 10, 20);
        verify("max", 20, (int) 4_100_000_000L, (int) 4_100_000_000L);
    }

    @Test
    public void u_long() throws Exception {
        makeTestClass(long.class, Type.TYPE_ULONG);

        assertEquals(0L, (long) mTestMethods.get("zero").invoke(null));

        verify("add", 10L, 20L, 30L);
        verify("add", 0xf000_0000_0000_0000L, 0x0100_0000_0000_0000L, 0xf100_0000_0000_0000L);
        verifyOverflow("add", 0xff00_0000_0000_0000L, 0x0100_0000_0000_0000L);

        verify("sub", 20L, 10L, 10L);
        verify("sub", 0xf100_0000_0000_0000L, 0xf000_0000_0000_0000L, 0x0100_0000_0000_0000L);
        verifyOverflow("sub", 10, 20);

        verify("mul", 4294967295L, 4294967295L, 0xffff_fffe_0000_0001L);
        verifyOverflow("mul", 0xffff_fffe_0000_0001L, 2);

        verify("div", 0xffff_fffe_0000_0002L, 4294967295L, 4294967295L);
        verifyOverflow("div", 10000, 0);

        verify("rem", 0xffff_fffe_0000_0002L, 4294967295L, 1L);
        verifyOverflow("rem", 200, 0);

        verify("and", 570650940326738900L, 6967706883642559695L, 45673715174344900L);
        verify("or", 570650940326738900L, 6967706883642559695L, 7492684108794953695L);
        verify("xor", 570650940326738900L, 6967706883642559695L, 7447010393620608795L);

        verify("min", 20L, 10L, 10L);
        verify("min", 20L, 0xffff_fffe_0000_0001L, 20L);

        verify("max", 20L, 10L, 20L);
        verify("max", 20L, 0xffff_fffe_0000_0001L, 0xffff_fffe_0000_0001L);
    }

    @Test
    public void byte_() throws Exception {
        makeTestClass(byte.class, Type.TYPE_BYTE);

        assertEquals((byte) 0, (byte) mTestMethods.get("zero").invoke(null));

        verify("add", (byte) 10, (byte) 20, (byte) 30);
        verifyOverflow("add", (byte) 100, (byte) 100);

        verify("sub", (byte) 20, (byte) 10, (byte) 10);
        verify("sub", (byte) 10, (byte) 20, (byte) -10);

        verify("mul", (byte) 10, (byte) 5, (byte) 50);
        verifyOverflow("mul", (byte) 100, (byte) 2);

        verify("div", (byte) 100, (byte) 3, (byte) 33);
        verifyOverflow("div", (byte) 100, (byte) 0);
        verifyOverflow("div", Byte.MIN_VALUE, (byte) -1);

        verify("rem", (byte) 100, (byte) 3, (byte) 1);
        verifyOverflow("rem", (byte) 100, (byte) 0);

        verify("and", (byte) 0b00011011, (byte) 0b01101100, (byte) 0b00001000);
        verify("or", (byte) 0b00011011, (byte) 0b01101100, (byte) 0b01111111);
        verify("xor", (byte) 0b00011011, (byte) 0b01101100, (byte) 0b01110111);

        verify("min", (byte) 20, (byte) 10, (byte) 10);
        verify("min", (byte) 20, (byte) 127, (byte) 20);
        verify("min", (byte) 20, (byte) -1, (byte) -1);

        verify("max", (byte) 20, (byte) 10, (byte) 20);
        verify("max", (byte) 20, (byte) 127, (byte) 127);
        verify("max", (byte) -1, (byte) 127, (byte) 127);
    }

    @Test
    public void short_() throws Exception {
        makeTestClass(short.class, Type.TYPE_SHORT);

        assertEquals((short) 0, (short) mTestMethods.get("zero").invoke(null));

        verify("add", (short) 10, (short) 20, (short) 30);
        verifyOverflow("add", (short) 20000, (short) 20000);

        verify("sub", (short) 20, (short) 10, (short) 10);
        verify("sub", (short) 10, (short) 20, (short) -10);

        verify("mul", (short) 100, (short) 200, (short) 20000);
        verifyOverflow("mul", (short) 200, (short) 200);

        verify("div", (short) 20000, (short) 301, (short) 66);
        verifyOverflow("div", (short) 10000, (short) 0);
        verifyOverflow("div", Short.MIN_VALUE, (short) -1);

        verify("rem", (short) 20000, (short) 301, (short) 134);
        verifyOverflow("rem", (short) 200, (short) 0);

        verify("and", (short) 48137, (short) 18337, (short) 1025);
        verify("or", (short) 48137, (short) 18337, (short) 65449);
        verify("xor", (short) 48137, (short) 18337, (short) 64424);

        verify("min", (short) 20, (short) 10, (short) 10);
        verify("min", (short) 20, (short) 32767, (short) 20);
        verify("min", (short) 20, (short) -1, (short) -1);

        verify("max", (short) 20, (short) 10, (short) 20);
        verify("max", (short) 20, (short) 32767, (short) 32767);
        verify("max", (short) -1, (short) 32767, (short) 32767);
    }

    @Test
    public void int_() throws Exception {
        makeTestClass(int.class, Type.TYPE_INT);

        assertEquals(0, (int) mTestMethods.get("zero").invoke(null));

        verify("add", 10, 20, 30);
        verifyOverflow("add", 2_000_000_000, 2_000_000_000);

        verify("sub", 20, 10, 10);
        verifyOverflow("sub", -10, Integer.MAX_VALUE);

        verify("mul", 46340, 46340, 2_147_395_600);
        verifyOverflow("mul", Integer.MAX_VALUE, 2);

        verify("div", 2_147_395_600, 46340, 46340);
        verifyOverflow("div", 10000, 0);

        verify("rem", 2_147_395_601, 46340, 1);
        verifyOverflow("rem", 200, 0);

        verify("and", (int) 4141963291L, 373335376, 373293072);
        verify("or", (int) 4141963291L, 373335376, (int) 4142005595L);
        verify("xor", (int) 4141963291L, 373335376, (int) 3768712523L);

        verify("min", 20, 10, 10);
        verify("min", 20, Integer.MAX_VALUE, 20);
        verify("min", -1, Integer.MAX_VALUE, -1);

        verify("max", 20, 10, 20);
        verify("max", 20, Integer.MAX_VALUE, Integer.MAX_VALUE);
        verify("max", -1, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    @Test
    public void long_() throws Exception {
        makeTestClass(long.class, Type.TYPE_LONG);

        assertEquals(0L, (long) mTestMethods.get("zero").invoke(null));

        verify("add", 10L, 20L, 30L);
        verify("add", 0x1000_0000_0000_0000L, 0x1000_0000_0000_0000L, 0x2000_0000_0000_0000L);
        verifyOverflow("add", 0x7000_0000_0000_0000L, 0x1000_0000_0000_0000L);

        verify("sub", 20L, 10L, 10L);
        verify("sub", 0x2000_0000_0000_0000L, 0x1000_0000_0000_0000L, 0x1000_0000_0000_0000L);
        verifyOverflow("sub", -10L, Long.MAX_VALUE);

        verify("mul", 10L, 20L, 200L);
        verifyOverflow("mul", 4294967295L, 4294967295L);

        verify("div", 200L, 10L, 20L);
        verifyOverflow("div", 10000, 0);

        verify("rem", 10L, 3L, 1L);
        verifyOverflow("rem", 200, 0);

        verify("and", 570650940326738900L, 6967706883642559695L, 45673715174344900L);
        verify("or", 570650940326738900L, 6967706883642559695L, 7492684108794953695L);
        verify("xor", 570650940326738900L, 6967706883642559695L, 7447010393620608795L);

        verify("min", 20L, 10L, 10L);
        verify("min", 20L, Long.MAX_VALUE, 20L);
        verify("min", -1L, Long.MAX_VALUE, -1L);

        verify("max", 20L, 10L, 20L);
        verify("max", 20L, Long.MAX_VALUE, Long.MAX_VALUE);
        verify("max", -1L, Long.MAX_VALUE, Long.MAX_VALUE);
    }

    @Test
    public void float_() throws Exception {
        makeTestClass(float.class, Type.TYPE_FLOAT);

        assertTrue(0f == (float) mTestMethods.get("zero").invoke(null));

        verify("add", 1.25f, 2.25f, 3.5f);
        verify("sub", 3.5f, 1.25f, 2.25f);
        verify("mul", 0.25f, 2f, 0.5f);
        verify("div", 0.5f, 2f, 0.25f);
        verify("rem", 3.5f, 2f, 1.5f);

        verifyUnsupported("and", 0f, 0f);
        verifyUnsupported("or", 0f, 0f);
        verifyUnsupported("xor", 0f, 0f);

        verify("min", 2f, 1f, 1f);
        verify("max", 2f, 10f, 10f);
    }

    @Test
    public void double_() throws Exception {
        makeTestClass(double.class, Type.TYPE_DOUBLE);

        assertTrue(0d == (double) mTestMethods.get("zero").invoke(null));

        verify("add", 1.25d, 2.25d, 3.5d);
        verify("sub", 3.5d, 1.25d, 2.25d);
        verify("mul", 0.25d, 2d, 0.5d);
        verify("div", 0.5d, 2d, 0.25d);
        verify("rem", 3.5d, 2d, 1.5d);

        verifyUnsupported("and", 0d, 0d);
        verifyUnsupported("or", 0d, 0d);
        verifyUnsupported("xor", 0d, 0d);

        verify("min", 2d, 1d, 1d);
        verify("max", 2d, 10d, 10d);
    }

    @Test
    public void bigInteger() throws Exception {
        makeTestClass(BigInteger.class, Type.TYPE_BIG_INTEGER);

        assertEquals(BigInteger.ZERO, mTestMethods.get("zero").invoke(null));

        verify("add", bi(1), bi(2), bi(3));
        verify("sub", bi(3), bi(2), bi(1));
        verify("mul", bi(2), bi(3), bi(6));
        verify("div", bi(6), bi(2), bi(3));
        verify("rem", bi(5), bi(2), bi(1));
        verify("and", bi(570650940326738900L), bi(6967706883642559695L), bi(45673715174344900L));
        verify("or", bi(570650940326738900L), bi(6967706883642559695L), bi(7492684108794953695L));
        verify("xor", bi(570650940326738900L), bi(6967706883642559695L), bi(7447010393620608795L));
        verify("min", bi(2), bi(1), bi(1));
        verify("max", bi(2), bi(10), bi(10));
    }

    private static BigInteger bi(long v) {
        return BigInteger.valueOf(v);
    }

    @Test
    public void bigDecmial() throws Exception {
        makeTestClass(BigDecimal.class, Type.TYPE_BIG_DECIMAL);

        assertEquals(BigDecimal.ZERO, mTestMethods.get("zero").invoke(null));

        verify("add", bd("1.25"), bd("2.25"), bd("3.50"));
        verify("sub", bd("3.50"), bd("1.25"), bd("2.25"));
        verify("mul", bd("0.25"), bd("2"), bd("0.50"));
        verify("div", bd("0.50"), bd("2"), bd("0.25"));
        verify("rem", bd("3.5"), bd("2"), bd("1.5"));

        verifyUnsupported("and", null, null);
        verifyUnsupported("or", null, null);
        verifyUnsupported("xor", null, null);

        verify("min", bd("2"), bd("1"), bd("1"));
        verify("max", bd("2"), bd("10"), bd("10"));
    }

    private static BigDecimal bd(String v) {
        return new BigDecimal(v);
    }
}
