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

package org.cojen.tupl.rows;

import java.lang.reflect.Method;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.util.ArrayList;
import java.util.Random;

import java.util.concurrent.atomic.AtomicLong;

import org.cojen.maker.AnnotationMaker;
import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;

import org.cojen.tupl.Nullable;
import org.cojen.tupl.PrimaryKey;

import org.cojen.tupl.core.TestUtils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RowTestUtils extends TestUtils {
    private static final AtomicLong packageNum = new AtomicLong();

    public static String newRowTypeName() {
        // Generate different packages to faciliate class unloading.
        return "test.p" + packageNum.getAndIncrement() + ".TestRow";
    }

    public static ClassMaker newRowTypeMaker() {
        return newRowTypeMaker(null);
    }

    /**
     * @param rowTypeName can be null to assign automatically
     */
    public static ClassMaker newRowTypeMaker(String rowTypeName) {
        ClassMaker cm;
        if (rowTypeName == null) {
            cm = ClassMaker.begin(newRowTypeName());
        } else {
            cm = ClassMaker.beginExternal(rowTypeName);
        }
        return cm.public_().interface_();
    }

    /**
     * Define a new row type interface. Specification consists of alternating Class and name
     * pairs. A name suffix of '?' indicates that it's Nullable. A name prefix of '+' or '-'
     * indicates that it's part of the primary key.
     *
     * @param rowTypeName can be null to assign automatically
     */
    public static Class<?> newRowType(String rowTypeName, Object... spec) {
        if ((spec.length & 1) != 0) {
            throw new IllegalArgumentException("Odd spec length");
        }

        ClassMaker cm = newRowTypeMaker(rowTypeName);

        var pkNames = new ArrayList<String>();

        for (int i=0; i<spec.length; i+=2) {
            var type = (Class) spec[i];
            var name = (String) spec[i + 1];

            boolean nullable = false;

            if (name.endsWith("?")) {
                nullable = true;
                name = name.substring(0, name.length() - 1);
            }

            if (name.startsWith("+") || name.startsWith("-")) {
                pkNames.add(name);
                name = name.substring(1);
            }

            MethodMaker mm = cm.addMethod(type, name).public_().abstract_();
            if (nullable) {
                mm.addAnnotation(Nullable.class, true);
            }

            cm.addMethod(void.class, name, type).public_().abstract_();
        }

        AnnotationMaker am = cm.addAnnotation(PrimaryKey.class, true);
        am.put("value", pkNames.toArray());

        return cm.finish();
    }

    /**
     * Returns the getters and setters for the generated row type.
     */
    static Method[][] access(Object[] spec, Class<?> rowType) throws Exception {
        var getters = new Method[spec.length / 2];
        var setters = new Method[spec.length / 2];

        for (int i=0; i<setters.length; i++) {
            var type = (Class) spec[i * 2];

            var name = (String) spec[i * 2 + 1];
            if (name.startsWith("+") || name.startsWith("-")) {
                name = name.substring(1);
            }
            if (name.endsWith("?")) {
                name = name.substring(0, name.length() - 1);
            }

            getters[i] = rowType.getMethod(name);
            setters[i] = rowType.getMethod(name, type);
        }

        return new Method[][] {getters, setters};
    }

    static Object randomValue(Random rnd, Object[] spec, int colNum) {
        var type = (Class) spec[colNum * 2];
        var name = (String) spec[colNum * 2 + 1];
        return randomValue(rnd, type, name.endsWith("?"));
    }

    static Object randomValue(Random rnd, Class<?> type, boolean nullable) {
        if (nullable && rnd.nextInt(10) == 0) {
            return null;
        }

        if (type == boolean.class || type == Boolean.class) {
            return rnd.nextBoolean();
        } else if (type == byte.class || type == Byte.class) {
            return (byte) rnd.nextInt();
        } else if (type == char.class || type == Character.class) {
            return randomChar(rnd);
        } else if (type == double.class || type == Double.class) {
            return rnd.nextDouble();
        } else if (type == float.class || type == Float.class) {
            return rnd.nextFloat();
        } else if (type == int.class || type == Integer.class) {
            return rnd.nextInt();
        } else if (type == long.class || type == Long.class) {
            return rnd.nextLong();
        } else if (type == short.class || type == Short.class) {
            return (short) rnd.nextInt();
        } else if (type == String.class) {
            var codepoints = new int[rnd.nextInt(10)];
            for (int i=0; i<codepoints.length; i++) {
                codepoints[i] = rnd.nextInt(Character.MAX_CODE_POINT + 1);
            }
            return new String(codepoints, 0, codepoints.length);
        } else if (type == BigInteger.class) {
            return RowTestUtils.randomBigInteger(rnd);
        } else if (type == BigDecimal.class) {
            return RowTestUtils.randomBigDecimal(rnd);
        } else {
            throw new AssertionError();
        }
    }

    static char randomChar(Random rnd) {
        while (true) {
            char c = (char) rnd.nextInt();
            if (c < Character.MIN_SURROGATE || c > Character.MAX_SURROGATE) {
                return c;
            }
        }
    }

    static char randomDigit(Random rnd) {
        return (char) ('0' + rnd.nextInt(10));
    }

    static BigInteger randomBigInteger(Random rnd) {
        var digits = new char[1 + rnd.nextInt(20)];
        for (int i=0; i<digits.length; i++) {
            digits[i] = randomDigit(rnd);
        }
        if (digits.length > 1 && rnd.nextBoolean()) {
            digits[0] = '-';
        }
        return new BigInteger(new String(digits));
    }

    static BigDecimal randomBigDecimal(Random rnd) {
        var digits = new char[1 + rnd.nextInt(20)];
        for (int i=0; i<digits.length; i++) {
            digits[i] = randomDigit(rnd);
        }
        if (digits.length > 1 && rnd.nextBoolean()) {
            digits[0] = '-';
        }
        if (digits.length > 2) {
            int decimalPos = rnd.nextInt(digits.length - 1);
            if (decimalPos > 1) {
                digits[decimalPos] = '.';
            }
        }
        return new BigDecimal(new String(digits));
    }
}
