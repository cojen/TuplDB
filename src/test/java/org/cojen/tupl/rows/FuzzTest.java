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

import java.math.*;

import java.util.*;

import java.util.concurrent.atomic.AtomicLong;

import org.cojen.maker.*;

import org.cojen.tupl.*;

import org.junit.*;
import static org.junit.Assert.*;


/**
 * 
 *
 * @author Brian S O'Neill
 */
public class FuzzTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(FuzzTest.class.getName());
    }

    @Test
    public void fuzz() throws Exception {
        var rnd = new Random(8675309);

        Database db = Database.open(new DatabaseConfig());
        RowStore rs = new RowStore(db);
        rsRef = rs;

        for (int i=0; i<100; i++) {
            Class<?> rowType = randomRowType(rnd);
            var rowIndex = rs.openRowIndex(rowType);
            // FIXME: Perform some operations on it.
        }
    }

    // Prevent GC. The generated code depends on weak references. When the feature is finished,
    // the RowStore will be referenced by the Database and won't go away immediately.
    private static Object rsRef;

    private static final AtomicLong packageNum = new AtomicLong();

    /**
     * @return an interface
     */
    static Class randomRowType(Random rnd) {
        ClassMaker cm = ClassMaker.begin("test.p" + packageNum.getAndIncrement() + ".TestRow");
        cm.public_().interface_();

        Column[] columns = randomColumns(rnd);

        for (Column c : columns) {
            Type t = c.type;
            MethodMaker mm = cm.addMethod(t.clazz, c.name).public_().abstract_();
            if (t.nullable) {
                mm.addAnnotation(Nullable.class, true);
            }
            cm.addMethod(void.class, c.name, t.clazz).public_().abstract_();
        }

        // Now add the primary key.

        Collections.shuffle(Arrays.asList(columns), rnd);
        int pkNum = 1 + rnd.nextInt(columns.length);
        var pkNames = new String[pkNum];

        for (int i=0; i<pkNames.length; i++) {
            pkNames[i] = columns[i].name;
        }

        AnnotationMaker am = cm.addAnnotation(PrimaryKey.class, true);
        am.put("value", pkNames);

        return cm.finish();
    }

    static Column[] randomColumns(Random rnd) {
        var columns = new Column[1 + rnd.nextInt(20)];

        for (int i=0; i<columns.length; i++) {
            columns[i] = new Column(randomType(rnd), String.valueOf((char) ('a' + i)));
        }

        Collections.shuffle(Arrays.asList(columns), rnd);

        return columns;
    }

    static class Column {
        final Type type;
        final String name;

        Column(Type type, String name) {
            this.type = type;
            this.name = name;
        }
    }

    static Type randomType(Random rnd) {
        int n = rnd.nextInt(19);

        final Class clazz;

        switch (n) {
        default: throw new AssertionError();
        case 0: clazz = boolean.class; break;
        case 1: clazz = byte.class; break;
        case 2: clazz = short.class; break;
        case 3: clazz = int.class; break;
        case 4: clazz = long.class; break;
        case 5: clazz = float.class; break;
        case 6: clazz = double.class; break;
        case 7: clazz = char.class; break;
        case 8: clazz = Boolean.class; break;
        case 9: clazz = Byte.class; break;
        case 10: clazz = Short.class; break;
        case 11: clazz = Integer.class; break;
        case 12: clazz = Long.class; break;
        case 13: clazz = Float.class; break;
        case 14: clazz = Double.class; break;
        case 15: clazz = Character.class; break;
        case 16: clazz = String.class; break;
        case 17: clazz = BigInteger.class; break;
        case 18: clazz = BigDecimal.class; break;
        }

        boolean nullable;
        if (clazz.isPrimitive()) {
            nullable = false;
        } else {
            nullable = rnd.nextBoolean();
        }

        return new Type(clazz, nullable);
    }

    static class Type {
        final Class clazz;
        final boolean nullable;

        Type(Class clazz, boolean nullable) {
            this.clazz = clazz;
            this.nullable = nullable;
        }
    }
}
