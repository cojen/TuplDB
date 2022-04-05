/*
 *  Copyright (C) 2022 Cojen.org
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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.reflect.Method;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.*;

import static org.cojen.tupl.rows.RowTestUtils.*;
import static org.cojen.tupl.rows.TransformMaker.Availability.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TransformMakerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(TransformMakerTest.class.getName());
    }

    @Test
    public void fuzzBinary() throws Throwable {
        // Test pure binary transformations (never examines a row instance).

        var tasks = new TestUtils.TestTask[4];
        for (int i=0; i<tasks.length; i++) {
            tasks[i] = TestUtils.startTestTask(() -> fuzz(2, 0.0f, 1.0f, 0.0f));
        }
        for (var task : tasks) {
            task.join();
        }
    }

    @Test
    public void fuzzRow() throws Throwable {
        // Test transformations where row columns are usually (but not always) available.

        var tasks = new TestUtils.TestTask[4];
        for (int i=0; i<tasks.length; i++) {
            tasks[i] = TestUtils.startTestTask(() -> fuzz(2, 5.0f, 1.0f, 0.0f));
        }
        for (var task : tasks) {
            task.join();
        }
    }

    @Test
    public void fuzzMix() throws Throwable {
        // Test transformations where row column availability is always, never, or conditional.

        var tasks = new TestUtils.TestTask[4];
        for (int i=0; i<tasks.length; i++) {
            tasks[i] = TestUtils.startTestTask(() -> fuzz(2, 1.0f, 1.0f, 1.0f));
        }
        for (var task : tasks) {
            task.join();
        }
    }

    /**
     * @param n number of test iterations (times 100)
     * @param always probability of columns always being available in the row
     * @param never probability of columns never being available in the row
     * @param conditional probability of columns conditionally being available in the row
     */
    private static void fuzz(int n, float always, float never, float conditional) {
        for (int i=0; i<n; i++) {
            long seed = ThreadLocalRandom.current().nextLong();
            try {
                fuzzIt(seed, always, never, conditional);
            } catch (Throwable e) {
                var err = new AssertionError("seed: " + seed, e);
                err.printStackTrace(System.out);
                throw err;
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void fuzzIt(long seed, float always, float never, float conditional)
        throws Throwable
    {
        var rnd = new Random(seed);

        Database db = Database.open(new DatabaseConfig());

        for (int t=0; t<100; t++) {
            FuzzTest.Column[] columns = FuzzTest.randomColumns(rnd);
            Class<?> srcRowType = FuzzTest.randomRowType(rnd, columns);
            Class<?>[] dstRowTypes = transformedRowTypes(rnd, srcRowType, columns, 3);

            Index srcIndex = db.openIndex(srcRowType.getName());
            Table srcTable = srcIndex.asTable(srcRowType);

            var dstIndexes = new Index[dstRowTypes.length];
            var dstTables = new Table[dstRowTypes.length];
            for (int i=0; i<dstRowTypes.length; i++) {
                dstIndexes[i] = db.openIndex(dstRowTypes[i].getName());
                dstTables[i] = dstIndexes[i].asTable(dstRowTypes[i]);
            }

            var row = srcTable.newRow();
            FuzzTest.fillInColumns(rnd, columns, row);
            srcTable.store(null, row);

            byte[] srcKey, srcValue;
            try (Cursor c = srcIndex.newCursor(null)) {
                c.first();
                srcKey = c.key();
                srcValue = c.value();
            }

            Map<String, TransformMaker.Availability> available =
                availability(rnd, always, never, conditional, srcRowType);

            var useRow = row;

            if (available != null && conditional > 0) {
                useRow = srcTable.newRow();

                for (Map.Entry<String, TransformMaker.Availability> e : available.entrySet()) {
                    var avail = e.getValue();
                    if (avail == TransformMaker.Availability.ALWAYS || rnd.nextBoolean()) {
                        String name = e.getKey();
                        Method getter = srcRowType.getMethod(name);
                        Class<?> colType = getter.getReturnType();
                        Method setter = srcRowType.getMethod(name, colType);
                        setter.invoke(useRow, getter.invoke(row));
                    }
                }
            }

            MethodHandle transformer = makeTransformer(rnd, available, srcRowType, dstRowTypes);

            var result = (byte[][]) transformer.invoke(useRow, srcKey, srcValue);

            for (int i = 0; i < result.length; i += 2) {
                dstIndexes[i >> 1].store(null, result[i], result[i + 1]);
            }

            Class<?> rowClass = row.getClass();

            for (int i=0; i<dstTables.length; i++) {
                Object dstRow;
                try (RowScanner s = dstTables[i].newRowScanner(null)) {
                    dstRow = s.row();
                }

                Class<?> dstRowClass = dstRow.getClass();

                for (FuzzTest.Column c : columns) {
                    Method dstMethod;
                    try {
                        dstMethod = dstRowClass.getMethod(c.name);
                    } catch (NoSuchMethodException e) {
                        continue;
                    }
                    Method srcMethod = rowClass.getMethod(c.name);
                    Object expect = srcMethod.invoke(row);
                    Object actual = dstMethod.invoke(dstRow);
                    if (expect == null) {
                        assertNull(actual);
                    } else if (!expect.getClass().isArray()) {
                        assertEquals(c.name, expect, actual);
                    } else {
                        assertTrue(c.name, Objects.deepEquals(expect, actual));
                    }
                }
            }

            FuzzTest.truncateAndClose(srcIndex, srcTable);

            for (int i=0; i<dstTables.length; i++) {
                FuzzTest.truncateAndClose(dstIndexes[i], dstTables[i]);
            }
        }

        db.close();
    }

    /**
     * Returns 1 or more transformed types. The order of the column array is shuffled as a
     * side-effect, and the primary keys are altered.
     */
    private static Class[] transformedRowTypes(Random rnd, Class<?> srcRowType,
                                               FuzzTest.Column[] columns, int max)
    {
        var rowTypes = new Class[1 + rnd.nextInt(max)];

        for (int i=0; i<rowTypes.length; i++) {
            Collections.shuffle(Arrays.asList(columns), rnd);
            var subColumns = Arrays.copyOfRange(columns, 0, 1 + rnd.nextInt(columns.length));
            rowTypes[i] = FuzzTest.randomRowType(rnd, subColumns);
        }

        return rowTypes;
    }

    /**
     * @return column availability, which might be null if all are never available
     */
    private static Map<String, TransformMaker.Availability> availability
        (Random rnd, float always, float never, float conditional, Class<?> srcType)
    {
        Map<String, TransformMaker.Availability> available = null;

        if (always != 0 || conditional != 0) {
            RowInfo srcInfo = RowInfo.find(srcType);
            available = new HashMap<>();

            float sum = always + never + conditional;
            always = always / sum;
            if (conditional == 0) {
                never = 1.0f; // avoid possible rounding issue
            } else {
                never = never / sum + always;
            }

            for (String name : srcInfo.allColumns.keySet()) {
                TransformMaker.Availability avail;
                float n = rnd.nextFloat();
                if (n < always) {
                    avail = TransformMaker.Availability.ALWAYS;
                } else if (n < never) {
                    avail = TransformMaker.Availability.NEVER;
                } else {
                    avail = TransformMaker.Availability.CONDITIONAL;
                }
                available.put(name, avail);
            }
        }

        return available;
    }

    /**
     * Transformer method returns an array of alternating key/value pairs.
     *
     *    byte[][] _(Row row, byte[] key, byte[] value)
     */
    private static MethodHandle makeTransformer(Random rnd,
                                                Map<String, TransformMaker.Availability> available,
                                                Class<?> srcType, Class<?>... dstTypes)
        throws Throwable
    {
        var tm = new TransformMaker<>(srcType, null, available);

        for (var dstType : dstTypes) {
            RowInfo dstInfo = RowInfo.find(dstType);
            tm.addKeyTarget(dstInfo, 0, rnd.nextBoolean()); // randomly eager
            tm.addValueTarget(dstInfo, 1, rnd.nextBoolean()); // one byte for schema version
        }

        ClassMaker cm = RowInfo.find(srcType).rowGen().beginClassMaker(null, srcType, "trans");

        Class<?> rowClass = RowMaker.find(srcType);

        MethodType mt = MethodType.methodType(byte[][].class, rowClass, byte[].class, byte[].class);
        MethodMaker mm = cm.addMethod("_", mt).static_();

        var rowVar = mm.param(0);
        var keyVar = mm.param(1);
        var valueVar = mm.param(2);

        tm.begin(mm, rowVar, keyVar, valueVar, -1);

        var resultVar = mm.new_(byte[][].class, dstTypes.length << 1);

        for (int i=0; i<dstTypes.length; i++) {
            resultVar.aset(i << 1, tm.encodeKey(i));
            var dstValueVar = tm.encodeValue(i);
            dstValueVar.aset(0, 1); // schema version
            resultVar.aset((i << 1) + 1, dstValueVar);
        }

        mm.return_(resultVar);

        var lookup = cm.finishLookup();
        var clazz = lookup.lookupClass();

        return lookup.findStatic(clazz, "_", mt);
    }
}
