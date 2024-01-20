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

package org.cojen.tupl.table;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;

import java.lang.reflect.Method;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;

import java.util.concurrent.ThreadLocalRandom;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;

import org.cojen.tupl.*;

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
                doFuzz(seed, always, never, conditional);
            } catch (Throwable e) {
                var err = new AssertionError("seed: " + seed, e);
                err.printStackTrace(System.out);
                throw err;
            }
        }
    }

    private static void doFuzz(long seed, float always, float never, float conditional)
        throws Throwable
    {
        var rnd = new Random(seed);

        Database db = Database.open(new DatabaseConfig());

        for (int t=0; t<100; t++) {
            doFuzz(rnd, db, always, never, conditional);
        }

        db.close();
    }

    @SuppressWarnings("unchecked")
    private static void doFuzz(Random rnd, Database db,
                               float always, float never, float conditional)
        throws Throwable
    {
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

        MethodHandle transformer = makeTransformer
            (rnd, false, available, srcRowType, dstRowTypes);

        var kvPairs = (byte[][]) transformer.invoke(useRow, srcKey, srcValue);

        for (int i = 0; i < kvPairs.length; i += 2) {
            dstIndexes[i >> 1].store(null, kvPairs[i], kvPairs[i + 1]);
        }

        for (int i=0; i<dstTables.length; i++) {
            Object dstRow;
            try (Scanner s = dstTables[i].newScanner(null)) {
                dstRow = s.row();
            }
            verifyTransform(columns, row, dstRow);
        }

        FuzzTest.truncateAndClose(srcIndex, srcTable);

        for (int i=0; i<dstTables.length; i++) {
            FuzzTest.truncateAndClose(dstIndexes[i], dstTables[i]);
        }
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
     *
     * If diff is true:
     *
     *    DiffResult _(Row row, byte[] key, byte[] value, byte[] oldValue)
     */
    private static MethodHandle makeTransformer(Random rnd, boolean diff,
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

        MethodType mt;
        if (!diff) {
            mt = MethodType.methodType(byte[][].class, rowClass, byte[].class, byte[].class);
        } else {
            mt = MethodType.methodType
                (DiffResult.class, rowClass, byte[].class, byte[].class, byte[].class);
        }

        MethodMaker mm = cm.addMethod("_", mt).static_();

        var rowVar = mm.param(0);
        var keyVar = mm.param(1);
        var valueVar = mm.param(2);

        TransformMaker oldMaker = null;

        if (!diff) {
            tm.begin(mm, rowVar, keyVar, valueVar, -1);
        } else {
            oldMaker = tm.beginValueDiff(mm, rowVar, keyVar, valueVar, -1, mm.param(3));
        }

        var kvPairs = mm.new_(byte[][].class, dstTypes.length << 1);

        for (int i=0; i<dstTypes.length; i++) {
            int id = i << 1;
            kvPairs.aset(id, tm.encode(id));
            var dstValueVar = tm.encode(++id);
            dstValueVar.aset(0, 1); // schema version
            kvPairs.aset(id, dstValueVar);
        }

        if (!diff) {
            mm.return_(kvPairs);
        } else {
            var diffs = mm.new_(boolean[].class, dstTypes.length);
            for (int i=0; i<dstTypes.length; i++) {
                Label skip = mm.label();
                if (oldMaker.diffValueCheck(skip, i << 1, (i << 1) + 1)) {
                    diffs.aset(i, true);
                    skip.here();
                }
            }

            var oldKvPairs = mm.new_(byte[][].class, dstTypes.length << 1);

            for (int i=0; i<dstTypes.length; i++) {
                int id = i << 1;
                oldKvPairs.aset(id, oldMaker.encode(id));
                var dstValueVar = oldMaker.encode(++id);
                dstValueVar.aset(0, 1); // schema version
                oldKvPairs.aset(id, dstValueVar);
            }

            mm.return_(mm.new_(DiffResult.class, kvPairs, diffs, oldKvPairs));
        }

        var lookup = cm.finishLookup();
        var clazz = lookup.lookupClass();

        return lookup.findStatic(clazz, "_", mt);
    }

    @Test
    public void fuzzDiff() throws Throwable {
        // Test transformations where row column availability is always, never, or conditional.

        var tasks = new TestUtils.TestTask[4];
        for (int i=0; i<tasks.length; i++) {
            tasks[i] = TestUtils.startTestTask(() -> fuzzDiff(1));
        }
        for (var task : tasks) {
            task.join();
        }
    }

    /**
     * @param n number of test iterations (times 100)
     */
    private static void fuzzDiff(int n) {
        for (int i=0; i<n; i++) {
            long seed = ThreadLocalRandom.current().nextLong();
            try {
                doFuzzDiff(seed);
            } catch (Throwable e) {
                var err = new AssertionError("seed: " + seed, e);
                err.printStackTrace(System.out);
                throw err;
            }
        }
    }

    private static void doFuzzDiff(long seed) throws Throwable {
        var rnd = new Random(seed);

        Database db = Database.open(new DatabaseConfig());

        for (int t=0; t<100; t++) {
            doFuzzDiff(rnd, db);
        }

        db.close();
    }

    @SuppressWarnings("unchecked")
    private static void doFuzzDiff(Random rnd, Database db) throws Throwable {
        FuzzTest.Column[] columns = FuzzTest.randomColumns(rnd);
        Class<?> srcRowType = FuzzTest.randomRowType(rnd, columns);
        FuzzTest.Column[] srcColumns = FuzzTest.cloneColumns(columns);
        Class<?>[] dstRowTypes = transformedRowTypes(rnd, srcRowType, columns, 3);

        Map<String, TransformMaker.Availability> available = 
            availability(rnd, 1.0f, 1.0f, 0.0f, srcRowType);

        MethodHandle transformer = makeTransformer
            (rnd, true, available, srcRowType, dstRowTypes);

        Index srcIndex = db.openIndex(srcRowType.getName());
        Table srcTable = srcIndex.asTable(srcRowType);

        var dstIndexes = new Index[dstRowTypes.length];
        var dstTables = new Table[dstRowTypes.length];
        for (int i=0; i<dstRowTypes.length; i++) {
            dstIndexes[i] = db.openIndex(dstRowTypes[i].getName());
            dstTables[i] = dstIndexes[i].asTable(dstRowTypes[i]);
        }

        var row1 = srcTable.newRow();
        FuzzTest.fillInColumns(rnd, columns, row1);
        srcTable.store(null, row1);

        byte[] srcKey1, srcValue1;
        try (Cursor c = srcIndex.newCursor(null)) {
            c.first();
            srcKey1 = c.key();
            srcValue1 = c.value();
            c.delete();
        }

        var row2 = srcTable.newRow();
        Set<String> altered = FuzzTest.alterValueColumns(rnd, srcColumns, row1, row2);
        srcTable.store(null, row2);

        byte[] srcKey2, srcValue2;
        try (Cursor c = srcIndex.newCursor(null)) {
            c.first();
            srcKey2 = c.key();
            srcValue2 = c.value();
            c.delete();
        }

        assertTrue(Arrays.equals(srcKey1, srcKey2));

        if (altered.isEmpty()) {
            assertTrue(Arrays.equals(srcValue1, srcValue2));
        }

        var result = (DiffResult) transformer.invoke(row2, srcKey2, srcValue2, srcValue1);

        // Insert and verify the new transformed kvPairs.

        for (int i = 0; i < result.kvPairs.length; i += 2) {
            dstIndexes[i >> 1].store(null, result.kvPairs[i], result.kvPairs[i + 1]);
        }

        var dstRows2 = new Object[dstTables.length];

        for (int i=0; i<dstTables.length; i++) {
            Object dstRow;
            try (Scanner s = dstTables[i].newScanner(null)) {
                dstRow = s.row();
            }
            dstRows2[i] = dstRow;

            verifyTransform(columns, row2, dstRow);

            try (Cursor c = dstIndexes[i].newCursor(null)) {
                c.autoload(false);
                for (c.first(); c.key() != null; c.next()) {
                    c.delete();
                }
            }
        }

        // Store the old values and verify.

        for (int i = 0; i < result.oldKvPairs.length; i += 2) {
            dstIndexes[i >> 1].store(null, result.oldKvPairs[i], result.oldKvPairs[i + 1]);
        }

        var dstRows1 = new Object[dstTables.length];

        for (int i=0; i<dstTables.length; i++) {
            Object dstRow;
            try (Scanner s = dstTables[i].newScanner(null)) {
                dstRow = s.row();
            }
            dstRows1[i] = dstRow;
            verifyTransform(columns, row1, dstRow);
        }

        // Verify that the targets that changed were detected as different.

        for (int i=0; i<dstTables.length; i++) {
            boolean diff = result.diffs[i];
            if (diff) {
                assertNotEquals(dstRows2[i], dstRows1[i]);
            } else {
                assertEquals(dstRows2[i], dstRows1[i]);
            }
        }

        FuzzTest.truncateAndClose(srcIndex, srcTable);

        for (int i=0; i<dstTables.length; i++) {
            FuzzTest.truncateAndClose(dstIndexes[i], dstTables[i]);
        }
    }

    private static void verifyTransform(FuzzTest.Column[] columns, Object srcRow, Object dstRow)
        throws Exception
    {
        Class<?> srcRowClass = srcRow.getClass();
        Class<?> dstRowClass = dstRow.getClass();

        for (FuzzTest.Column c : columns) {
            Method dstMethod;
            try {
                dstMethod = dstRowClass.getMethod(c.name);
            } catch (NoSuchMethodException e) {
                continue;
            }
            Method srcMethod = srcRowClass.getMethod(c.name);
            Object expect = srcMethod.invoke(srcRow);
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

    public static record DiffResult(byte[][] kvPairs, boolean[] diffs, byte[][] oldKvPairs) {}
}
