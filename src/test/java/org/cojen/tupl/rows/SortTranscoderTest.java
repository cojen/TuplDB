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

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.core.CoreDatabase;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SortTranscoderTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SortTranscoderTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mDb = (CoreDatabase) Database.open(new DatabaseConfig());
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    private CoreDatabase mDb;

    @Test
    public void permute() throws Exception {
        Index ix = mDb.openIndex("test");
        Table<TestRow> table = ix.asTable(TestRow.class);

        fill(table);

        permuteSpecs(spec -> {
            var transcoder = newTranscoder(ix, table, null, spec, null);
            Index sorted = sort(ix, transcoder);
            verifyOrder(table, spec, transcoder, sorted);
            mDb.deleteIndex(sorted);
        }, "id", "name", "status");
    }

    @Test
    public void distinct() throws Exception {
        Index ix = mDb.openIndex("test");
        Table<TestRow> table = ix.asTable(TestRow.class);

        fill(table);

        var expect = new TreeSet<Integer>();
        try (var s = table.newRowScanner(null, "{status}")) {
            for (var row = s.row(); row != null; row = s.step(row)) {
                expect.add(row.status());
            }
        }

        var projection = Set.of("status");
        var transcoder = newTranscoder(ix, table, projection, "+status", projection);
        Index sorted = sort(ix, transcoder);

        Iterator<Integer> expectIt = expect.iterator();

        try (Cursor c = sorted.newCursor(null)) {
            TestRow row = table.newRow();
            for (c.first(); c.key() != null; c.next()) {
                transcoder.decodeRow(row, c.key(), c.value());

                assertEquals((Integer) row.status(), expectIt.next());

                try {
                    row.id();
                    fail();
                } catch (UnsetColumnException e) {
                }

                try {
                    row.name();
                    fail();
                } catch (UnsetColumnException e) {
                }
            }
        }

        assertFalse(expectIt.hasNext());
    }

    @Test
    public void duplicates() throws Exception {
        Index ix = mDb.openIndex("test");
        Table<TestRow> table = ix.asTable(TestRow.class);

        int num = fill(table);

        var projection = Set.of("status");
        var transcoder = newTranscoder(ix, table, null, "+status", projection);
        Index sorted = sort(ix, transcoder);

        int count = 0;
        int lastStatus = -1;
        boolean duplicates = false;

        try (Cursor c = sorted.newCursor(null)) {
            TestRow row = table.newRow();
            for (c.first(); c.key() != null; c.next()) {
                count++;
                transcoder.decodeRow(row, c.key(), c.value());

                if (lastStatus >= 0) {
                    assertTrue(row.status() >= lastStatus);
                    duplicates |= row.status() == lastStatus;
                }

                lastStatus = row.status();

                try {
                    row.id();
                    fail();
                } catch (UnsetColumnException e) {
                }

                try {
                    row.name();
                    fail();
                } catch (UnsetColumnException e) {
                }
            }
        }

        assertEquals(num, count);
        assertTrue(duplicates);
    }

    @Test
    public void secondary() throws Exception {
        final Index ix = mDb.openIndex("test");
        final Table<TestRow> table = ix.asTable(TestRow.class);

        fill(table);

        Table<TestRow> statusTable = table.viewSecondaryIndex("status").viewUnjoined();
        Index statusIx = ((BaseTable) statusTable).mSource;

        Class<TestRow> rowType = table.rowType();
        RowInfo primaryInfo = RowInfo.find(rowType);
        ColumnSet secondaryColumns = primaryInfo.secondaryIndexes.iterator().next();
        byte[] secondaryDesc = RowStore.secondaryDescriptor(secondaryColumns, false);
        SecondaryInfo secondaryInfo = RowStore.secondaryRowInfo(primaryInfo, secondaryDesc);
        OrderBy orderBy = OrderBy.forSpec(secondaryInfo, "+id");
        var transcoder = new SortTranscoderMaker<TestRow>
            (mDb.rowStore(), rowType, table.newRow().getClass(), secondaryInfo, ix.id(),
             null, orderBy, null).finish();

        Index sorted = sort(statusIx, transcoder);

        try (RowScanner<TestRow> s = table.viewPrimaryKey().newRowScanner(null, "{id, status}")) {
            try (Cursor c = sorted.newCursor(null)) {
                TestRow row = table.newRow();
                for (c.first(); c.key() != null; c.next()) {
                    transcoder.decodeRow(row, c.key(), c.value());

                    TestRow expect = s.row();
                    s.step();

                    assertEquals(expect, row);
                }
            }

            assertNull(s.row());
        }
    }

    private int fill(Table<TestRow> table) throws Exception {
        final int num = 10;

        for (int i=0; i<num; i++) {
            var row = table.newRow();
            row.id(i);

            if (i != 1) {
                row.name("name-" + (i ^ 0b101010));
            } else {
                row.name(null);
            }

            row.status((i % 7) ^ 0b010101);

            table.store(null, row);
        }

        return num;
    }

    private <R> void dump(Table<R> table) throws Exception {
        try (var s = table.newRowScanner(null)) {
            for (var row = s.row(); row != null; row = s.step(row)) {
                System.out.println(row);
            }
        }
    }
    private <R> void dump(Table<R> table, Index sorted, Transcoder<R> transcoder) throws Exception {
        try (Cursor c = sorted.newCursor(null)) {
            R row = table.newRow();
            for (c.first(); c.key() != null; c.next()) {
                transcoder.decodeRow(row, c.key(), c.value());
                System.out.println(row);
            }
        }
    }

    /**
     * @param available columns available in the source rows; can pass null if all are available
     * @param orderBySpec ordering specification
     * @param projection columns to keep; can pass null to project all available columns
     */
    private <R> Transcoder<R> newTranscoder(Index ix, Table<R> table,
                                            Set<String> available, String orderBySpec,
                                            Set<String> projection)
        throws Exception
    {
        return newMaker(ix, table, available, orderBySpec, projection).finish();
    }

    /**
     * @param available columns available in the source rows; can pass null if all are available
     * @param orderBySpec ordering specification
     * @param projection columns to keep; can pass null to project all available columns
     */
    private <R> SortTranscoderMaker<R> newMaker(Index ix, Table<R> table,
                                                Set<String> available, String orderBySpec,
                                                Set<String> projection)
        throws Exception
    {
        Class<R> rowType = table.rowType();
        RowInfo info = RowInfo.find(rowType);
        OrderBy orderBy = OrderBy.forSpec(info, orderBySpec);
        return new SortTranscoderMaker<R>
            (mDb.rowStore(), rowType, table.newRow().getClass(), info, ix.id(),
             available, orderBy, projection);
    }

    private Index sort(Index ix, Transcoder<?> transcoder) throws Exception {
        return fillSorter(ix, transcoder).finish();
    }

    private Sorter fillSorter(Index ix, Transcoder<?> transcoder) throws Exception {
        var sorter = mDb.newSorter();

        var kvPairs = new byte[10][];
        int num = 0;

        try (Cursor c = ix.newCursor(null)) {
            for (c.first(); c.key() != null; c.next()) {
                if ((num << 1) >= kvPairs.length) {
                    sorter.addBatch(kvPairs, 0, num);
                    num = 0;
                }
                transcoder.transcode(c.key(), c.value(), kvPairs, num << 1);
                num++;
            }
        }

        if (num != 0) {
            sorter.addBatch(kvPairs, 0, num);
        }

        return sorter;
    }

    private <R> Comparator<R> newComparator(Table<R> table, String orderBySpec) {
        return new ComparatorMaker<R>(table.rowType(), orderBySpec).finish();
    }

    private <R> void verifyOrder(Table<R> table, String orderBySpec,
                                 Transcoder<R> transcoder, Index sorted)
        throws Exception
    {
        var comparator = newComparator(table, orderBySpec);

        R lastRow = null;

        try (Cursor c = sorted.newCursor(null)) {
            for (c.first(); c.key() != null; c.next()) {
                R row = table.newRow();
                transcoder.decodeRow(row, c.key(), c.value());

                if (lastRow != null) {
                    int cmp = comparator.compare(lastRow, row);
                    assertTrue(cmp <= 0);
                }

                lastRow = row;
            }
        }
    }

    @FunctionalInterface
    static interface SpecConsumer {
        void accept(String spec) throws Exception;
    }

    private void permuteSpecs(SpecConsumer consumer, String... names) throws Exception {
        var selections = new int[names.length];
        int maxSelection = names.length * 5;

        var specs = new HashSet<String>();

        outer: while (true) {
            var selectedNames = new HashSet<String>();
            var b = new StringBuilder();

            for (int i=0; i<selections.length; i++) {
                int selection = selections[i];
                int mode = selection % 5;
                if (mode == 0) {
                    continue;
                }
                String name = names[selection / 5];
                if (selectedNames.contains(name)) {
                    continue;
                }
                selectedNames.add(name);
                b.append(switch (mode) {
                    default -> "+"; case 2 -> "-"; case 3 -> "+!"; case 4 -> "-!";
                });
                b.append(name);
            }

            if (!b.isEmpty()) {
                String spec = b.toString();
                if (!specs.contains(spec)) {
                    specs.add(spec);
                    consumer.accept(spec);
                }
            }

            int pos = 0;
            while (true) {
                if (++selections[pos] < maxSelection) {
                    break;
                }
                selections[pos] = 0;
                pos++;
                if (pos >= selections.length) {
                    break outer;
                }
            }
        }
    }

    @PrimaryKey("id")
    @SecondaryIndex("status")
    public static interface TestRow {
        int id();
        void id(int id);

        @Nullable
        String name();
        void name(String n);

        int status();
        void status(int s);
    }
}
