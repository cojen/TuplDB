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

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.core.LocalDatabase;

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
        mDb = (LocalDatabase) Database.open(new DatabaseConfig());
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    private LocalDatabase mDb;

    @Test
    public void permute() throws Exception {
        Index ix = mDb.openIndex("test");
        Table<TestRow> table = ix.asTable(TestRow.class);

        fill(table);

        permuteSpecs(spec -> {
            var td = newTranscoder(ix, table, null, spec, null, true);
            Index sorted = sort(ix, td);
            verifyOrder(table, spec, td, sorted);
            mDb.deleteIndex(sorted);
        }, "id", "name", "status");
    }

    @Test
    public void distinct() throws Exception {
        Index ix = mDb.openIndex("test");
        Table<TestRow> table = ix.asTable(TestRow.class);

        fill(table);

        var expect = new TreeSet<Integer>();
        try (var s = table.newScanner(null, "{status}")) {
            for (var row = s.row(); row != null; row = s.step(row)) {
                expect.add(row.status());
            }
        }

        var projection = Set.of("status");
        var td = newTranscoder(ix, table, null, "+status", projection, false);
        Index sorted = sort(ix, td);

        Iterator<Integer> expectIt = expect.iterator();

        try (Cursor c = sorted.newCursor(null)) {
            TestRow row = table.newRow();
            for (c.first(); c.key() != null; c.next()) {
                td.decoder.decodeRow(row, c.key(), c.value());

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
        var td = newTranscoder(ix, table, null, "+status", projection, true);
        Index sorted = sort(ix, td);

        int count = 0;
        int lastStatus = -1;
        boolean duplicates = false;

        try (Cursor c = sorted.newCursor(null)) {
            TestRow row = table.newRow();
            for (c.first(); c.key() != null; c.next()) {
                count++;
                td.decoder.decodeRow(row, c.key(), c.value());

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
        final var table = (StoredTable<TestRow>) ix.asTable(TestRow.class);

        fill(table);

        Table<TestRow> statusTable = table.viewSecondaryIndex("status").viewUnjoined();
        Index statusIx = ((StoredTable) statusTable).mSource;

        Class<TestRow> rowType = table.rowType();
        RowInfo primaryInfo = RowInfo.find(rowType);
        ColumnSet secondaryColumns = primaryInfo.secondaryIndexes.iterator().next();
        byte[] secondaryDesc = RowStore.secondaryDescriptor(secondaryColumns, false);
        SecondaryInfo secondaryInfo = RowStore.secondaryRowInfo(primaryInfo, secondaryDesc);
        Set<String> projection = Set.of("id", "status");
        var td = newTranscoder(ix, table, secondaryInfo, "+id", projection, true);

        Index sorted = sort(statusIx, td);

        try (Scanner<TestRow> s = table.newScanner(null, "{+id, status}")) {
            try (Cursor c = sorted.newCursor(null)) {
                TestRow row = table.newRow();
                for (c.first(); c.key() != null; c.next()) {
                    td.decoder.decodeRow(row, c.key(), c.value());

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
        try (var s = table.newScanner(null)) {
            for (var row = s.row(); row != null; row = s.step(row)) {
                System.out.println(row);
            }
        }
    }
    private <R> void dump(Table<R> table, Index sorted, TD<R> td) throws Exception {
        try (Cursor c = sorted.newCursor(null)) {
            R row = table.newRow();
            for (c.first(); c.key() != null; c.next()) {
                td.decoder.decodeRow(row, c.key(), c.value());
                System.out.println(row);
            }
        }
    }

    private record TD<R>(Transcoder transcoder, RowDecoder<R> decoder) { };

    /**
     * @param rowInfo can pass null for primary row
     * @param orderBySpec ordering specification
     * @param projection columns to keep; can pass null to project all available columns
     */
    private <R> TD<R> newTranscoder(Index primaryIndex, Table<R> table, RowInfo rowInfo,
                                    String orderBySpec, Set<String> projection,
                                    boolean allowDuplicates)
        throws Exception
    {
        Class<R> rowType = table.rowType();

        SecondaryInfo sortedInfo = SortDecoderMaker.findSortedInfo
            (rowType, orderBySpec, projection, allowDuplicates);

        RowDecoder<R> decoder = SortDecoderMaker.findDecoder(rowType, sortedInfo, projection);

        if (rowInfo == null) {
            rowInfo = RowInfo.find(rowType);
        }

        Transcoder transcoder = SortTranscoderMaker.makeTranscoder
            (mDb.rowStore(), rowType, rowInfo, primaryIndex.id(), sortedInfo);

        return new TD<R>(transcoder, decoder);
    }

    private Index sort(Index ix, TD td) throws Exception {
        return fillSorter(ix, td).finish();
    }

    private Sorter fillSorter(Index ix, TD td) throws Exception {
        var sorter = mDb.newSorter();

        var kvPairs = new byte[10][];
        int num = 0;

        try (Cursor c = ix.newCursor(null)) {
            for (c.first(); c.key() != null; c.next()) {
                if ((num << 1) >= kvPairs.length) {
                    sorter.addBatch(kvPairs, 0, num);
                    num = 0;
                }
                td.transcoder.transcode(c.key(), c.value(), kvPairs, num << 1);
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
                                 TD<R> td, Index sorted)
        throws Exception
    {
        var comparator = newComparator(table, orderBySpec);

        R lastRow = null;

        try (Cursor c = sorted.newCursor(null)) {
            for (c.first(); c.key() != null; c.next()) {
                R row = table.newRow();
                td.decoder.decodeRow(row, c.key(), c.value());

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
