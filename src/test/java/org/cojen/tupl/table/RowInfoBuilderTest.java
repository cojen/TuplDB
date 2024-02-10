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

package org.cojen.tupl.table;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.maker.ClassMaker;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class RowInfoBuilderTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RowInfoBuilderTest.class.getName());
    }

    @Test
    public void broken() throws Exception {
        try {
            var b = new RowInfoBuilder("xxx");

            b.addColumn("a", ColumnInfo.TYPE_INT);
            b.addColumn("a", ColumnInfo.TYPE_UTF8);

            b.addColumn("b", ColumnInfo.TYPE_INT, false, 20, 10);

            b.addColumn("c", ColumnInfo.TYPE_INT, true, 10, 20);

            b.addColumn("d", ColumnInfo.TYPE_UTF8, true, 10, 20);

            b.build();
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("duplicate column"));
            assertTrue(e.getMessage().contains("illegal automatic range"));
            assertTrue(e.getMessage().contains("at most one column can be automatic"));
            assertTrue(e.getMessage().contains("cannot be automatic"));
        }

        try {
            var b = new RowInfoBuilder("xxx");
            b.addColumn("a", ColumnInfo.TYPE_INT);
            b.addToPrimaryKey("b");
            b.build();
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("column that doesn't exist"));
        }

        try {
            var b = new RowInfoBuilder("xxx");
            b.addColumn("a", ColumnInfo.TYPE_INT);
            b.addColumn("b", ColumnInfo.TYPE_INT, false, 10, 20);
            b.addToPrimaryKey("a");
            b.build();
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("last in the primary key"));
        }
    }

    @Test
    public void basic() throws Exception {
        var b = new RowInfoBuilder("a.b.C");
        b.addColumn("a", ColumnInfo.TYPE_INT);
        b.addColumn("b", ColumnInfo.TYPE_UTF8 | ColumnInfo.TYPE_NULLABLE);
        b.addToPrimaryKey("-a");
        b.addToSecondaryIndex("+!b");

        RowInfo info = b.build();

        assertEquals(0, info.alternateKeys.size());
        assertEquals(1, info.secondaryIndexes.size());

        ClassMaker cm = ClassMaker.beginExplicit("a.b.C", null, new Object());
        Class<?> rowType = info.makeRowType(cm);

        RowInfo info2 = RowInfo.find(rowType);
        rowInfoCheck(info, info2);
    }

    @Test
    public void basic2() throws Exception {
        var b = new RowInfoBuilder("a.b.C");
        b.addColumn("a", ColumnInfo.TYPE_INT);
        b.addColumn("b", ColumnInfo.TYPE_UTF8 | ColumnInfo.TYPE_NULLABLE);
        b.addColumn("c", ColumnInfo.TYPE_BIG_INTEGER);
        b.addColumn("d", ColumnInfo.TYPE_LONG);

        b.addToPrimaryKey("-a");

        b.addToSecondaryIndex("b");
        b.addToSecondaryIndex("-a");
        b.finishSecondaryIndex();
        b.addToSecondaryIndex("c");

        b.addToAlternateKey("d");

        RowInfo info = b.build();

        assertEquals(1, info.alternateKeys.size());
        assertEquals(2, info.secondaryIndexes.size());

        ClassMaker cm = ClassMaker.beginExplicit("a.b.C", null, new Object());
        Class<?> rowType = info.makeRowType(cm);

        RowInfo info2 = RowInfo.find(rowType);
        rowInfoCheck(info, info2);
    }

    @Test
    public void basic3() throws Exception {
        var b = new RowInfoBuilder("a.b.C");
        b.addColumn("a", ColumnInfo.TYPE_INT, false, 10, 20);
        b.addColumn("b", ColumnInfo.TYPE_UTF8, true);
        b.addColumn("c", ColumnInfo.TYPE_INT);

        b.addToPrimaryKey("-a");

        b.addToAlternateKey("b");
        b.finishAlternateKey();
        b.addToAlternateKey("c");
        b.addToAlternateKey("b");

        RowInfo info = b.build();

        assertEquals(2, info.alternateKeys.size());
        assertEquals(0, info.secondaryIndexes.size());

        ClassMaker cm = ClassMaker.beginExplicit("a.b.C", null, new Object());
        Class<?> rowType = info.makeRowType(cm);

        RowInfo info2 = RowInfo.find(rowType);
        rowInfoCheck(info, info2);
    }

    private static void rowInfoCheck(RowInfo info, RowInfo info2) {
        assertNotSame(info, info2);
        assertEquals(info.keyColumns, info2.keyColumns);
        assertEquals(info.valueColumns, info2.valueColumns);
        assertEquals(info.allColumns, info2.allColumns);
        assertEquals(info.alternateKeys, info2.alternateKeys);
        assertEquals(info.secondaryIndexes, info2.secondaryIndexes);
    }
}
