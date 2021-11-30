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

package org.cojen.tupl.filter;

import java.util.HashMap;
import java.util.Map;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.rows.ColumnInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RowFilterTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RowFilterTest.class.getName());
    }

    private static HashMap<String, ColumnInfo> newColMap() {
        var colMap = new HashMap<String, ColumnInfo>();

        for (int n = 'a'; n <= 'c'; n++) {
            var info = new ColumnInfo();
            info.name = String.valueOf((char) n);
            info.typeCode = ColumnInfo.TYPE_UTF8;
            info.assignType();
            colMap.put(info.name, info);
        }

        return colMap;
    }

    @Test
    public void prioritize() {
        var colMap = newColMap();

        {
            RowFilter f1 = new Parser(colMap, "(a == ? || (b != ? && a == ?)) && (c == ?)").parse();

            RowFilter f2 = f1.prioritize(subMap(colMap, "a"));
            assertEquals("(a == ?0 || (a == ?2 && b != ?1)) && c == ?3", f2.toString());

            RowFilter f3 = f1.prioritize(subMap(colMap, "b"));
            assertEquals("((b != ?1 && a == ?2) || a == ?0) && c == ?3", f3.toString());

            RowFilter f4 = f1.prioritize(subMap(colMap, "c"));
            assertEquals("c == ?3 && (a == ?0 || (b != ?1 && a == ?2))", f4.toString());

            RowFilter f5 = f1.prioritize(subMap(colMap, "a", "b"));
            assertEquals("(a == ?0 || (b != ?1 && a == ?2)) && c == ?3", f5.toString());

            RowFilter f6 = f1.prioritize(subMap(colMap, "a", "c"));
            assertEquals("c == ?3 && (a == ?0 || (a == ?2 && b != ?1))", f6.toString());

            RowFilter f7 = f1.prioritize(subMap(colMap, "b", "c"));
            assertEquals("c == ?3 && ((b != ?1 && a == ?2) || a == ?0)", f7.toString());
        }

        {
            RowFilter f1 = new Parser(colMap, "((a == ? && b != ?) || a == ?) && (c == ?)").parse();

            RowFilter f2 = f1.prioritize(subMap(colMap, "a"));
            assertEquals("(a == ?2 || (a == ?0 && b != ?1)) && c == ?3", f2.toString());

            RowFilter f3 = f1.prioritize(subMap(colMap, "b"));
            assertEquals("((b != ?1 && a == ?0) || a == ?2) && c == ?3", f3.toString());

            RowFilter f4 = f1.prioritize(subMap(colMap, "c"));
            assertEquals("c == ?3 && ((a == ?0 && b != ?1) || a == ?2)", f4.toString());

            RowFilter f5 = f1.prioritize(subMap(colMap, "a", "b"));
            assertEquals("((a == ?0 && b != ?1) || a == ?2) && c == ?3", f5.toString());

            RowFilter f6 = f1.prioritize(subMap(colMap, "a", "c"));
            assertEquals("c == ?3 && (a == ?2 || (a == ?0 && b != ?1))", f6.toString());

            RowFilter f7 = f1.prioritize(subMap(colMap, "b", "c"));
            assertEquals("c == ?3 && ((b != ?1 && a == ?0) || a == ?2)", f7.toString());
        }
    }

    @Test
    public void retain() {
        var colMap = newColMap();

        {
            RowFilter f1 = new Parser(colMap, "a < ?").parse();

            RowFilter f2 = f1.retain(subMap(colMap, "a"), TrueFilter.THE);
            assertEquals(f1, f2);

            RowFilter f3 = f1.retain(subMap(colMap, "b"), TrueFilter.THE);
            assertEquals(TrueFilter.THE, f3);

            RowFilter f4 = f1.retain(subMap(colMap, "b"), FalseFilter.THE);
            assertEquals(FalseFilter.THE, f4);

            RowFilter f5 = f1.retain(subMap(colMap), TrueFilter.THE);
            assertEquals(TrueFilter.THE, f3);
        }

        {
            RowFilter f1 = new Parser(colMap, "(a == ? || (b == ? && a != ?)) && (c == ?)").parse();

            RowFilter f2 = f1.retain(subMap(colMap, "a", "b", "c"), TrueFilter.THE);
            assertEquals(f1, f2);

            RowFilter f3 = f1.retain(subMap(colMap, "b", "c"), TrueFilter.THE);
            assertEquals("c == ?3", f3.toString());
            
            RowFilter f4 = f1.retain(subMap(colMap, "a", "c"), TrueFilter.THE);
            assertEquals("(a == ?0 || a != ?2) && c == ?3", f4.toString());

            RowFilter f5 = f1.retain(subMap(colMap, "a", "b"), TrueFilter.THE);
            assertEquals("a == ?0 || (b == ?1 && a != ?2)", f5.toString());

            RowFilter f6 = f1.retain(subMap(colMap, "a"), TrueFilter.THE);
            assertEquals("a == ?0 || a != ?2", f6.toString());

            RowFilter f7 = f1.retain(subMap(colMap, "b"), TrueFilter.THE);
            assertEquals(TrueFilter.THE, f7);

            RowFilter f8 = f1.retain(subMap(colMap, "c"), TrueFilter.THE);
            assertEquals("c == ?3", f8.toString());
        }

        {
            RowFilter f1 = new Parser(colMap, "(a == ? && (b == ? || a != ?)) || (c == ?)").parse();

            RowFilter f3 = f1.retain(subMap(colMap, "b", "c"), TrueFilter.THE);
            assertEquals(TrueFilter.THE, f3);
            
            RowFilter f4 = f1.retain(subMap(colMap, "a", "c"), TrueFilter.THE);
            assertEquals("a == ?0 || c == ?3", f4.toString());

            RowFilter f5 = f1.retain(subMap(colMap, "a", "b"), TrueFilter.THE);
            assertEquals(TrueFilter.THE, f5);

            RowFilter f6 = f1.retain(subMap(colMap, "a"), TrueFilter.THE);
            assertEquals(TrueFilter.THE, f6);

            RowFilter f7 = f1.retain(subMap(colMap, "b"), TrueFilter.THE);
            assertEquals(TrueFilter.THE, f7);

            RowFilter f8 = f1.retain(subMap(colMap, "c"), TrueFilter.THE);
            assertEquals(TrueFilter.THE, f8);
        }
    }

    private static Map<String, ColumnInfo> subMap(Map<String, ColumnInfo> colMap, String... names) {
        var subMap = new HashMap<String, ColumnInfo>(names.length);
        for (String name : names) {
            subMap.put(name, colMap.get(name));
        }
        return subMap;
    }
}
