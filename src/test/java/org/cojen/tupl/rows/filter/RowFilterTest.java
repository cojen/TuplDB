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

package org.cojen.tupl.rows.filter;

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

        for (int n = 'a'; n <= 'd'; n++) {
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
            RowFilter f1 = new Parser(colMap, "(a == ? || (b != ? && a == ?)) && (c == ?)")
                .parseFilter();

            RowFilter f2 = f1.prioritize(subMap(colMap, "a"));
            assertEquals("(a == ?1 || (a == ?3 && b != ?2)) && c == ?4", f2.toString());

            RowFilter f3 = f1.prioritize(subMap(colMap, "b"));
            assertEquals("((b != ?2 && a == ?3) || a == ?1) && c == ?4", f3.toString());

            RowFilter f4 = f1.prioritize(subMap(colMap, "c"));
            assertEquals("c == ?4 && (a == ?1 || (b != ?2 && a == ?3))", f4.toString());

            RowFilter f5 = f1.prioritize(subMap(colMap, "a", "b"));
            assertEquals("(a == ?1 || (b != ?2 && a == ?3)) && c == ?4", f5.toString());

            RowFilter f6 = f1.prioritize(subMap(colMap, "a", "c"));
            assertEquals("c == ?4 && (a == ?1 || (a == ?3 && b != ?2))", f6.toString());

            RowFilter f7 = f1.prioritize(subMap(colMap, "b", "c"));
            assertEquals("c == ?4 && ((b != ?2 && a == ?3) || a == ?1)", f7.toString());
        }

        {
            RowFilter f1 = new Parser(colMap, "((a == ? && b != ?) || a == ?) && (c == ?)")
                .parseFilter();

            RowFilter f2 = f1.prioritize(subMap(colMap, "a"));
            assertEquals("(a == ?3 || (a == ?1 && b != ?2)) && c == ?4", f2.toString());

            RowFilter f3 = f1.prioritize(subMap(colMap, "b"));
            assertEquals("((b != ?2 && a == ?1) || a == ?3) && c == ?4", f3.toString());

            RowFilter f4 = f1.prioritize(subMap(colMap, "c"));
            assertEquals("c == ?4 && ((a == ?1 && b != ?2) || a == ?3)", f4.toString());

            RowFilter f5 = f1.prioritize(subMap(colMap, "a", "b"));
            assertEquals("((a == ?1 && b != ?2) || a == ?3) && c == ?4", f5.toString());

            RowFilter f6 = f1.prioritize(subMap(colMap, "a", "c"));
            assertEquals("c == ?4 && (a == ?3 || (a == ?1 && b != ?2))", f6.toString());

            RowFilter f7 = f1.prioritize(subMap(colMap, "b", "c"));
            assertEquals("c == ?4 && ((b != ?2 && a == ?1) || a == ?3)", f7.toString());
        }
    }

    @Test
    public void retain() {
        var colMap = newColMap();

        {
            RowFilter f1 = new Parser(colMap, "a < ?").parseFilter();

            RowFilter f2 = f1.retain(subMap(colMap, "a"), true, TrueFilter.THE);
            assertEquals(f1, f2);

            RowFilter f3 = f1.retain(subMap(colMap, "b"), true, TrueFilter.THE);
            assertEquals(TrueFilter.THE, f3);

            RowFilter f4 = f1.retain(subMap(colMap, "b"), true, FalseFilter.THE);
            assertEquals(FalseFilter.THE, f4);

            RowFilter f5 = f1.retain(subMap(colMap), true, TrueFilter.THE);
            assertEquals(TrueFilter.THE, f5);
        }

        {
            RowFilter f1 = new Parser(colMap, "(a == ? || (b == ? && a != ?)) && (c == ?)")
                .parseFilter();

            RowFilter f2 = f1.retain(subMap(colMap, "a", "b", "c"), true, TrueFilter.THE);
            assertEquals(f1, f2);

            RowFilter f3 = f1.retain(subMap(colMap, "b", "c"), true, TrueFilter.THE);
            assertEquals("c == ?4", f3.toString());
            
            RowFilter f4 = f1.retain(subMap(colMap, "a", "c"), true, TrueFilter.THE);
            assertEquals("(a == ?1 || a != ?3) && c == ?4", f4.toString());

            RowFilter f5 = f1.retain(subMap(colMap, "a", "b"), true, TrueFilter.THE);
            assertEquals("a == ?1 || (b == ?2 && a != ?3)", f5.toString());

            RowFilter f6 = f1.retain(subMap(colMap, "a"), true, TrueFilter.THE);
            assertEquals("a == ?1 || a != ?3", f6.toString());

            RowFilter f7 = f1.retain(subMap(colMap, "b"), true, TrueFilter.THE);
            assertEquals(TrueFilter.THE, f7);

            RowFilter f8 = f1.retain(subMap(colMap, "c"), true, TrueFilter.THE);
            assertEquals("c == ?4", f8.toString());
        }

        {
            RowFilter f1 = new Parser(colMap, "(a == ? && (b == ? || a != ?)) || (c == ?)")
                .parseFilter();

            RowFilter f3 = f1.retain(subMap(colMap, "b", "c"), true, TrueFilter.THE);
            assertEquals(TrueFilter.THE, f3);
            
            RowFilter f4 = f1.retain(subMap(colMap, "a", "c"), true, TrueFilter.THE);
            assertEquals("a == ?1 || c == ?4", f4.toString());

            RowFilter f5 = f1.retain(subMap(colMap, "a", "b"), true, TrueFilter.THE);
            assertEquals(TrueFilter.THE, f5);

            RowFilter f6 = f1.retain(subMap(colMap, "a"), true, TrueFilter.THE);
            assertEquals(TrueFilter.THE, f6);

            RowFilter f7 = f1.retain(subMap(colMap, "b"), true, TrueFilter.THE);
            assertEquals(TrueFilter.THE, f7);

            RowFilter f8 = f1.retain(subMap(colMap, "c"), true, TrueFilter.THE);
            assertEquals(TrueFilter.THE, f8);
        }

        {
            RowFilter f1 = new Parser(colMap, "(a == ? && b == a) || c == ?").parseFilter();
            RowFilter f2 = f1.retain(subMap(colMap, "b", "c"), false, TrueFilter.THE);
            assertEquals("b == a || c == ?2", f2.toString());
        }
    }

    @Test
    public void split() {
        var colMap = newColMap();

        {
            RowFilter f0 = new Parser(colMap, "(a==?&&b==?&&c==?&&d==?)||a==?").parseFilter();
            RowFilter f1 = f0.cnf();

            RowFilter[] result = f1.split(subMap(colMap, "a"));
            assertEquals("a == ?1 || a == ?5", result[0].toString());
            assertEquals("(b == ?2 || a == ?5) && (c == ?3 || a == ?5) && (d == ?4 || a == ?5)",
                         result[1].toString());

            result = f1.split(subMap(colMap, "b"));
            assertEquals(TrueFilter.THE, result[0]);
            assertEquals(f0, result[1].reduceMore());

            result = f1.split(subMap(colMap, "a", "b"));
            assertEquals("(a == ?1 || a == ?5) && (b == ?2 || a == ?5)", result[0].toString());
            assertEquals("(c == ?3 || a == ?5) && (d == ?4 || a == ?5)", result[1].toString());
            assertEquals("(a == ?1 && b == ?2) || a == ?5", result[0].reduceMore().toString());
            assertEquals("(c == ?3 && d == ?4) || a == ?5", result[1].reduceMore().toString());

            result = f1.split(subMap(colMap, "a", "b", "c"));
            assertEquals("(a == ?1 && b == ?2 && c == ?3) || a == ?5",
                         result[0].reduceMore().toString());
            assertEquals("d == ?4 || a == ?5", result[1].reduceMore().toString());

            result = f1.split(colMap);
            assertEquals(f1, result[0]);
            assertEquals(TrueFilter.THE, result[1]);
        }

        {
            RowFilter f0 = new Parser(colMap, "a == b").parseFilter();

            RowFilter[] result = f0.split(subMap(colMap, "a"));
            assertEquals(TrueFilter.THE, result[0]);
            assertEquals(f0, result[1]);

            result = f0.split(subMap(colMap, "a", "b"));
            assertEquals(f0, result[0]);
            assertEquals(TrueFilter.THE, result[1]);
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
