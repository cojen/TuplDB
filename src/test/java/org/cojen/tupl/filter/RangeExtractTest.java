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

import java.util.Map;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.rows.ColumnInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RangeExtractTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RangeExtractTest.class.getName());
    }

    @Test
    public void mix() throws Throwable {
        Map<String, ColumnInfo> colMap = NormalizeTest.newColMap(2);

        RowFilter f0 = new Parser(colMap, "a==?&&(b>?||b>?)&&(b>?||b>?)&&(b>?||b>?)").parse();
        RowFilter[] range = f0.cnf().rangeExtract(colMap.get("a"), colMap.get("b"));

        check(range, new String[] {
            "(b > ?1 || b > ?2) && (b > ?3 || b > ?4) && (b > ?5 || b > ?6)",
            "a >= ?0", "a <= ?0"
        });

        RowFilter f1 = f0.dnf();
        RowFilter[][] ranges = f1.multiRangeExtract
            (false, false, colMap.get("a"), colMap.get("b"));

        check(ranges, new String[][] {
            {"(b > ?4 || b > ?3) && (b > ?6 || b > ?5)",
             "a == ?0 && b > ?1", "a <= ?0"},
            {"(b > ?4 || b > ?3) && (b > ?6 || b > ?5)",
             "a == ?0 && b > ?2", "a <= ?0"}
        });

        ranges = f1.multiRangeExtract(true, false, colMap.get("a"), colMap.get("b"));

        check(ranges, new String[][] {
            {"(b > ?4 || b > ?3) && (b > ?6 || b > ?5)",
             "a == ?0 && b > ?1", "a <= ?0"},
            {"(b > ?4 || b > ?3) && (b > ?6 || b > ?5)",
             "a == ?0 && b > ?2", "a == ?0 && b <= ?1"}
        });

        // Switch the direction of a few operators.
        RowFilter f2 = new Parser(colMap, "a==?&&(b<?||b<?)&&(b>?||b>?)&&(b>?||b>?)").parse();
        range = f2.cnf().rangeExtract(colMap.get("a"), colMap.get("b"));

        check(range, new String[] {
            "(b < ?1 || b < ?2) && (b > ?3 || b > ?4) && (b > ?5 || b > ?6)",
            "a >= ?0", "a <= ?0"
        });

        RowFilter f3 = f2.dnf();
        ranges = f3.multiRangeExtract(false, true, colMap.get("a"), colMap.get("b"));

        check(ranges, new String[][] {
            {"(b > ?4 || b > ?3) && (b > ?6 || b > ?5)",
             "a >= ?0", "a == ?0 && b < ?1"},
            {"(b > ?4 || b > ?3) && (b > ?6 || b > ?5)",
             "a >= ?0", "a == ?0 && b < ?2"}
        });

        ranges = f3.multiRangeExtract(true, true, colMap.get("a"), colMap.get("b"));

        check(ranges, new String[][] {
            {"(b > ?4 || b > ?3) && (b > ?6 || b > ?5)",
             "a >= ?0", "a == ?0 && b < ?1"},
            {"(b > ?4 || b > ?3) && (b > ?6 || b > ?5)",
             "a == ?0 && b >= ?1", "a == ?0 && b < ?2"}
        });
    }

    private static void check(RowFilter[] range, String[] expect) {
        assertEquals(expect.length, range.length);
        for (int i=0; i<expect.length; i++) {
            assertEquals(expect[i], range[i].toString());
        }
    }

    private static void check(RowFilter[][] ranges, String[][] expectedRanges) {
        assertEquals(expectedRanges.length, ranges.length);
        for (int i=0; i<expectedRanges.length; i++) {
            //System.out.println(java.util.Arrays.toString(ranges[i]));
            check(ranges[i], expectedRanges[i]);
        }
    }
}
