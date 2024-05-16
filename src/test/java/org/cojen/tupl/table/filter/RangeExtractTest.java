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

package org.cojen.tupl.table.filter;

import java.util.Arrays;
import java.util.Map;
import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.table.ColumnInfo;

import static org.cojen.tupl.table.filter.ColumnFilter.*;

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
    public void mix() throws Exception {
        Map<String, ColumnInfo> colMap = NormalizeTest.newColMap(2);

        RowFilter f0 = parse(colMap, "a==?&&(b>?||b>?)&&(b>?||b>?)&&(b>?||b>?)");
        RowFilter[] range = f0.cnf().rangeExtract(colMap.get("a"), colMap.get("b"));

        check(range, new String[] {
            "a >= ?1", "a <= ?1",
            "(b > ?2 || b > ?3) && (b > ?4 || b > ?5) && (b > ?6 || b > ?7)",
            null
        });

        RowFilter f1 = f0.dnf();
        RowFilter[][] ranges = f1.multiRangeExtract
            (false, false, colMap.get("a"), colMap.get("b"));

        check(ranges, new String[][] {
            {"a == ?1 && b > ?2", "a <= ?1",
             "(b > ?5 || b > ?4) && (b > ?7 || b > ?6)", null},
            {"a == ?1 && b > ?3", "a <= ?1",
             "(b > ?5 || b > ?4) && (b > ?7 || b > ?6)", null}
        });

        ranges = f1.multiRangeExtract(true, false, colMap.get("a"), colMap.get("b"));

        check(ranges, new String[][] {
            {"a == ?1 && b > ?2", "a <= ?1",
             "(b > ?5 || b > ?4) && (b > ?7 || b > ?6)", null},
            {"a == ?1 && b > ?3", "a == ?1 && b <= ?2",
             "(b > ?5 || b > ?4) && (b > ?7 || b > ?6)", null}
        });

        // Switch the direction of a few operators.
        RowFilter f2 = parse(colMap, "a==?&&(b<?||b<?)&&(b>?||b>?)&&(b>?||b>?)");
        range = f2.cnf().rangeExtract(colMap.get("a"), colMap.get("b"));

        check(range, new String[] {
            "a >= ?1", "a <= ?1", 
            "(b < ?2 || b < ?3) && (b > ?4 || b > ?5) && (b > ?6 || b > ?7)", null
        });

        RowFilter f3 = f2.dnf();
        ranges = f3.multiRangeExtract(false, true, colMap.get("a"), colMap.get("b"));

        check(ranges, new String[][] {
            {"a >= ?1", "a == ?1 && b < ?2",
             "(b > ?5 || b > ?4) && (b > ?7 || b > ?6)", null},
            {"a >= ?1", "a == ?1 && b < ?3",
             "(b > ?5 || b > ?4) && (b > ?7 || b > ?6)", null}
        });

        ranges = f3.multiRangeExtract(true, true, colMap.get("a"), colMap.get("b"));

        check(ranges, new String[][] {
            {"a >= ?1", "a == ?1 && b < ?2",
             "(b > ?5 || b > ?4) && (b > ?7 || b > ?6)", null},
            {"a == ?1 && b >= ?2", "a == ?1 && b < ?3",
             "(b > ?5 || b > ?4) && (b > ?7 || b > ?6)", null}
        });
    }

    @Test
    public void random() throws Exception {
        Map<String, ColumnInfo> colMap = NormalizeTest.newColMap(4);
        var columns = colMap.values().toArray(ColumnInfo[]::new);

        final var rnd = new Random(5916309638498201512L);

        int numFilters = 10000;
        int numResults = 1000;

        for (int i=0; i<numFilters; i++) {
            RowFilter filter = NormalizeTest.randomFilter(rnd, colMap, 4, 4, 4, OP_IN - 1);
            RowFilter dnf = filter.dnf();

            RowFilter[][] ranges = dnf.multiRangeExtract(false, false, columns);

            if (ranges.length == 1) {
                RowFilter[] range = ranges[0];
                if (range[0] == null && range[1] == null) {
                    // Full scan or FalseFilter.
                    continue;
                }
            }

            RowFilter combined = FalseFilter.THE;

            for (RowFilter[] range : ranges) {
                RowFilter rangeCombined;
                rangeCombined = range[0] != null ? range[0] : TrueFilter.THE;
                if (range[1] != null) {
                    rangeCombined = rangeCombined.and(range[1]);
                }
                if (range[2] != null) {
                    rangeCombined = rangeCombined.and(range[2]);
                }
                combined = combined.or(rangeCombined);
            }

            combined = combined.dnf().sort();

            if (dnf.sort().equals(combined)) {
                continue;
            }

            // When the combined filter doesn't match the original dnf filter, it's generally
            // because the combined filter has applied even more reductions. Evaluate the
            // filters against random data to help verify that they're equivalent.

            boolean[] results1 = NormalizeTest.eval
                (filter, 4, new Random(3551161645581228324L), numResults);

            boolean[] results2 = NormalizeTest.eval
                (combined, 4, new Random(3551161645581228324L), numResults);

            assertTrue(Arrays.equals(results1, results2));
        }
    }

    private static void check(RowFilter[] range, String[] expect) {
        assertEquals(expect.length, range.length);
        for (int i=0; i<expect.length; i++) {
            if (expect[i] == null) {
                assertNull(range[i]);
            } else {
                assertEquals(expect[i], range[i].toString());
            }
        }
    }

    private static void check(RowFilter[][] ranges, String[][] expectedRanges) {
        assertEquals(expectedRanges.length, ranges.length);
        for (int i=0; i<expectedRanges.length; i++) {
            //System.out.println(Arrays.toString(ranges[i]));
            check(ranges[i], expectedRanges[i]);
        }
    }

    static RowFilter parse(Map<String, ColumnInfo> colMap, String filter) {
        return RowFilterTest.parse(colMap, filter);
    }
}
