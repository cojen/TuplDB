/*
 *  Copyright (C) 2019 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.repl;

import java.util.TreeSet;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RangeSetTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RangeSetTest.class.getName());
    }

    @Test
    public void basicRanges() throws Exception {
        for (int start = 10; start <= 70; start += 10) {
            for (int end = 10; end <= 70; end += 10) {
                var set = new RangeSet();
                var set2 = new TreeSet<Long>();
                add(set, 30, 50, set2);
                add(set, start, end, set2);
                verify(set, set2);
            }
        }
    }

    @Test
    public void combineAdjacent() throws Exception {
        var set = new RangeSet();
        var set2 = new TreeSet<Long>();

        add(set, 10, 20, set2);
        add(set, 30, 40, set2);
        add(set, 50, 60, set2);
        add(set, 70, 80, set2);
        verify(set, set2);

        add(set, 15, 55, set2);
        verify(set, set2);
    }

    @Test
    public void permute() throws Exception {
        for (int i=0; i<32; i++) {
            for (int start = 9; start <= 26; start++) {
                for (int end = 9; end <= 27; end++) {
                    var set = new RangeSet();
                    var set2 = new TreeSet<Long>();
                    for (int b=1; b<32; b<<=1) {
                        if ((i & b) != 0) {
                            add(set, 10 + b - 1, 10 + b, set2);
                        }
                    }
                    verify(set, set2);
                    add(set, start, end, set2);
                    verify(set, set2);
                }
            }
        }
    }

    @Test
    public void internal() throws Exception {
        var set = new RangeSet();
        var set2 = new TreeSet<Long>();

        add(set, 10, 40, set2);
        add(set, 20, 30, set2);
        verify(set, set2);
    }

    private static void add(RangeSet set, long start, long end, TreeSet<Long> set2) {
        set.add(start, end);
        while (start < end) {
            set2.add(start);
            start++;
        }
    }

    private static void verify(RangeSet set, TreeSet<Long> set2) {
        set = set.copy();
        set2 = new TreeSet<>(set2);

        RangeSet.Range last = null;
        while (true) {
            RangeSet.Range range = set.removeLowest();
            if (range == null) {
                break;
            }

            if (range.start >= range.end) {
                throw new AssertionError(range);
            }

            if (last != null) {
                if (last.end >= range.start) {
                    throw new AssertionError("" + last + " >= " + range);
                }
            }

            for (long v = range.start; v < range.end; v++) {
                assertTrue(set2.remove(v));
            }

            last = range;
        }

        assertTrue(set.isEmpty());
        assertTrue(set2.isEmpty());
    }
}
