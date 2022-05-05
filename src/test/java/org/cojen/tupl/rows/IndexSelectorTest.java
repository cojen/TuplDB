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

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.filter.FullFilter;
import org.cojen.tupl.filter.Parser;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class IndexSelectorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(IndexSelectorTest.class.getName());
    }

    @Test
    public void set1() throws Exception {
        withType(Type1.class);

        verify("id == ?", "+id", "id == ?0");
        verify("a == ?", "+a+id", "a == ?0");
        verify("d == ?", "+d+b+id", "d == ?0");
        verify("c == ? && b == ?", "+c+b+id", "c == ?0 && b == ?1");
        verify("b == ? && c == ?", "+c+b+id", "b == ?0 && c == ?1");
        verify("c > ? && b == ?", "+b+id", "c > ?0 && b == ?1");
        verify("c > ? && b > ?", "+c+b+id", "c > ?0 && b > ?1");
        verify("c != ? && b > ?", "+b+id", "c != ?0 && b > ?1");
        verify("d > ? && b > ? && c > ?", "+d+c+b+id", "d > ?0 && b > ?1 && c > ?2");
        verify("b > ? && c > ? && d > ?", "+d+c+b+id", "b > ?0 && c > ?1 && d > ?2");
        verify("d > ? && d < ?", "+d+b+id", "d > ?0 && d < ?1");
        verify("d < ?", "+id", "d < ?0");
        verify("d == ? && b > ? && c >= ? && c < ?", "+d+c+b+id",
               "d == ?0 && b > ?1 && c >= ?2 && c < ?3");
        verify("a <= ? && id > ? && a > ? && c >= ? && d == ? && b > ?", "+d+c+b+id", 
               "a <= ?0 && id > ?1 && a > ?2 && c >= ?3 && d == ?4 && b > ?5");
        verify("d == ? && b > ? && c > ? && b < ?", "+d+b+id",
               "d == ?0 && b > ?1 && c > ?2 && b < ?3");
        verify("e == ? && b == ?", "+e+b+id", "e == ?0 && b == ?1");
        verify("b == ? && e == ?", "+b+e+id", "b == ?0 && e == ?1");

        verify("{c, b}: c != ? && b != ?", "+c+b+id", "{c, b}: c != ?0 && b != ?1");
        verify("{c, b, e}: c != ? && b != ?", "+id", "{c, b, e}: c != ?0 && b != ?1");

        verify("a == ? || b == ?", "+a+id", "a == ?0", "+b+id", "b == ?1 && a != ?0");
        verify("a == ? || b == ? || id == ?", "+a+id", "a == ?0", "+b+id", "b == ?1 && a != ?0",
               "+id", "id == ?2 && a != ?0 && b != ?1");
        verify("a == ? || b == ? || c == ? && b == ? || d == ? && c != ?",
               "+a+id", "a == ?0",
               "+b+id", "b == ?1 && a != ?0",
               "+c+b+id", "c == ?2 && b == ?3 && a != ?0 && b != ?1",
               "+d+c+b+id", "d == ?4 && c != ?5 && a != ?0 && b != ?1 && (c != ?2 || b != ?3)");

        verify("{c, b}: c > ? && b > ? || id == ?",
               "+id", "{c, b}: id == ?2",
               "+c+b+id", "{c, b}: c > ?0 && b > ?1 && id != ?2");

        verify("{c, b}: c != ? || b == ?", "+c+b+id", "{c, b}: c != ?0 || b == ?1");

        verify("{c, b}: c != ? || b == ? && d != ?",
               "+d+c+b+id", "{c, b}: c != ?0 || (b == ?1 && d != ?2)");

        verify("{c, b}: c != ? || b == ? || d == ?",
               "+d+c+b+id", "{c, b}: c != ?0 || b == ?1 || d == ?2");
    }

    @PrimaryKey("id")
    @SecondaryIndex("a")
    @SecondaryIndex("b")
    @SecondaryIndex({"c", "b"})
    @SecondaryIndex({"d", "b"})
    @SecondaryIndex({"d", "c", "b"})
    @SecondaryIndex({"e", "b"})
    @SecondaryIndex({"b", "e"})
    public static interface Type1 {
        long id();
        void id(long id);

        String a();
        void a(String a);

        String b();
        void b(String b);

        String c();
        void c(String c);

        String d();
        void d(String d);

        String e();
        void e(String e);
    }

    private RowInfo mInfo;

    private void withType(Class<?> type) {
        mInfo = RowInfo.find(Type1.class);
    }

    /**
     * @param expect index/filter string pairs
     */
    private void verify(String filter, String... expect) {
        var selector = selector(filter);
        int numSelected = selector.analyze();

        assertEquals(expect.length, numSelected * 2);

        for (int i=0; i<numSelected; i++) {
            assertEquals(expect[i * 2], selector.selectedIndex(i).indexSpec());
            assertEquals(expect[i * 2 + 1], selector.selectedFilter(i).toString());
        }
    }

    private FullFilter parse(String filter) {
        return new Parser(mInfo.allColumns, filter).parseFull(null);
    }

    private <R> IndexSelector selector(String filter) {
        return new IndexSelector(mInfo, parse(filter), false);
    }
}
