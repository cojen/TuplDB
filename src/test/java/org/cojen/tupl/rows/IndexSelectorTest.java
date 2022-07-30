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

import org.cojen.tupl.filter.Parser;
import org.cojen.tupl.filter.Query;

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
        verify("a == ?", "+id", "a == ?0");
    }

    @Test
    public void set2() throws Exception {
        withType(Type2.class);

        verify("id == ?", "+id", "id == ?0");
        verify("a == ?", "+a+id", "a == ?0");
        verify("d == ?", "+d+b+id", "d == ?0");
        verify("c == ? && b == ?", "+c+b+id", "c == ?0 && b == ?1");
        verify("b == ? && c == ?", "+c+b+id", "b == ?0 && c == ?1");
        verify("c > ? && b == ?", "+b+id", "c > ?0 && b == ?1");
        verify("c > ? && b > ?", "+id", "c > ?0 && b > ?1");
        verify("c != ? && b > ?", "+id", "c != ?0 && b > ?1");
        verify("d > ? && b > ? && c > ?", "+id", "d > ?0 && b > ?1 && c > ?2");
        verify("b > ? && c > ? && d > ?", "+id", "b > ?0 && c > ?1 && d > ?2");
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
        verify("{c, b}: c != ? || b == ? || d == c",
               "+d+c+b+id", "{c, b}: c != ?0 || b == ?1 || d == c");
        verify("{c, b}: c != ? || b == ? || d == e", "+id", "{c, b}: c != ?0 || b == ?1 || d == e");
        verify("{*}: c != ? || b == ? || d == e", "+id", "c != ?0 || b == ?1 || d == e");

        verify("a == ? || a == ?", "+a+id", "a == ?0 || a == ?1");
        verify("a == ? || b == ? || a == ?",
               "+b+id", "b == ?1", "+a+id", "(a == ?0 || a == ?2) && b != ?1");

        verify("e == b", "+id", "e == b");
        verify("{e, b}: e == b", "+e+b+id", "{e, b}: e == b");
        verify("{e, b}: b == e", "+b+e+id", "{e, b}: b == e");
        verify("{b, e}: b == e", "+b+e+id", "{b, e}: b == e");
        verify("{b, e}: e != ? && b in ?", "+e+b+id", "{b, e}: e != ?0 && b in ?1");

        verify("{b, id}: b == ? && id == ?", "+b+id", "{b, id}: b == ?0 && id == ?1");
        verify("d == ? && c in ? && b > ?", "+d+b+id", "d == ?0 && c in ?1 && b > ?2");
        verify("d == ? && !(c in ?) && b in ?", "+d+c+b+id", "d == ?0 && !(c in ?1) && b in ?2");

        verify("d == ? && c > ? && b > ?", "+d+c+b+id", "d == ?0 && c > ?1 && b > ?2");
        verify("d == ? && c != ? && b > ?", "+d+b+id", "d == ?0 && c != ?1 && b > ?2");
        verify("d == ? && b > ? && c > ?", "+d+c+b+id", "d == ?0 && b > ?1 && c > ?2");

        verify("{+c, *}: c > ? && b > ?", "+c+b+id", "c > ?0 && b > ?1");
        verify("{+b, *}: c != ? && b > ?", "+b+id", "c != ?0 && b > ?1");
        verify("{+d, *}: d > ? && b > ? && c > ?", "+d+c+b+id", "d > ?0 && b > ?1 && c > ?2");
        verify("{+d, +b, *}: b > ? && c > ? && d > ?", "+d+c+b+id", "b > ?0 && c > ?1 && d > ?2");

        verify("{+b, *}: e == ? && b == ?", "+b+e+id", "e == ?0 && b == ?1");
        verify("{-b, *}: e == ? && b == ?", "+b+e+id", "e == ?0 && b == ?1");
        verify("{+e, *}: e == ? && b == ?", "+e+b+id", "e == ?0 && b == ?1");

        verify("{+d, +c, *}: d == ?", "+d+c+b+id", "d == ?0");
        verify("{-d, -c, *}: d == ?", "+d+c+b+id", "d == ?0");
        verify("{+d, -c, *}: d == ?", "+d+b+id", "d == ?0");
        verify("{-d, +c, *}: d == ?", "+d+b+id", "d == ?0");

        verify("{+c, *}: c > ? && c < ? && b > ? && b < ? || d == ?",
               "+d+b+id", "d == ?4",
               "+c+b+id", "c > ?0 && c < ?1 && b > ?2 && b < ?3 && d != ?4");

        verify("{+c, *}: c > ? && b > ? && b < ? || d == ?",
               "+d+b+id", "d == ?3",
               "+b+id", "c > ?0 && b > ?1 && b < ?2 && d != ?3");

        verify("{+c, *}: c > ? && b > ? && b < ?", "+b+id", "c > ?0 && b > ?1 && b < ?2");
    }

    @Test
    public void set3() throws Exception {
        withType(Type3.class);

        verify("id == ?", "+id", "id == ?0");
        verify("a == ?", "+a", "a == ?0");
        verify("b == ?", "+b-c+id", "b == ?0");
        verify("{b}: b != ? && d == ?", "+b-c+id", "{b}: b != ?0 && d == ?1");
        verify("{a, id}: a != ? || id == ?", "+a", "{a, id}: a != ?0 || id == ?1");

        // FIXME: verify that reverse scan order is selected (when implemented)
        verify("c == ? && e > ? && a > ?", "+c-e+id", "c == ?0 && e > ?1 && a > ?2");
        verify("c == ? && a > ? && e > ?", "+c+a+id", "c == ?0 && a > ?1 && e > ?2");
        verify("c == ? && e < ? && a < ?", "+c-e+id", "c == ?0 && e < ?1 && a < ?2");
        // FIXME: verify that reverse scan order is selected (when implemented)
        verify("c == ? && a < ? && e < ?", "+c+a+id", "c == ?0 && a < ?1 && e < ?2");
        verify("c == ? && e < ? && a > ?", "+c-e+id", "c == ?0 && e < ?1 && a > ?2");
        verify("c == ? && a > ? && e < ?", "+c+a+id", "c == ?0 && a > ?1 && e < ?2");

        // FIXME: verify that reverse scan order is selected (when implemented)
        verify("c == ? && e > ? && a >= ?", "+c-e+id", "c == ?0 && e > ?1 && a >= ?2");
        verify("c == ? && a > ? && e >= ?", "+c+a+id", "c == ?0 && a > ?1 && e >= ?2");
        verify("c == ? && e < ? && a <= ?", "+c-e+id", "c == ?0 && e < ?1 && a <= ?2");
        // FIXME: verify that reverse scan order is selected (when implemented)
        verify("c == ? && a < ? && e <= ?", "+c+a+id", "c == ?0 && a < ?1 && e <= ?2");
        verify("c == ? && e < ? && a >= ?", "+c-e+id", "c == ?0 && e < ?1 && a >= ?2");
        verify("c == ? && a > ? && e <= ?", "+c+a+id", "c == ?0 && a > ?1 && e <= ?2");

        verify("c == ? && a == d", "+c+a+id", "c == ?0 && a == d");
        verify("c == ? && a == e", "+c+a+id", "c == ?0 && a == e");
        verify("c == ? && e == a", "+c-e+id", "c == ?0 && e == a");

        verify("e != ? && c > ? && c < ?", "+c-e+id", "e != ?0 && c > ?1 && c < ?2");
        verify("e != ? && c > ? && c < ? && c >= ?",
               "+c-e+id", "e != ?0 && c > ?1 && c < ?2 && c >= ?3");
        verify("e != ? && c > ? && c < ? && c >= ? && c < ?",
               "+c-e+id", "e != ?0 && c > ?1 && c < ?2 && c >= ?3 && c < ?4");

        verify("a == ? || id < ?", "+a", "a == ?0", "+id", "id < ?1 && a != ?0");

        verify("{+a, *}: a > ? || (b > ? && b < ?)", "+id", "a > ?0 || (b > ?1 && b < ?2)");

        verify("{+a, *}: (b > ? && b < ?) || a > ?", "+id", "(b > ?0 && b < ?1) || a > ?2");

        verify("{-a, *}: (a > ? && a < ?) || (b > ? && b < ?)",
               "+a", "a > ?0 && a < ?1",
               "+b-c+id", "b > ?2 && b < ?3 && (a <= ?0 || a >= ?1)");
    }

    @PrimaryKey("id")
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

    @PrimaryKey("id")
    @SecondaryIndex("a")
    @SecondaryIndex("b")
    @SecondaryIndex({"c", "b"})
    @SecondaryIndex({"d", "b"})
    @SecondaryIndex({"d", "c", "b"})
    @SecondaryIndex({"e", "b"})
    @SecondaryIndex({"b", "e"})
    public static interface Type2 extends Type1 {
    }

    @PrimaryKey("id")
    @AlternateKey("a")
    @SecondaryIndex({"b", "-c", "id", "d"})
    @SecondaryIndex({"c", "a"})
    @SecondaryIndex({"c", "-e"})
    public static interface Type3 extends Type1 {
    }

    private RowInfo mInfo;

    private void withType(Class<?> type) {
        mInfo = RowInfo.find(type);
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
            assertEquals(expect[i * 2 + 1], selector.selectedQuery(i).toString());
        }
    }

    private Query parse(String filter) {
        return new Parser(mInfo.allColumns, filter).parseQuery(null);
    }

    private <R> IndexSelector selector(String filter) {
        return new IndexSelector(mInfo, parse(filter), false);
    }
}
