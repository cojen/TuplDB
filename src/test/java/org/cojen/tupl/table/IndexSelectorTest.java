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

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.table.expr.Parser;

import org.cojen.tupl.table.filter.QuerySpec;

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
        verify("id == ?", "+id", "id == ?1");
        verify("a == ?", "+id", "a == ?1");
    }

    @Test
    public void set2() throws Exception {
        withType(Type2.class);

        verify("id == ?", "+id", "id == ?1");
        verify("a == ?", "+a+id", "a == ?1");
        verify("d == ?", "+d+b+id", "d == ?1");
        verify("c == ? && b == ?", "+c+b+id", "c == ?1 && b == ?2");
        verify("b == ? && c == ?", "+c+b+id", "b == ?1 && c == ?2");
        verify("c > ? && b == ?", "+b+id", "c > ?1 && b == ?2");
        verify("c > ? && b > ?", "+id", "c > ?1 && b > ?2");
        verify("c != ? && b > ?", "+id", "c != ?1 && b > ?2");
        verify("d > ? && b > ? && c > ?", "+id", "d > ?1 && b > ?2 && c > ?3");
        verify("b > ? && c > ? && d > ?", "+id", "b > ?1 && c > ?2 && d > ?3");
        verify("d > ? && d < ?", "+d+b+id", "d > ?1 && d < ?2");
        verify("d < ?", "+id", "d < ?1");
        verify("d == ? && b > ? && c >= ? && c < ?", "+d+c+b+id",
               "d == ?1 && b > ?2 && c >= ?3 && c < ?4");
        verify("a <= ? && id > ? && a > ? && c >= ? && d == ? && b > ?", "+d+c+b+id", 
               "a <= ?1 && id > ?2 && a > ?3 && c >= ?4 && d == ?5 && b > ?6");
        verify("d == ? && b > ? && c > ? && b < ?", "+d+b+id",
               "d == ?1 && b > ?2 && c > ?3 && b < ?4");
        verify("e == ? && b == ?", "+e+b+id", "e == ?1 && b == ?2");
        verify("b == ? && e == ?", "+b+e+id", "b == ?1 && e == ?2");

        verify("{c, b} c != ? && b != ?", "+c+b+id", "{c, b} c != ?1 && b != ?2");
        verify("{c, b, e} c != ? && b != ?", "+id", "{c, b, e} c != ?1 && b != ?2");

        verify("a == ? || b == ?", "+a+id", "a == ?1", "+b+id", "b == ?2 && a != ?1");
        verify("a == ? || b == ? || id == ?", "+a+id", "a == ?1", "+b+id", "b == ?2 && a != ?1",
               "+id", "id == ?3 && a != ?1 && b != ?2");
        verify("a == ? || b == ? || c == ? && b == ? || d == ? && c != ?",
               "+a+id", "a == ?1",
               "+b+id", "b == ?2 && a != ?1",
               "+c+b+id", "c == ?3 && b == ?4 && a != ?1 && b != ?2",
               "+d+c+b+id", "d == ?5 && c != ?6 && a != ?1 && b != ?2 && (c != ?3 || b != ?4)");

        verify("{c, b} c > ? && b > ? || id == ?",
               "+id", "{c, b} id == ?3",
               "+c+b+id", "{c, b} c > ?1 && b > ?2 && id != ?3");

        verify("{c, b} c != ? || b == ?", "+c+b+id", "{c, b} c != ?1 || b == ?2");

        verify("{c, b} c != ? || b == ? && d != ?",
               "+d+c+b+id", "{c, b} c != ?1 || (b == ?2 && d != ?3)");

        verify("{c, b} c != ? || b == ? || d == ?",
               "+d+c+b+id", "{c, b} c != ?1 || b == ?2 || d == ?3");

        // Note: The expression evaluation order is swapped because the column-to-column
        // comparison is less likely to throw an exception at runtime. See BinaryOpExpr.make.
        verify("{c, b} c != ? || b == ? || d == c",
               "+d+c+b+id", "{c, b} d == c || c != ?1 || b == ?2");
        verify("{c, b} c != ? || b == ? || d == e",
               "+id", "{c, b} d == e || c != ?1 || b == ?2");
        verify("{*} c != ? || b == ? || d == e", "+id", "d == e || c != ?1 || b == ?2");

        verify("a == ? || a == ?", "+a+id", "a == ?1 || a == ?2");
        verify("a == ? || b == ? || a == ?",
               "+b+id", "b == ?2", "+a+id", "(a == ?1 || a == ?3) && b != ?2");

        verify("e == b", "+id", "e == b");
        verify("{e, b} e == b", "+e+b+id", "{e, b} e == b");
        verify("{e, b} b == e", "+b+e+id", "{e, b} b == e");
        verify("{b, e} b == e", "+b+e+id", "{b, e} b == e");
        verify("{b, e} e != ? && b in ?", "+e+b+id", "{b, e} e != ?1 && b in ?2");

        verify("{b, id} b == ? && id == ?", "+b+id", "{b, id} b == ?1 && id == ?2");
        verify("d == ? && c in ? && b > ?", "+d+b+id", "d == ?1 && c in ?2 && b > ?3");
        verify("d == ? && !(c in ?) && b in ?", "+d+c+b+id", "d == ?1 && !(c in ?2) && b in ?3");

        verify("d == ? && c > ? && b > ?", "+d+c+b+id", "d == ?1 && c > ?2 && b > ?3");
        verify("d == ? && c != ? && b > ?", "+d+b+id", "d == ?1 && c != ?2 && b > ?3");
        verify("d == ? && b > ? && c > ?", "+d+c+b+id", "d == ?1 && b > ?2 && c > ?3");

        verify("{+c, *} c > ? && b > ?", "+c+b+id", "c > ?1 && b > ?2");
        verify("{+b, *} c != ? && b > ?", "+b+id", "c != ?1 && b > ?2");
        verify("{+d, *} d > ? && b > ? && c > ?", "+d+c+b+id", "d > ?1 && b > ?2 && c > ?3");
        verify("{+d, +b, *} b > ? && c > ? && d > ?", "+d+c+b+id", "b > ?1 && c > ?2 && d > ?3");

        verify("{+b, *} e == ? && b == ?", "+b+e+id", "e == ?1 && b == ?2");
        verify("{-b, *} e == ? && b == ?", "+b+e+id", "e == ?1 && b == ?2");
        verify("{+e, *} e == ? && b == ?", "+e+b+id", "e == ?1 && b == ?2");

        verify("{+d, +c, *} d == ?", "+d+c+b+id", "d == ?1");
        verify("{-d, -c, *} d == ?", "+d+c+b+id", "d == ?1");
        verify("{+d, -c, *} d == ?", "+d+b+id", "d == ?1");
        verify("{-d, +c, *} d == ?", "+d+b+id", "d == ?1");

        verify("{+c, *} c > ? && c < ? && b > ? && b < ? || d == ?",
               "+d+b+id", "d == ?5",
               "+c+b+id", "c > ?1 && c < ?2 && b > ?3 && b < ?4 && d != ?5");

        verify("{+c, *} c > ? && b > ? && b < ? || d == ?",
               "+d+b+id", "d == ?4",
               "+b+id", "c > ?1 && b > ?2 && b < ?3 && d != ?4");

        verify("{+c, *} c > ? && b > ? && b < ?", "+b+id", "c > ?1 && b > ?2 && b < ?3");

        verify("b == ? && id == ? && c == ?", "+id", "b == ?1 && id == ?2 && c == ?3");
        verify("{b} b == ? && id == ? && c == ?", "+c+b+id", "{b} b == ?1 && id == ?2 && c == ?3");

        // No filter, not all columns are projected, but the requested ordering matches the
        // natural order of the primary index. Should do a full scan of the primary index and
        // not a secondary index. If a secondary index was chosen, then sorting is required.
        verify("{+id, a}", "+id", "{id, a}");
        verify("{-id, a}", "R+id", "{id, a}");
        verify("{+id, +a}", "+id", "{id, a}");
    }

    @Test
    public void set3() throws Exception {
        withType(Type3.class);

        verify("id == ?", "+id", "id == ?1");
        verify("a == ?", "+a", "a == ?1");
        verify("b == ?", "+b-c+id", "b == ?1");
        verify("{b} b != ? && d == ?", "+b-c+id", "{b} b != ?1 && d == ?2");
        verify("{a, id} a != ? || id == ?", "+a", "{a, id} a != ?1 || id == ?2");

        verify("c == ? && e > ? && a > ?", "+c-e+id", "c == ?1 && e > ?2 && a > ?3");
        verify("c == ? && a > ? && e > ?", "+c+a+id", "c == ?1 && a > ?2 && e > ?3");
        verify("c == ? && e < ? && a < ?", "+c-e+id", "c == ?1 && e < ?2 && a < ?3");
        verify("c == ? && a < ? && e < ?", "+c+a+id", "c == ?1 && a < ?2 && e < ?3");
        verify("c == ? && e < ? && a > ?", "+c-e+id", "c == ?1 && e < ?2 && a > ?3");
        verify("c == ? && a > ? && e < ?", "+c+a+id", "c == ?1 && a > ?2 && e < ?3");

        verify("c == ? && e > ? && a >= ?", "+c-e+id", "c == ?1 && e > ?2 && a >= ?3");
        verify("c == ? && a > ? && e >= ?", "+c+a+id", "c == ?1 && a > ?2 && e >= ?3");
        verify("c == ? && e < ? && a <= ?", "+c-e+id", "c == ?1 && e < ?2 && a <= ?3");
        verify("c == ? && a < ? && e <= ?", "+c+a+id", "c == ?1 && a < ?2 && e <= ?3");
        verify("c == ? && e < ? && a >= ?", "+c-e+id", "c == ?1 && e < ?2 && a >= ?3");
        verify("c == ? && a > ? && e <= ?", "+c+a+id", "c == ?1 && a > ?2 && e <= ?3");

        // Note: The expression evaluation order is swapped because the column-to-column
        // comparison is less likely to throw an exception at runtime. See BinaryOpExpr.make.
        verify("c == ? && a == d", "+c+a+id", "a == d && c == ?1");
        verify("c == ? && a == e", "+c+a+id", "a == e && c == ?1");
        verify("c == ? && e == a", "+c-e+id", "e == a && c == ?1");

        verify("e != ? && c > ? && c < ?", "+c-e+id", "e != ?1 && c > ?2 && c < ?3");
        verify("e != ? && c > ? && c < ? && c >= ?",
               "+c-e+id", "e != ?1 && c > ?2 && c < ?3 && c >= ?4");
        verify("e != ? && c > ? && c < ? && c >= ? && c < ?",
               "+c-e+id", "e != ?1 && c > ?2 && c < ?3 && c >= ?4 && c < ?5");

        verify("a == ? || id < ?", "+a", "a == ?1", "+id", "id < ?2 && a != ?1");

        verify("{+a, *} a > ? || (b > ? && b < ?)", "+id", "a > ?1 || (b > ?2 && b < ?3)");

        verify("{+a, *} (b > ? && b < ?) || a > ?", "+id", "(b > ?1 && b < ?2) || a > ?3");

        verify("{-a, *} (a > ? && a < ?) || (b > ? && b < ?)",
               "R+a", "a > ?1 && a < ?2",
               "+b-c+id", "b > ?3 && b < ?4 && (a <= ?1 || a >= ?2)");

        // A few of these should do a reverse scan from a starting point, which is more
        // efficient than checking a predicate for each row.
        verify("id > ?", "+id", "id > ?1");
        verify("id < ?", "R+id", "id < ?1");
        verify("{a} a > ?", "+a", "{a} a > ?1");
        verify("{a} a < ?", "R+a", "{a} a < ?1");
    }

    @Test
    public void set4() throws Exception {
        withType(Type4.class);

        // A few of these should do a reverse scan from a starting point, which is more
        // efficient than checking a predicate for each row.
        verify("id > ?", "R-id", "id > ?1");
        verify("id < ?", "-id", "id < ?1");
        verify("{a} a > ?", "R-a+b-id", "{a} a > ?1");
        verify("{a} a < ?", "-a+b-id", "{a} a < ?1");
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

    @PrimaryKey("-id")
    @SecondaryIndex({"-a", "b"})
    public static interface Type4 extends Type1 {
    }

    private Class<?> mRowType;
    private RowInfo mInfo;

    private void withType(Class<?> type) {
        mInfo = RowInfo.find(type);
        mRowType = type;
    }

    /**
     * @param expect index/filter string pairs; if the index spec starts with 'R', then a
     * reverse scan is expected
     */
    private void verify(String filter, String... expect) throws Exception {
        var selector = selector(filter);
        int numSelected = selector.numSelected();

        assertEquals(expect.length, numSelected * 2);

        for (int i=0; i<numSelected; i++) {
            String indexSpec = expect[i * 2];
            String subFilter = expect[i* 2 + 1];

            boolean reverse = false;
            if (indexSpec.startsWith("R")) {
                indexSpec = indexSpec.substring(1);
                reverse = true;
            }

            assertEquals(indexSpec, selector.selectedIndex(i).indexSpec());
            assertEquals(subFilter, selector.selectedQuery(i).toString());
            assertEquals(reverse, selector.selectedReverse(i));
        }
    }

    private QuerySpec parse(String filter) {
        return Parser.parseQuerySpec(mRowType, filter);
    }

    private <R> IndexSelector<R> selector(String filter) throws Exception {
        return new IndexSelector<R>(null, mInfo, parse(filter), false);
    }
}
