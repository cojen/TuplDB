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

package org.cojen.tupl.table.expr;

import java.util.HashMap;
import java.util.Map;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.PrimaryKey;
import org.cojen.tupl.Table;

import org.cojen.tupl.table.ColumnInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ParserTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ParserTest.class.getName());
    }

    private Table<TestRow> mTable;

    @Before
    public void setup() throws Exception {
        mTable = Database.open(new DatabaseConfig()).openTable(TestRow.class);
    }

    @Test
    public void failures() throws Exception {
        parseFail("", "Identifier expected");
        parseFail("a", "Cannot convert double to boolean");
        parseFail("q", "Unknown column or variable: q");
        parseFail("a>?|", "Identifier expected");
        parseFail("a>?&", "Identifier expected");
        parseFail("a=?&", "Equality operator");
        parseFail("a!?&", "Unexpected trailing");
        parseFail("a==?)", "Unexpected trailing");
        parseFail("a i?)", "Unexpected trailing");
        parseFail("a^?)", "Bitwise operation not allowed");
        parseFail("a?)", "Unexpected trailing");
        parseFail("c\u1f600?)", "Unexpected trailing");
        parseFail("!", "Identifier expected");
        parseFail("!x", "Unknown column or variable: x");
        parseFail("(a==)", "Identifier expected");
        parseFail("a<c\u1f600", "No common type");
        parseFail("!(a\u2003== ?", "Right paren");
        parseFail("a inx", "Unexpected trailing");
        parseFail("a<=?a", "Malformed argument number");
        parseFail("a<=?999999999999999999999999999999999999", "Malformed argument number");
        parseFail("{~a, -a}", "Excluded projection not found");
        parseFail("{~*}", "Wildcard disallowed");
        parseFail("{~!a}", "Identifier expected");
        parseFail("{~a}", "Excluded projection not found");
        parseFail("a<=?0", "at least one");
        parseFail("()!)", "Identifier expected");
    }

    private void parseFail(String queryStr, String message) {
        try {
            Parser.parse(mTable, queryStr);
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage(), e.getMessage().contains(message));
        }
    }

    @Test
    public void flatten() throws Exception {
        String filterStr = "a == ? && (a == ? && a < ?1) && !((a == ? || a < ?1) || a != ?)";
        RelationExpr expr = Parser.parse(mTable, filterStr);
        assertEquals("a == ?1 && a == ?2 && a < ?1 && a != ?3 && a >= ?1 && a == ?4",
                     expr.querySpec(mTable.rowType()).toString());
    }

    @Test
    public void basic() throws Exception {
        passQuery("{}");
        passQuery("{*}");
        passQuery("{*, ~a}", "{b, c\u1f600}");
        passQuery("{*, ~ a}", "{b, c\u1f600}");
        passQuery("{+a}");
        passQuery("{+ a}", "{+a}");
        passQuery("{+a, *}");
        passQuery("{+ a, *}", "{+a, *}");
        passQuery("{+ !a}", "{+!a}");
        passQuery("{+ ! a}", "{+!a}");
        passQuery("{-b, + ! a}", "{-b, +!a}");

        passQuery("a == ?", "a == ?1");
        passQuery("a < ?2", "a < ?2");
        passQuery("? == a", "a == ?1");
        passQuery("?2 < a", "a > ?2");

        passQuery("true || false", "{*}");
    }

    private void passQuery(String queryStr) throws Exception {
        passQuery(queryStr, queryStr);
    }

    private void passQuery(String queryStr, String expect) throws Exception {
        RelationExpr expr = Parser.parse(mTable, queryStr);
        assertEquals(expect, expr.querySpec(mTable.rowType()).toString());
    }

    @PrimaryKey("a")
    public static interface TestRow {
        double a();
        void a(double a);

        int b();
        void b(int b);

        boolean c\u1f600();
        void c\u1f600(boolean c);
    }
}
