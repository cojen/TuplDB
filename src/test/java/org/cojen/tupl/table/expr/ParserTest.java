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
import org.cojen.tupl.QueryException;
import org.cojen.tupl.Table;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ParserTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ParserTest.class.getName());
    }

    private Database mDb;
    private Table<TestRow> mTable;

    @Before
    public void setup() throws Exception {
        mDb = Database.open(new DatabaseConfig());
        mTable = mDb.openTable(TestRow.class);
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
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
        parseFail("{~*}", "Wildcard disallowed");
        parseFail("{~!a}", "Cannot convert double to boolean");
        parseFail("a<=?0", "at least one");
        parseFail("()!)", "Identifier expected");
        parseFail("{1}", "must be assigned");
        parseFail("{1 + sum(1)}", "must be assigned");
        parseFail("{sum(1)}", "must be assigned");
        parseFail("{foo(1)}", "Unknown function");
        parseFail("{q=0xq}", "Right brace expected");
        parseFail("{q=0b2}", "Right brace expected");
        parseFail("{q=0x.fg}", "Right brace expected");
        parseFail("{q=0.0L}", "Right brace expected");
        parseFail("{\u270b}", "Identifier expected");
    }

    private void parseFail(String queryStr, String message) {
        try {
            Parser.parse(mTable, null, queryStr);
            fail();
        } catch (QueryException e) {
            assertTrue(e.getMessage(), e.getMessage().contains(message));
        }
    }

    @Test
    public void flatten() throws Exception {
        String filterStr = "a == ? && (a == ? && a < ?1) && !((a == ? || a < ?1) || a != ?)";
        RelationExpr expr = Parser.parse(mTable, mTable.rowType(), filterStr);
        assertEquals("a == ?1 && a == ?2 && a < ?1 && a != ?3 && a >= ?1 && a == ?4",
                     expr.querySpec().toString());
    }

    @Test
    public void basic() throws Exception {
        passQuery("{}");
        passQuery("{*}");
        passQuery("{*, ~a}", "{b, c\u1f600, \u1e00}");
        passQuery("{*, ~ a}", "{b, c\u1f600, \u1e00}");
        passQuery("{\u1e00}", "{\u1e00}");
        passQuery("{+a}");
        passQuery("{+ a}", "{+a}");
        passQuery("{*, +a}");
        passQuery("{+ a, *}", "{*, +a}");
        passQuery("{+ !a}", "{+!a}");
        passQuery("{+ ! a}", "{+!a}");
        passQuery("{-b, + ! a}", "{-b, +!a}");

        passQuery("a == ?", "a == ?1");
        passQuery("a < ?2", "a < ?2");
        passQuery("? == a", "a == ?1");
        passQuery("?2 < a", "a > ?2");

        passQuery("true || false", "{*}");

        passQuery("`a` == `b`", "a == b");

        passQuery("{~a, -a}", "{-a}");
        passQuery("{~a}", "{}");
        passQuery("{~-a}", "{-a}"); // FIXME: should expect the original
        passQuery("{~+a}", "{+a}"); // FIXME: should expect the original
    }

    @Test
    public void literals() throws Exception {
        passQueryNoSpec("{q='\\0'}", "{q = \"\0\"}");
        passQueryNoSpec("{q='\\b'}", "{q = \"\b\"}");
        passQueryNoSpec("{q='\\t'}", "{q = \"\t\"}");
        passQueryNoSpec("{q='\\n'}", "{q = \"\n\"}");
        passQueryNoSpec("{q='\\f'}", "{q = \"\f\"}");
        passQueryNoSpec("{q='\\r'}", "{q = \"\r\"}");
        passQueryNoSpec("{q='\\\\'}", "{q = \"\\\"}");
        passQueryNoSpec("{q='\\''}", "{q = \"\'\"}");
        passQueryNoSpec("{q='\\\"'}", "{q = '\"'}");
        passQueryNoSpec("{q='\\`'}", "{q = \"`\"}");
        passQueryNoSpec("{q='\\Q'}", "{q = \"\\Q\"}");

        passQueryNoSpec("{q='\r'}", "{q = \"\r\"}");
        passQueryNoSpec("{q='\r\n'}", "{q = \"\n\"}");

        passQueryNoSpec("{q=.1}", "{q = 0.1}");
        passQueryNoSpec("{q=0xff, r=0X_f}", "{q = 255, r = 15}");
        passQueryNoSpec("{q=0b101, r=0B_10}", "{q = 5, r = 2}");
        passQueryNoSpec("{q=1e2}", "{q = 100.0}");
        passQueryNoSpec("{q=1e+2}", "{q = 100.0}");
        passQueryNoSpec("{q=1e-2}", "{q = 0.01}");
        passQueryNoSpec("{q=0x1e-2}", "{q = 30 - 2}");
        passQueryNoSpec("{q=0x1.0}", "{q = 1.0}");
        passQueryNoSpec("{q=100g}", "{q = 100}");
        passQueryNoSpec("{q=0xfG}", "{q = 15}");
        passQueryNoSpec("{q=100.0g}", "{q = 100.0}");
        passQueryNoSpec("{q=0.1G}", "{q = 0.1}");
        passQueryNoSpec("{q=0.1d}", "{q = 0.1}");
    }

    private void passQuery(String queryStr) throws Exception {
        passQuery(queryStr, queryStr);
    }

    private void passQuery(String queryStr, String expect) throws Exception {
        RelationExpr expr = Parser.parse(mTable, mTable.rowType(), queryStr);
        assertEquals(expect, expr.querySpec().toString());
    }

    private void passQueryNoSpec(String queryStr, String expect) throws Exception {
        RelationExpr expr = Parser.parse(mTable, null, queryStr);
        String exprStr = expr.toString();
        assertTrue(exprStr, exprStr.endsWith(expect));
    }

    @PrimaryKey("a")
    public static interface TestRow {
        double a();
        void a(double a);

        int b();
        void b(int b);

        boolean c\u1f600();
        void c\u1f600(boolean c);

        boolean \u1e00();
        void \u1e00(boolean c);
    }
}
