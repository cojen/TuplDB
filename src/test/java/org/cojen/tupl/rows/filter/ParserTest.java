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
public class ParserTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ParserTest.class.getName());
    }

    private Map<String, ColumnInfo> mColumnMap;

    @Before
    public void setup() {
        mColumnMap = new HashMap<>();

        var info = new ColumnInfo();
        info.name = "a";
        info.typeCode = ColumnInfo.TYPE_DOUBLE;
        info.assignType();
        mColumnMap.put(info.name, info);

        info = new ColumnInfo();
        info.name = "b";
        info.typeCode = ColumnInfo.TYPE_INT;
        info.assignType();
        mColumnMap.put(info.name, info);

        info = new ColumnInfo();
        info.name = "c\u1f600";
        info.typeCode = ColumnInfo.TYPE_BOOLEAN;
        info.assignType();
        mColumnMap.put(info.name, info);
    }

    @Test
    public void failures() throws Exception {
        pf("", "Column name expected");
        pf("a", "Relational operator expected");
        pf("q", "Unknown column: q");
        pf("a>?|", "Or operator");
        pf("a>?&", "And operator");
        pf("a=?&", "Equality operator");
        pf("a!?&", "Inequality operator");
        pf("a==?)", "Unexpected trailing");
        pf("a i?)", "Unknown operator");
        pf("a^?)", "Unknown operator");
        pf("a?)", "Relational operator missing");
        pf("c\u1f600?)", "Relational operator missing");
        pf("!", "Left paren");
        pf("!x", "Left paren");
        pf("(a==)", "Not a valid character");
        pf("a in a", "Argument number");
        pf("a<c\u1f600", "type mismatch");
        pf("a in ?1 || a == ?1", "Mismatched argument usage");
        pf("a in ?1 || ?1 == a", "Mismatched argument usage");
        pf("!(a==\u2003a)||a>?5||a in?5", "Mismatched argument usage");
        pf("!(a==\u2003a)||?5<a||a in?5", "Mismatched argument usage");
        pf("!(a\u2003== ?", "Right paren");
        pf("a inx", "Unknown operator");
        pf("1==?", "Not a valid character");
        pf("a<=?a", "Malformed argument number");
        pf("a<=?999999999999999999999999999999999999", "too large");
        pf("{a, ~a}", "Cannot exclude");
        pf("{~a, -a}", "Cannot select");
        pf("{~*}", "Wildcard disallowed");
        pf("{~!a}", "Not a valid character");
        pf("{~a}", "Must include wildcard");
        pf("a<=?0", "at least one");
        pf("? in a", "Unsupported operator");
        pf("()!)", "Unexpected trailing");
    }

    // pf: parse failure
    private void pf(String filterStr, String message) {
        try {
            new Parser(mColumnMap, filterStr).parseQuery(null);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains(message));
        }
    }

    @Test
    public void flatten() throws Exception {
        String filterStr = "a == ? && (a == ? && a < ?1) && !((a == ? || a < ?1) || a != ?)";
        RowFilter filter = new Parser(mColumnMap, filterStr).parseFilter();
        assertEquals("a == ?1 && a == ?2 && a < ?1 && a != ?3 && a >= ?1 && a == ?4",
                     filter.toString());
    }

    @Test
    public void basic() throws Exception {
        passQuery("{}");
        passQuery("{*}");
        passQuery("{~b, *}", "{a, c\u1f600}");
        passQuery("{*, ~a}", "{b, c\u1f600}");
        passQuery("{*, ~ a}", "{b, c\u1f600}");
        passQuery("{+a}");
        passQuery("{+ a}", "{+a}");
        passQuery("{+a, *}");
        passQuery("{+ a, *}", "{+a, *}");
        passQuery("{+ !a}", "{+!a}");
        passQuery("{+ ! a}", "{+!a}");
        passQuery("{-b, + ! a, -a}", "{-b, +!a}");
        passQuery("{-b, + ! a}", "{-b, +!a}");

        passQuery("a == ?", "a == ?1");
        passQuery("a < ?2", "a < ?2");
        passQuery("? == a", "a == ?1");
        passQuery("?2 < a", "a > ?2");
    }

    private void passQuery(String filterStr) throws Exception {
        passQuery(filterStr, filterStr);
    }

    private void passQuery(String filterStr, String expect) throws Exception {
        QuerySpec q = new Parser(mColumnMap, filterStr).parseQuery(null);
        assertEquals(expect, q.toString());
    }

    @Test
    public void trueFalse() throws Exception {
        RowFilter filter = parseFilter("()");
        assertEquals(TrueFilter.THE, filter);
        assertEquals("()", filter.toString());

        assertEquals(TrueFilter.THE, parseFilter("( )"));

        filter = parseFilter("!()");
        assertEquals(FalseFilter.THE, filter);
        assertEquals("!()", filter.toString());

        assertEquals(FalseFilter.THE, parseFilter("!()"));
        assertEquals(FalseFilter.THE, parseFilter("!( )"));
        assertEquals(FalseFilter.THE, parseFilter("! ()"));
        assertEquals(FalseFilter.THE, parseFilter("! ( )"));
    }

    private RowFilter parseFilter(String filterStr) throws Exception {
        return new Parser(mColumnMap, filterStr).parseFilter();
    }
}
