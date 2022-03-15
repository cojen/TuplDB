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
        pf("a in ?0 || a == ?0", "Mismatched argument usage");
        pf("!(a==\u2003a)||a>?5||a in?5", "Mismatched argument usage");
        pf("!(a\u2003== ?", "Right paren");
        pf("a inx", "Unknown operator");
        pf("1==?", "Not a valid character");
        pf("a<=?a", "Malformed argument number");
        pf("a<=?999999999999999999999999999999999999", "too large");
    }

    // pf: parse failure
    private void pf(String filterStr, String message) {
        try {
            new Parser(mColumnMap, filterStr).parseFilter();
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains(message));
        }
    }

    @Test
    public void flatten() throws Exception {
        String filterStr = "a == ? && (a == ? && a < ?0) && !((a == ? || a < ?0) || a != ?)";
        RowFilter filter = new Parser(mColumnMap, filterStr).parseFilter();
        assertEquals("a == ?0 && a == ?1 && a < ?0 && a != ?2 && a >= ?0 && a == ?3",
                     filter.toString());
    }
}
