/*
 *  Copyright (C) 2023 Cojen.org
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

package org.cojen.tupl.table.join;

import java.util.Map;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.PrimaryKey;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.RowInfo;

import org.cojen.tupl.table.expr.Parser;
import org.cojen.tupl.table.expr.RelationExpr;

import org.cojen.tupl.table.filter.QuerySpec;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class JoinParserTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(JoinParserTest.class.getName());
    }

    @Test
    public void basic() throws Exception {
        pass("rowC.cValue == ?1");
        pass("joinAB.rowA.aValue == ?1");
        pass("joinAB.rowA.aValue != ?2 || joinAB.rowB.bValue == ?2");

        pass("{} rowC.cValue == ?1");
        pass("{*} rowC.cValue == ?1", "rowC.cValue == ?1");
        pass("{joinAB.rowA.aValue} rowC.cValue == ?1");

        pass("{rowC.cValue, *} rowC.cValue == ?1", "{rowC.cValue, joinAB, rowC} rowC.cValue == ?1");
        pass("{*, ~joinAB} rowC.cValue == ?1", "{rowC} rowC.cValue == ?1");
        pass("{~joinAB, *} rowC.cValue == ?1", "rowC.cValue == ?1");

        pass("{joinAB.rowA.*} rowC.cValue == ?1",
             "{joinAB.rowA.aValue, joinAB.rowA.id} rowC.cValue == ?1");
    }

    @Test
    public void failures() throws Exception {
        parseFail("* == ?", "Wildcard disallowed");
        parseFail("rowC.cValue.* == ?", "disallowed");
        parseFail("{joinAB.rowA.id.*} rowC.cValue == ?1", "disallowed for scalar column");
        parseFail("{~joinAB.rowA.*}", "Cannot exclude by wildcard");
    }

    private void pass(String filterStr) throws Exception {
        pass(filterStr, filterStr);
    }

    private void pass(String filterStr, String expect) throws Exception {
        RelationExpr expr = Parser.parse(JoinABC.class, filterStr);
        QuerySpec spec = expr.querySpec();
        assertEquals(expect, spec.toString());
    }

    private void parseFail(String filterStr, String message) {
        try {
            Parser.parse(JoinABC.class, filterStr);
            fail();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage(), e.getMessage().contains(message));
        }
    }

    @PrimaryKey("id")
    public static interface RowA {
        int id();
        void id(int id);

        String aValue();
        void aValue(String v);
    }

    @PrimaryKey("id")
    public static interface RowB {
        int id();
        void id(int id);

        String bValue();
        void bValue(String v);
    }

    @PrimaryKey("id")
    public static interface RowC {
        int id();
        void id(int id);

        String cValue();
        void cValue(String v);
    }

    public static interface JoinAB {
        RowA rowA();
        void rowA(RowA a);

        RowB rowB();
        void rowB(RowB b);
    }

    public static interface JoinABC {
        JoinAB joinAB();
        void joinAB(JoinAB ab);

        RowC rowC();
        void rowC(RowC c);
    }
}
