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

import java.util.HashMap;
import java.util.Map;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.table.filter.ColumnFilter.*;

import org.cojen.tupl.table.ColumnInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ReductionTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ReductionTest.class.getName());
    }

    private static HashMap<String, ColumnInfo> newColMap() {
        return newColMap("a");
    }

    private static HashMap<String, ColumnInfo> newColMap(String... names) {
        var colMap = new HashMap<String, ColumnInfo>();
        for (String name : names) {
            var info = new ColumnInfo();
            info.name = name;
            info.typeCode = ColumnInfo.TYPE_UTF8;
            info.assignType();
            colMap.put(info.name, info);
        }
        return colMap;
    }

    @Test
    public void operatorReduction() throws Exception {
        var colMap = newColMap();

        String[] ops = {"&&", "||"};
        String[] relOps = {"==", "!=", "<", ">=", ">", "<="};
        for (String op : ops) {
            for (String relOp1 : relOps) {
                for (String relOp2 : relOps) {
                    String filterStr = "a" + relOp1 + "?1" + op + "a" + relOp2 + "?1";
                    RowFilter filter = parse(colMap, filterStr);
                    RowFilter reduced = filter.reduce();

                    assertNotEquals(filter, reduced);

                    if (reduced instanceof GroupFilter gf) {
                        assertEquals(0, gf.subFilters().length);
                    }

                    {
                        boolean result1 = eval(filter, 1, 1);
                        boolean result2 = eval(reduced, 1, 1);
                        assertEquals(result1, result2);
                    }

                    {
                        boolean result1 = eval(filter, 1, 2);
                        boolean result2 = eval(reduced, 1, 2);
                        assertEquals(result1, result2);
                    }

                    {
                        boolean result1 = eval(filter, 2, 1);
                        boolean result2 = eval(reduced, 2, 1);
                        assertEquals(result1, result2);
                    }
                }
            }
        }
    }

    @Test
    public void operatorReductionMatrix() throws Exception {
        int handled = 0;

        for (int op1 = OP_EQ; op1 <= OP_GT; op1++) {
            for (int op2 = OP_EQ; op2 <= OP_GT; op2++) {
                int result = reduceOperatorForAnd(op1, op2);
                assertNotEquals(Integer.MIN_VALUE, result);

                int result2 = reduceOperatorForOr(flipOperator(op1), flipOperator(op2));
                result2 = flipOperator(result2);
                if (result2 == Integer.MAX_VALUE - 1) {
                    assertEquals(Integer.MAX_VALUE, result);
                } else {
                    assertEquals(result, result2);
                }

                if (op1 == op2) {
                    assertEquals(op1, result);
                    handled++;
                } else if (op1 == flipOperator(op2)) {
                    assertEquals(Integer.MAX_VALUE, result);
                    handled++;
                } else if (hasEqualComponent(op1)) {
                    if (hasEqualComponent(op2)) {
                        if (reverseOperator(op1) == op2) {
                            assertEquals(~OP_EQ, result);
                        } else {
                            assertTrue(op1 == OP_EQ || op2 == OP_EQ);
                        }
                        handled++;
                    } else {
                        if (op1 == OP_EQ) {
                            assertEquals(Integer.MAX_VALUE, result);
                            handled++;
                        } else if (op2 == OP_NE) {
                            assertEquals(result, ~removeEqualComponent(op1));
                            handled++;
                        } else {
                            assertEquals(result, removeEqualComponent(op1));
                            handled++;
                        }
                    }
                } else {
                    if (hasEqualComponent(op2)) {
                        if (op2 == OP_EQ) {
                            assertEquals(Integer.MAX_VALUE, result);
                            handled++;
                        } else if (op1 == OP_NE) {
                            assertEquals(result, ~removeEqualComponent(op2));
                            handled++;
                        } else {
                            assertEquals(result, removeEqualComponent(op2));
                            handled++;
                        }
                    } else {
                        if (reverseOperator(op1) == op2) {
                            assertEquals(Integer.MAX_VALUE, result);
                        } else {
                            assertTrue(op1 == OP_NE || op2 == OP_NE);
                        }
                        handled++;
                    }
                }
            }
        }

        assertEquals(36, handled);
    }

    @Test
    public void partialReduction() throws Exception {
        // The reduce method isn't guaranteed to be complete, but a call to dnf or cnf can help
        // the reduction along.

        String filterStr = "((a > ?2 && a > ?3) || (a > ?4)) && (a > ?2 && a > ?3)";
        RowFilter filter = parse(newColMap(), filterStr).reduce();

        assertEquals(5, filter.numTerms());
        assertFalse(filter.isDnf());
        assertFalse(filter.isCnf());

        for (int i=1; i<=3; i++) {
            RowFilter reduced = switch (i) {
            case 1 -> filter.dnf();
            case 2 -> filter.cnf();
            default -> filter.reduceMore();
            };
            assertEquals(2, reduced.numTerms());
            assertEquals("a > ?2 && a > ?3", reduced.toString());
        }
    }

    @Test
    public void dnfOperatorReduction() throws Exception {
        String[] cases = {
            "a > ?1 || (a >= ?1 && b > ?2)",
            "a > ?1 || (a == ?1 && b > ?2)",

            "(a > ?1 && b > ?2) || (a >= ?1 && b > ?2)",
            "(a > ?1 && b > ?2) || (a == ?1 && b > ?2)",

            "a > ?1 || (a >= ?1 && b > ?2) || (a >= ?1 && b == ?2 && c > ?3)",
            "a > ?1 || (a == ?1 && b > ?2) || (a == ?1 && b == ?2 && c > ?3)",

            "a > ?1 || (a >= ?1 && b > ?2) || (a == ?1 && b == ?2 && c > ?3)",
            "a > ?1 || (a == ?1 && b > ?2) || (a == ?1 && b == ?2 && c > ?3)",

            "a > ?1 || (a == ?1 && b > ?2) || (a == ?1 && b == ?2 && c > ?3)",
            "a > ?1 || (a == ?1 && b > ?2) || (a == ?1 && b == ?2 && c > ?3)",

            "a >= ?1 && (a > ?1 || b >= ?2) && (a > ?1 || b > ?2 || c > ?3)",
            "a > ?1 || (a == ?1 && b > ?2) || (a == ?1 && b == ?2 && c > ?3)",
        };

        var colMap = newColMap("a", "b", "c");

        for (int i=0; i<cases.length; i+=2) {
            RowFilter filter = parse(colMap, cases[i]);
            RowFilter dnf = filter.dnf();
            assertEquals(cases[i + 1], dnf.toString());
        }
    }

    private static boolean eval(RowFilter filter, int colValue, int argValue) {
        var visitor = new Visitor() {
            Boolean result;

            @Override
            public void visit(OrFilter filter) {
                RowFilter[] subFilters = filter.subFilters();
                if (subFilters.length == 0) {
                    assertEquals(FalseFilter.THE, filter);
                    result = false;
                } else {
                    subFilters[0].accept(this);
                    boolean r = result;
                    for (int i=0; i<subFilters.length; i++) {
                        subFilters[i].accept(this);
                        r |= result;
                    }
                    result = r;
                }
            }

            @Override
            public void visit(AndFilter filter) {
                RowFilter[] subFilters = filter.subFilters();
                if (subFilters.length == 0) {
                    assertEquals(TrueFilter.THE, filter);
                    result = true;
                } else {
                    subFilters[0].accept(this);
                    boolean r = result;
                    for (int i=0; i<subFilters.length; i++) {
                        subFilters[i].accept(this);
                        r &= result;
                    }
                    result = r;
                }
            }

            @Override
            public void visit(ColumnToArgFilter filter) {
                switch (filter.operator()) {
                    case OP_EQ -> result = colValue == argValue;
                    case OP_NE -> result = colValue != argValue;
                    case OP_LT -> result = colValue < argValue;
                    case OP_GE -> result = colValue >= argValue;
                    case OP_GT -> result = colValue > argValue;
                    case OP_LE -> result = colValue <= argValue;
                }
            }

            @Override
            public void visit(ColumnToColumnFilter filter) {
                fail();
            }
        };

        filter.accept(visitor);

        return visitor.result;
    }

    static RowFilter parse(Map<String, ColumnInfo> colMap, String filter) {
        return RowFilterTest.parse(colMap, filter);
    }
}
