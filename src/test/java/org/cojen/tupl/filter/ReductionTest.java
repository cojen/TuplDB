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

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.filter.ColumnFilter.*;

import org.cojen.tupl.rows.ColumnInfo;

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
        var colMap = new HashMap<String, ColumnInfo>();
        var info = new ColumnInfo();
        info.name = "a";
        info.typeCode = ColumnInfo.TYPE_UTF8;
        info.assignType();
        colMap.put(info.name, info);
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
                    String filterStr = "a" + relOp1 + "?0" + op + "a" + relOp2 + "?0";
                    RowFilter filter = new Parser(colMap, filterStr).parse();
                    RowFilter reduced = filter.reduce();

                    assertNotEquals(filter, reduced);

                    if (reduced instanceof GroupFilter) {
                        assertEquals(0, ((GroupFilter) reduced).subFilters().length);
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
                        if (descendingOperator(op1) == op2) {
                            assertEquals(~OP_EQ, result);
                            handled++;
                        } else {
                            assertTrue(op1 == OP_EQ || op2 == OP_EQ);
                            handled++;
                        }
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
                        if (descendingOperator(op1) == op2) {
                            assertEquals(Integer.MAX_VALUE, result);
                            handled++;
                        } else {
                            assertTrue(op1 == OP_NE || op2 == OP_NE);
                            handled++;
                        }
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

        String filterStr = "((a > ?1 && a > ?2) || (a > ?3)) && (a > ?1 && a > ?2)";
        RowFilter filter = new Parser(newColMap(), filterStr).parse().reduce();

        assertEquals(5, filter.numTerms());
        assertFalse(filter.isDnf());
        assertFalse(filter.isCnf());

        for (int i=0; i<2; i++) {
            RowFilter reduced = i == 0 ? filter.dnf() : filter.cnf();
            assertEquals(2, reduced.numTerms());
            assertEquals("a > ?1 && a > ?2", reduced.toString());
        }
    }

    private static boolean eval(RowFilter filter, int colValue, int argValue) {
        var visitor = new Visitor() {
            Boolean result;

            @Override
            public void visit(OrFilter filter) {
                RowFilter[] subFilters = filter.subFilters();
                if (subFilters.length == 0) {
                    assertEquals(OrFilter.FALSE, filter);
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
                    assertEquals(AndFilter.TRUE, filter);
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
                case OP_EQ: result = colValue == argValue; break;
                case OP_NE: result = colValue != argValue; break;
                case OP_LT: result = colValue < argValue; break;
                case OP_GE: result = colValue >= argValue; break;
                case OP_GT: result = colValue > argValue; break;
                case OP_LE: result = colValue <= argValue; break;
                }
            }
        };

        filter.accept(visitor);

        return visitor.result;
    }
}
