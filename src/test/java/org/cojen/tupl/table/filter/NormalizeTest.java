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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.table.filter.ColumnFilter.*;

import org.cojen.tupl.table.ColumnInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class NormalizeTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(NormalizeTest.class.getName());
    }

    static Map<String, ColumnInfo> newColMap(int num) {
        var colMap = new HashMap<String, ColumnInfo>();
        for (int i=0; i<num; i++) {
            var info = new ColumnInfo();
            info.name = String.valueOf((char) ('a' + i));
            info.typeCode = ColumnInfo.TYPE_UTF8;
            info.assignType();
            colMap.put(info.name, info);
        }
        return colMap;
    }

    @Test
    public void complex() throws Throwable {
        // Generates complex filters and verifies that normalization completes or fails with a
        // ComplexFilterException.

        Map<String, ColumnInfo> colMap = newColMap(1);

        ExecutorService exec = Executors.newWorkStealingPool(4);

        var passed = new AtomicLong();
        var failed = new AtomicLong();
        var exceptions = new ConcurrentLinkedQueue<Throwable>();

        final var rnd = new Random(159274);
        int started = 0;

        for (int i=0; i<2000; i++) {
            RowFilter filter = randomFilter(rnd, colMap, 100, 10, 6, OP_NOT_IN);

            if (i < 1000) {
                // The interesting cases are generated after 1000.
                continue;
            }

            exec.execute(() -> {
                try {
                    filter.dnf();
                    passed.incrementAndGet();
                } catch (ComplexFilterException e) {
                    failed.incrementAndGet();
                } catch (Throwable e) {
                    exceptions.add(e);
                }
            });

            exec.execute(() -> {
                try {
                    filter.cnf();
                    passed.incrementAndGet();
                } catch (ComplexFilterException e) {
                    failed.incrementAndGet();
                } catch (Throwable e) {
                    exceptions.add(e);
                }
            });

            started += 2;
        }

        exec.shutdown();
        exec.awaitTermination(20, TimeUnit.MINUTES);

        for (Throwable ex : exceptions) {
            throw ex;
        }

        assertEquals(started, passed.get() + failed.get());
        assertTrue(passed.get() > failed.get());
    }

    @Test
    public void verify() throws Exception {
        Map<String, ColumnInfo> colMap = newColMap(4);
        final var rnd = new Random(159274);

        int numFilters = 1000;
        int numResults = 100;
        int dnfDiff = 0;
        int cnfDiff = 0;

        for (int i=0; i<numFilters; i++) {
            RowFilter filter = randomFilter(rnd, colMap, 4, 4, 4, OP_IN - 1);

            RowFilter dnf = filter.dnf();
            if (!dnf.equals(filter)) {
                dnfDiff++;
            }

            RowFilter cnf = filter.cnf();
            if (!cnf.equals(filter)) {
                cnfDiff++;
            }

            boolean[] results1 = eval(filter, 4, new Random(472951), numResults);

            boolean[] results2 = eval(dnf, 4, new Random(472951), numResults);
            assertTrue(Arrays.equals(results1, results2));

            boolean[] results3 = eval(cnf, 4, new Random(472951), numResults);
            assertTrue(Arrays.equals(results1, results3));
        }

        assertTrue(dnfDiff > (numFilters / 2));
        assertTrue(cnfDiff > (numFilters / 2));
    }

    static RowFilter randomFilter(Random rnd, Map<String, ColumnInfo> colMap, int numArgs,
                                  int width, int height, int maxOp)
    {
        if (height <= 1) {
            int op = rnd.nextInt(maxOp + 1);
            int arg = rnd.nextInt(numArgs);

            ColumnInfo column;
            {
                var it = colMap.values().iterator();
                int num = Math.min(arg, colMap.size() - 1);
                do {
                    column = it.next();
                } while (--num >= 0);
            }

            if (op < OP_IN) {
                return new ColumnToArgFilter(column, op, arg);
            } else {
                var filter = new InFilter(column, arg);
                if (op == OP_NOT_IN) {
                    filter = filter.not();
                }
                return filter;
            }
        }

        height--;
        var subFilters = new RowFilter[1 + rnd.nextInt(width)];

        for (int i=0; i<subFilters.length; i++) {
            subFilters[i] = randomFilter(rnd, colMap, numArgs, width, height, maxOp);
        }

        if (rnd.nextBoolean()) {
            return AndFilter.flatten(subFilters, 0, subFilters.length);
        } else {
            return OrFilter.flatten(subFilters, 0, subFilters.length);
        }
    }

    static boolean[] eval(RowFilter filter, int numArgs, Random rnd, int numResults) {
        var colValues = new int[numArgs];
        var argValues = new int[numArgs];

        var results = new boolean[numResults];

        for (int i=0; i<numResults; i++) {
            for (int j=0; j<colValues.length; j++) {
                colValues[j] = rnd.nextInt(2) - 1;
            }
            for (int j=0; j<argValues.length; j++) {
                argValues[j] = rnd.nextInt(2) - 1;
            }
            results[i] = eval(filter, colValues, argValues);
        }

        return results;
    }

    static boolean eval(RowFilter filter, int[] colValues, int[] argValues) {
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
                int colValue = colValues[filter.column().name.charAt(0) - 'a'];
                int argValue = argValues[filter.argument()];

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
}
