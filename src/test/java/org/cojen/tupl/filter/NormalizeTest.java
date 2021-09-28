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
import java.util.Random;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicLong;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.filter.ColumnFilter.*;

import org.cojen.tupl.rows.ColumnInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class NormalizeTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(NormalizeTest.class.getName());
    }

    private static Map<String, ColumnInfo> newColMap() {
        var colMap = new HashMap<String, ColumnInfo>();
        var info = new ColumnInfo();
        info.name = "a";
        info.typeCode = ColumnInfo.TYPE_UTF8;
        info.assignType();
        colMap.put(info.name, info);
        return colMap;
    }

    @Test
    public void complex() throws Throwable {
        // Generates complex filters and verifies that normalization completes or fails with a
        // ComplexFilterException.

        Map<String, ColumnInfo> colMap = newColMap();

        ExecutorService exec = Executors.newWorkStealingPool(4);

        var passed = new AtomicLong();
        var failed = new AtomicLong();
        var exceptions = new ConcurrentLinkedQueue<Throwable>();

        final var rnd = new Random(159274);
        int started = 0;

        for (int i=0; i<2000; i++) {
            RowFilter filter = randomFilter(rnd, colMap.get("a"), 100, 10, 6);

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

    static RowFilter randomFilter(Random rnd, ColumnInfo column, int numArgs,
                                  int width, int height)
    {
        if (height <= 1) {
            int op = rnd.nextInt(OP_NOT_IN + 1);
            int arg = rnd.nextInt(numArgs);
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
            subFilters[i] = randomFilter(rnd, column, numArgs, width, height);
        }

        if (rnd.nextBoolean()) {
            return AndFilter.flatten(subFilters, 0, subFilters.length);
        } else {
            return OrFilter.flatten(subFilters, 0, subFilters.length);
        }
    }
}
