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

import java.util.ArrayList;
import java.util.List;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ComparatorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ComparatorTest.class.getName());
    }

    private Database mDb;
    private Table<A> A;
    private Table<B> B;
    private Table<A_B> A_B;

    @Before
    public void setup() throws Exception {
        mDb = Database.open(new DatabaseConfig());
        A = mDb.openTable(A.class);
        B = mDb.openTable(B.class);
        A_B = mDb.openJoinTable(A_B.class, "a >:< b");

        {
            A a = A.newRow();

            a.id(1);
            a.value(1);
            A.insert(null, a);

            a.id(2);
            a.value(null);
            A.insert(null, a);

            a.id(3);
            a.value(3);
            A.insert(null, a);

            a.id(4);
            a.value(4);
            A.insert(null, a);
        }

        {
            B b = B.newRow();

            b.id(1);
            b.value(10);
            B.insert(null, b);

            b.id(2);
            b.value(5);
            B.insert(null, b);

            b.id(5);
            b.value(1);
            B.insert(null, b);
        }
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
    }

    @PrimaryKey("id")
    public static interface A {
        int id();
        void id(int id);

        @Nullable
        Integer value();
        void value(Integer value);
    }

    @PrimaryKey("id")
    public static interface B {
        int id();
        void id(int id);

        int value();
        void value(int value);
    }

    @Test
    public void basic() throws Exception {
        List<A_B> all = new ArrayList<>(A_B.newStream(null, "a.id == b.id").toList());

        {
            all.sort(A_B.comparator("+a"));
            var it = all.iterator();
            A_B row = it.next();
            Integer aId_last = aId(row);
            while (it.hasNext()) {
                row = it.next();
                Integer aId = aId(row);
                assertTrue(compare(aId_last, aId, true) <= 0);
                aId_last = aId;
            }
        }

        {
            all.sort(A_B.comparator("-a"));
            var it = all.iterator();
            A_B row = it.next();
            Integer aId_last = aId(row);
            while (it.hasNext()) {
                row = it.next();
                Integer aId = aId(row);
                assertTrue(compare(aId_last, aId, true) >= 0);
                aId_last = aId;
            }
        }

        {
            all.sort(A_B.comparator("+!a"));
            var it = all.iterator();
            A_B row = it.next();
            Integer aId_last = aId(row);
            while (it.hasNext()) {
                row = it.next();
                Integer aId = aId(row);
                assertTrue(compare(aId_last, aId, false) <= 0);
                aId_last = aId;
            }
        }

        {
            all.sort(A_B.comparator("-!a"));
            var it = all.iterator();
            A_B row = it.next();
            Integer aId_last = aId(row);
            while (it.hasNext()) {
                row = it.next();
                Integer aId = aId(row);
                assertTrue(compare(aId_last, aId, false) >= 0);
                aId_last = aId;
            }
        }

        {
            all.sort(A_B.comparator("+b.value"));
            var it = all.iterator();
            A_B row = it.next();
            Integer bValue_last = bValue(row);
            while (it.hasNext()) {
                row = it.next();
                Integer bValue = bValue(row);
                assertTrue(compare(bValue_last, bValue, true) <= 0);
                bValue_last = bValue;
            }
        }

        {
            all.sort(A_B.comparator("-b.value"));
            var it = all.iterator();
            A_B row = it.next();
            Integer bValue_last = bValue(row);
            while (it.hasNext()) {
                row = it.next();
                Integer bValue = bValue(row);
                assertTrue(compare(bValue_last, bValue, true) >= 0);
                bValue_last = bValue;
            }
        }

        {
            all.sort(A_B.comparator("+!b.value"));
            var it = all.iterator();
            A_B row = it.next();
            Integer bValue_last = bValue(row);
            while (it.hasNext()) {
                row = it.next();
                Integer bValue = bValue(row);
                assertTrue(compare(bValue_last, bValue, false) <= 0);
                bValue_last = bValue;
            }
        }

        {
            all.sort(A_B.comparator("-!b.value"));
            var it = all.iterator();
            A_B row = it.next();
            Integer bValue_last = bValue(row);
            while (it.hasNext()) {
                row = it.next();
                Integer bValue = bValue(row);
                assertTrue(compare(bValue_last, bValue, false) >= 0);
                bValue_last = bValue;
            }
        }

        {
            all.sort(A_B.comparator("+b.value-a.id"));
            var it = all.iterator();
            A_B row = it.next();
            Integer bValue_last = bValue(row);
            while (it.hasNext()) {
                row = it.next();
                Integer bValue = bValue(row);
                if (bValue == null) {
                    break;
                }
                assertTrue(compare(bValue_last, bValue, true) < 0);
                bValue_last = bValue;
            }
            Integer aId_last = aId(row);
            while (it.hasNext()) {
                row = it.next();
                Integer aId = aId(row);
                assertTrue(compare(aId_last, aId, true) > 0);
                aId_last = aId;
            }
        }
    }

    private static Integer aId(A_B row) {
        A a = row.a();
        return a == null ? null : a.id();
    }

    private static Integer bValue(A_B row) {
        B b = row.b();
        return b == null ? null : b.value();
    }

    private static int compare(Integer a, Integer b, boolean nullHigh) {
        if (a == null) {
            return b == null ? 0 : (nullHigh ? 1 : -1);
        } else if (b == null) {
            return nullHigh ? -1 : 1;
        } else {
            return a.compareTo(b);
        }
    }

    public static interface A_B {
        @Nullable
        A a();
        void a(A a);

        @Nullable
        B b();
        void b(B b);
    }
}
