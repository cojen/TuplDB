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

package org.cojen.tupl.remote;

import java.net.ServerSocket;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteRowTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteRowTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mServerDb = newTempDatabase(getClass());

        var ss = new ServerSocket(0);
        mServerDb.newServer().acceptAll(ss, 123456);

        mClientDb = Database.connect(ss.getLocalSocketAddress(), null, 123456);
    }

    @After
    public void teardown() throws Exception {
        if (mClientDb != null) {
            mClientDb.close();
            mClientDb = null;
        }

        mServerDb = null;

        deleteTempDatabases(getClass());
    }

    private Database mServerDb;
    private Database mClientDb;

    @Test
    public void predicate() throws Exception {
        Table<TestRow> table = mClientDb.openTable(TestRow.class);

        try {
            table.predicate(null);
            fail();
        } catch (NullPointerException e) {
        }

        var predicate = table.predicate("{*} id == ? || str1 == ?", 10, "hello");
        var row = table.newRow();

        try {
            predicate.test(row);
            fail();
        } catch (UnsetColumnException e) {
        }

        row.id(1);
        row.str1("hello");
        assertTrue(predicate.test(row));
        row.str1("hello!");
        assertFalse(predicate.test(row));
        row.id(10);
        assertTrue(predicate.test(row));
    }

    @PrimaryKey("id")
    public interface TestRow {
        long id();
        void id(long id);

        String str1();
        void str1(String str);

        @Nullable
        String str2();
        void str2(String str);

        int num1();
        void num1(int num);
    }
}
