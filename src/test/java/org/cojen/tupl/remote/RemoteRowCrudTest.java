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

import org.cojen.tupl.table.RowCrudTest;
import org.cojen.tupl.table.RowTestUtils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RemoteRowCrudTest extends RowCrudTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteRowCrudTest.class.getName());
    }

    @Before
    @Override
    public void setup() throws Exception {
        setup(false);
    }

    protected void setup(boolean convert) throws Exception {
        mServerDb = Database.open(new DatabaseConfig());
        Server server = mServerDb.newServer();

        if (convert) {
            // Use a different row type definition on the server (num1 is different).
            Class<?> rowType = RowTestUtils.newRowType(TestRow.class.getName(),
                                                       long.class, "+id",
                                                       String.class, "str1",
                                                       String.class, "str2?",
                                                       long.class, "num1");

            server.classResolver(name -> name.equals(rowType.getName()) ? rowType : null);
        }

        var ss = new ServerSocket(0);
        server.acceptAll(ss, 123456);

        mDb = Database.connect(ss.getLocalSocketAddress(), null, 111, 123456);
        mTable = mDb.openTable(TestRow.class);
    }

    @After
    @Override
    public void teardown() throws Exception {
        if (mDb != null) {
            mDb.close();
            mDb = null;
        }
        if (mServerDb != null) {
            mServerDb.close();
            mServerDb = null;
        }
    }

    private Database mServerDb;
}
