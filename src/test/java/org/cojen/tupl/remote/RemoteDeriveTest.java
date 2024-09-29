/*
 *  Copyright (C) 2024 Cojen.org
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

import org.cojen.tupl.*;

import org.cojen.tupl.table.DeriveTest;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class RemoteDeriveTest extends DeriveTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemoteDeriveTest.class.getName());
    }

    private Database mServerDb;

    @Override
    protected void createTempDb() throws Exception {
        mServerDb = Database.open(new DatabaseConfig());

        var ss = new ServerSocket(0);
        mServerDb.newServer().acceptAll(ss, 123456);

        mDb = Database.connect(ss.getLocalSocketAddress(), null, 123456);
    }

    @Override
    protected boolean isLocalTest() {
        return false;
    }

    @After
    @Override
    public void teardown() throws Exception {
        if (mServerDb != null) {
            mServerDb.close();
            mServerDb = null;
        }
        super.teardown();
    }
}
