/*
 *  Copyright (C) 2011-2017 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl;

import org.junit.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CloseNonDurableTest extends CloseTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CloseNonDurableTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mDb = Database.open(new DatabaseConfig().directPageAccess(false));
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    @Test @Ignore
    public void cacheChurn() {
        // Non-durable database cannot exhaust the cache.
    }

    @Test
    public void dropChurn() throws Exception {
        // Verify that dropping indexes allows space to be reclaimed.

        for (int i=0; i<10000; i++) {
            Index ix = mDb.openIndex("ix-" + i);
            byte[] key = "hello".getBytes();
            ix.store(null, key, ("world-" + i).getBytes());
            ix.store(null, key, null);
            ix.drop();
        }
    }
}
