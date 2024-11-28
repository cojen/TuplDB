/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.core;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * Tests specialized union view operations. See MergeViewTest for more tests.
 *
 * @author Brian S O'Neill
 */
public class UnionViewTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(UnionViewTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        var config = new DatabaseConfig();
        config.maxCacheSize(100_000_000);
        mDb = Database.open(config);
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
        mDb = null;
    }

    protected Database mDb;

    @Test
    public void doubleReverseCount() throws Exception {
        Index empty = mDb.openIndex("empty");
        Index ix = mDb.openIndex("test");

        byte[] k1 = {0};
        byte[] k2 = {0, 0};
        byte[] k3 = {0, 0, 0};

        ix.store(null, k1, k1);
        ix.store(null, k2, k2);
        ix.store(null, k3, k3);

        assertEquals(2, ix.count(k1, k3));

        View view = ix.viewReverse().viewUnion(Combiner.first(), empty.viewReverse());
        view = view.viewReverse();

        assertEquals(2, view.count(k1, k3));
    }
}
