/*
 *  Copyright (C) 2018 Cojen.org
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

import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.core.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SecondaryCacheTest {
    public static void main(String[] args) {
        org.junit.runner.JUnitCore.main(SecondaryCacheTest.class.getName());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
    }

    @Test
    public void durable() throws Exception {
        test(true, false, false);
    }

    @Test
    public void durableDirect() throws Exception {
        test(true, false, true);
    }

    @Test
    public void nonDurable() throws Exception {
        test(false, false, false);
    }

    @Test
    public void nonDurableDirect() throws Exception {
        test(false, false, true);
    }

    @Test
    public void nonDurableFull() throws Exception {
        test(false, true, false);
    }

    @Test
    public void nonDurableFullDirect() throws Exception {
        test(false, true, true);
    }

    private void test(boolean durable, boolean full, boolean direct) throws Exception {
        var config = new DatabaseConfig()
            .minCacheSize(1_000_000)
            .secondaryCacheSize(100_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .directPageAccess(direct);

        if (full) {
            // No need to show a panic if it happens in just the wrong place.
            config.eventListener(new EventPrinter().observe((EventType.Category) null));
        }

        Database db;
        if (durable) {
            db = newTempDatabase(getClass(), config);
        } else {
            db = Database.open(config);
        }

        Index ix = db.openIndex("test");

        var rnd = new Random(98653245);

        final int count = full ? 1_000_000 : 100_000;

        boolean filled = false;

        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 10);
            byte[] value = randomStr(rnd, 100);
            try {
                ix.store(null, key, value);
            } catch (DatabaseFullException e) {
                if (!full) {
                    throw e;
                }
                filled = true;
                break;
            }
        }

        db.close();

        if (full) {
            assertTrue(filled);
        }
    }
}
