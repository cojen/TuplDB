/*
 *  Copyright 2019 Cojen.org
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

import java.io.File;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * Test conversion to/from replicated mode.
 *
 * @author Brian S O'Neill
 */
public class ConvertTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ConvertTest.class.getName());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
    }

    @Test
    public void toReplicatedFail1() throws Exception {
        // Cannot convert when redo log files exist.

        Database db = newTempDatabase(getClass());

        var newConfig = new DatabaseConfig().replicate(new NonReplicator());

        try {
            db = reopenTempDatabase(getClass(), db, newConfig);
            fail();
        } catch (DatabaseException e) {
            assertTrue(e.getMessage().contains("redo log files exist"));
        }
    }

    @Test
    public void toReplicatedFail2() throws Exception {
        // Cannot convert without complete replication data.

        Database db = newTempDatabase(getClass());

        var repl = new NonReplicator();
        repl.setInitialPosition(100);

        var newConfig = new DatabaseConfig().replicate(repl);

        try {
            db = reopenTempDatabase(getClass(), db, newConfig);
            fail();
        } catch (DatabaseException e) {
            assertTrue(e.getMessage().contains("without complete replication"));
        }
    }

    @Test
    public void toReplicated() throws Exception {
        Database db = newTempDatabase(getClass());
        Index ix = db.openIndex("test");
        ix.store(null, "hello".getBytes(), "world".getBytes());
        db.shutdown();

        var newConfig = new DatabaseConfig().replicate(new NonReplicator());

        db = reopenTempDatabase(getClass(), db, newConfig);

        ix = db.openIndex("test");
        fastAssertArrayEquals("world".getBytes(), ix.load(null, "hello".getBytes()));
    }

    @Test
    public void toNonReplicatedFail() throws Exception {
        // Cannot convert to non-replicated mode without special steps.

        var config = new DatabaseConfig().replicate(new NonReplicator());
        Database db = newTempDatabase(getClass(), config);
        db.shutdown();

        config.replicate((NonReplicator) null);

        try {
            db = reopenTempDatabase(getClass(), db, config);
            fail();
        } catch (DatabaseException e) {
            assertTrue(e.getMessage().contains("replicator"));
        }
    }

    @Test
    public void toNonReplicated() throws Exception {
        // Only need to touch a redo log file to allow conversion. It will be deleted
        // automatically.

        var config = new DatabaseConfig().replicate(new NonReplicator());
        Database db = newTempDatabase(getClass(), config);
        File base = baseFileForTempDatabase(getClass(), db);
        db.shutdown();

        config.replicate((NonReplicator) null);

        new File(base.getPath() + ".redo.0").createNewFile();

        db = reopenTempDatabase(getClass(), db, config);
    }
}
