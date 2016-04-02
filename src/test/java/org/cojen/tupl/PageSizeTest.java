/*
 *  Copyright 2013-2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import java.io.*;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class PageSizeTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(PageSizeTest.class.getName());
    }

    protected DatabaseConfig decorate(DatabaseConfig config) throws Exception {
        config.directPageAccess(false);
        return config;
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
    }

    @Test
    public void nonStandardPageSize() throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .pageSize(512).durabilityMode(DurabilityMode.NO_FLUSH);
        config = decorate(config);

        Database db = newTempDatabase(config);
        assertEquals(512, db.stats().pageSize());
        db.shutdown();

        // Page size not explicitly set, so use existing page size.
        config = new DatabaseConfig().durabilityMode(DurabilityMode.NO_FLUSH);
        config = decorate(config);

        db = reopenTempDatabase(db, config);
        assertEquals(512, db.stats().pageSize());
        db.shutdown();

        config = new DatabaseConfig()
            .eventListener((type, message, args) -> {}) // hide uncaught exception
            .pageSize(4096)
            .durabilityMode(DurabilityMode.NO_FLUSH);
        config = decorate(config);

        try {
            db = reopenTempDatabase(db, config);
            fail();
        } catch (DatabaseException e) {
            // Page size differs.
        }
    }

    @Test
    public void restore() throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .pageSize(512).durabilityMode(DurabilityMode.NO_FLUSH);
        config = decorate(config);

        Database db = newTempDatabase(config);
        assertEquals(512, db.stats().pageSize());

        Snapshot snap = db.beginSnapshot();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        snap.writeTo(out);
        db.close();

        // Page size not explicitly set, so use existing page size.
        config = new DatabaseConfig()
            .baseFile(baseFileForTempDatabase(db))
            .durabilityMode(DurabilityMode.NO_FLUSH);
        config = decorate(config);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        db = Database.restoreFromSnapshot(config, in);

        assertEquals(512, db.stats().pageSize());

        db.close();
    }
}
