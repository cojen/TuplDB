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
        deleteTempDatabases(getClass());
    }

    @Test
    public void nonStandardPageSize() throws Exception {
        DatabaseConfig config = new DatabaseConfig()
            .pageSize(512).durabilityMode(DurabilityMode.NO_FLUSH);
        config = decorate(config);

        Database db = newTempDatabase(getClass(), config);
        assertEquals(512, db.stats().pageSize());
        db.shutdown();

        // Page size not explicitly set, so use existing page size.
        config = new DatabaseConfig().durabilityMode(DurabilityMode.NO_FLUSH);
        config = decorate(config);

        db = reopenTempDatabase(getClass(), db, config);
        assertEquals(512, db.stats().pageSize());
        db.shutdown();

        config = new DatabaseConfig()
            .eventListener((type, message, args) -> {}) // hide uncaught exception
            .pageSize(4096)
            .durabilityMode(DurabilityMode.NO_FLUSH);
        config = decorate(config);

        try {
            db = reopenTempDatabase(getClass(), db, config);
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

        Database db = newTempDatabase(getClass(), config);
        assertEquals(512, db.stats().pageSize());

        Snapshot snap = db.beginSnapshot();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        snap.writeTo(out);
        db.close();

        // Page size not explicitly set, so use existing page size.
        config = new DatabaseConfig()
            .baseFile(baseFileForTempDatabase(getClass(), db))
            .durabilityMode(DurabilityMode.NO_FLUSH);
        config = decorate(config);

        ByteArrayInputStream in = new ByteArrayInputStream(out.toByteArray());
        db = Database.restoreFromSnapshot(config, in);

        assertEquals(512, db.stats().pageSize());

        db.close();
    }
}
