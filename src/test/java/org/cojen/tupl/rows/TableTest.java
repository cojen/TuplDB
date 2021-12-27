/*
 *  Copyright (C) 2021 Cojen.org
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

package org.cojen.tupl.rows;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;
import org.cojen.tupl.core.CoreDatabase;

import static org.cojen.tupl.TestUtils.*;

import static org.cojen.tupl.rows.RowTestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TableTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(TableTest.class.getName());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
    }

    @Test
    public void deleteTable() throws Exception {
        deleteTable(false, false, false);
    }

    @Test
    public void deleteTableRecover() throws Exception {
        deleteTable(true, false, false);
    }

    @Test
    public void deleteTableRecoverWithCheckpoint() throws Exception {
        deleteTable(true, false, true);
        teardown();
        deleteTable(true, true, false);
        teardown();
        deleteTable(true, true, true);
    }

    private void deleteTable(boolean stall, boolean checkpoint1, boolean checkpoint2)
        throws Exception
    {
        // Verifies that when a table's index is deleted, all metadata is deleted too.

        var config = new DatabaseConfig().directPageAccess(false);
        //config.eventListener(EventListener.printTo(System.out));
        var db = (CoreDatabase) newTempDatabase(getClass(), config);

        RowStore rs = db.rowStore();
        if (stall) {
            // Test mode only.
            rs.mStallTasks = true;
        }

        Index ix = db.openIndex("test");
        long indexId = ix.id();
        Table<TestRow> table = ix.asTable(TestRow.class);

        if (checkpoint1) {
            db.checkpoint();
        }

        db.deleteIndex(ix).run();

        try {
            TestRow row = table.newRow();
            row.id(1);
            row.name("name-1");
            table.store(null, row);
            fail();
        } catch (DeletedIndexException e) {
        }

        Index schemata = rs.schemata();
        var idBytes = new byte[8];
        RowUtils.encodeLongBE(idBytes, 0, indexId);

        if (stall) {
            assertFalse(schemata.viewPrefix(idBytes, 8).isEmpty());

            if (checkpoint2) {
                db.checkpoint();
            }

            db = (CoreDatabase) reopenTempDatabase(getClass(), db, config);
            rs = db.rowStore();
            schemata = rs.schemata();
        }

        for (int i=100; --i>=0; ) {
            try {
                assertTrue(schemata.viewPrefix(idBytes, 8).isEmpty());
                break;
            } catch (AssertionError e) {
                if (i == 0) {
                    throw e;
                }
            }
            sleep(100);
        }
    }

    private void dump(View ix) throws Exception {
        try (var c = ix.newCursor(null)) {
            for (c.first(); c.key() != null; c.next()) {
                System.out.println(RowUtils.toHex(c.key()) + " -> " + RowUtils.toHex(c.value()));
            }
        }
    }

    @PrimaryKey("id")
    public interface TestRow {
        long id();
        void id(long id);

        String name();
        void name(String str);
    }
}
