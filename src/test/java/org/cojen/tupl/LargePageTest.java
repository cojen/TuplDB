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

package org.cojen.tupl;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * Special tests when using the largest page size, 65536 bytes.
 *
 * @author Brian S O'Neill
 */
public class LargePageTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LargePageTest.class.getName());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
    }

    @Test
    public void deleteLast() throws Exception {
        // When the search vector is at the tail of a leaf node, deleting the last entry
        // should't cause the search vector start position to overflow. All the magic numbers
        // used in this test were determined through experimentation.

        DatabaseConfig config = new DatabaseConfig()
            .directPageAccess(false).checkpointRate(-1, null).pageSize(65536);
        Database db = newTempDatabase(getClass(), config);
        Index ix = db.openIndex("test");

        // Fill up one leaf node.
        for (int i=0; i<2730; i++) {
            byte[] key = ("key-" + (100000 + i)).getBytes();
            ix.store(Transaction.BOGUS, key, key);
        }

        Node root = ((Tree) ix).mRoot;
        assertTrue(root.isLeaf());
        assertEquals(4, root.availableBytes());

        // Delete one, and insert again to force a compaction.
        {
            byte[] key = "key-100000".getBytes();
            assertTrue(ix.delete(Transaction.BOGUS, key));
        }

        assertEquals(28, root.availableBytes());

        byte[] key = "key-200000xx".getBytes();
        byte[] value = key;
        ix.store(Transaction.BOGUS, key, key);

        // Leaf node is now packed, and the search vector is at the tail.
        assertEquals(0, root.availableBytes());
        assertEquals(65534, root.searchVecEnd());

        // Delete everything until empty.
        Cursor c = ix.newCursor(Transaction.BOGUS);
        c.autoload(false);
        for (c.first(); c.key() != null; c.next()) {
            c.store(null);
        }

        assertEquals(65524, root.availableBytes());
        // Note: Before fixing this bug, searchVecStart was 65536 (out of bounds) and the
        // database was corrupt when re-opened, as detected by the verifier.
        assertEquals(65534, root.searchVecStart());
        assertEquals(65532, root.searchVecEnd());

        db.checkpoint();
        db = reopenTempDatabase(getClass(), db, config);
        assertTrue(db.verify(null));
    }
}
