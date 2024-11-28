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

import java.util.Arrays;
import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

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

    private static DatabaseConfig newConfig() {
        return new DatabaseConfig().pageSize(65536);
    }

    @Test
    public void deleteLast() throws Exception {
        // When the search vector is at the tail of a leaf node, deleting the last entry
        // shouldn't cause the search vector start position to overflow. All the magic numbers
        // used in this test were determined through experimentation.

        var config = newConfig().checkpointRate(-1, null);
        Database db = newTempDatabase(getClass(), config);
        Index ix = db.openIndex("test");

        // Fill up one leaf node.
        for (int i=0; i<2730; i++) {
            byte[] key = ("key-" + (100000 + i)).getBytes();
            ix.store(Transaction.BOGUS, key, key);
        }

        Node root = ((BTree) ix).mRoot;
        assertTrue(root.isLeaf());
        assertEquals(4, root.availableBytes());

        // Delete one, and insert again to force a compaction.
        {
            byte[] key = "key-100000".getBytes();
            assertTrue(ix.delete(Transaction.BOGUS, key));
        }

        assertEquals(28, root.availableBytes());

        byte[] key = "key-200000xx".getBytes();
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
        assertTrue(db.verify(null, 1));
    }

    @Test
    public void largeHeader() throws Exception {
        // Values larger than 8192 bytes have a different header.

        Database db = Database.open(newConfig());
        Index ix = db.openIndex("test");
        var rnd = new Random(68598204);

        byte[] k1 = "k1".getBytes();
        byte[] k2 = "k2".getBytes();
        byte[] k3 = "k3".getBytes();

        byte[] v1 = randomStr(rnd, 8191);
        byte[] v2 = randomStr(rnd, 8192);
        byte[] v3 = randomStr(rnd, 8193);

        ix.store(null, k1, v1);
        ix.store(null, k2, v2);
        ix.store(null, k3, v3);

        fastAssertArrayEquals(v1, ix.load(null, k1));
        fastAssertArrayEquals(v2, ix.load(null, k2));
        fastAssertArrayEquals(v3, ix.load(null, k3));

        db.close();
    }

    @Test
    public void truncateExtend() throws Exception {
        // When truncating a fragmented value, the large value header can still be used.
        // Truncating a second time must still handle the case that the larger header used even
        // though it doesn't need to be. Note that the actual "value" that's stored in the node
        // is the fragmented value header, which includes direct pointers.

        Database db = newTempDatabase(getClass(), newConfig());
        Index ix = db.openIndex("test");
        var rnd = new Random(68598204);

        int length = 90_000_000;
        byte[] value = randomStr(rnd, length);

        // Store in multiple steps, to prevent generation of inline content.
        byte[] key = "key".getBytes();
        try (Cursor c = ix.newCursor(Transaction.BOGUS)) {
            c.find(key);
            for (int pos = 0; pos < length; pos += 1000) {
                c.valueWrite(pos, value, pos, 1000);
            }
        }

        fastAssertArrayEquals(value, ix.load(Transaction.BOGUS, key));

        for (int i=0; i<10; i++) {
            try (Cursor c = ix.newCursor(Transaction.BOGUS)) {
                c.find(key);
                length -= 1_000_000;
                c.valueLength(length);
            }

            value = Arrays.copyOfRange(value, 0, length);

            fastAssertArrayEquals(value, ix.load(Transaction.BOGUS, key));
        }

        // Extending the value must also cope with the larger header.

        value = Arrays.copyOfRange(value, 0, 90_000_000);
        int pos = length;
        while (length < 90_000_000) {
            value[length] = (byte) rnd.nextInt();
            length++;
        }

        try (Cursor c = ix.newCursor(Transaction.BOGUS)) {
            c.find(key);
            c.valueWrite(pos, value, pos, value.length - pos);
        }

        fastAssertArrayEquals(value, ix.load(Transaction.BOGUS, key));
    }
}
