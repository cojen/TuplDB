/*
 *  Copyright 2012-2015 Cojen.org
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

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class LargeValueTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LargeValueTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mDb = newTempDatabase();
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
        mDb = null;
    }

    protected Database mDb;

    @Test
    public void testStoreBasic() throws Exception {
        Random rnd = new Random(82348976232L);
        int[] sizes = {1000, 2000, 3000, 4000, 5000, 6000, 10000, 100000};

        for (int size : sizes) {
            testStoreBasic(rnd, size, null);
            testStoreBasic(rnd, size, Transaction.BOGUS);
            testStoreBasic(rnd, size, mDb.newTransaction());
        }
    }

    private void testStoreBasic(Random rnd, int size, Transaction txn) throws Exception {
        Index ix = mDb.openIndex("test");

        try {
            ix.store(txn, null, null);
            fail();
        } catch (NullPointerException e) {
            // Expected.
        }

        byte[] key = "hello".getBytes();
        byte[] key2 = "howdy".getBytes();

        byte[] value = randomStr(rnd, size);
        byte[] value2 = randomStr(rnd, size);

        ix.store(txn, key, value);
        fastAssertArrayEquals(value, ix.load(txn, key));

        ix.store(txn, key, value2);
        fastAssertArrayEquals(value2, ix.load(txn, key));

        assertNull(ix.load(txn, key2));

        ix.store(txn, key, null);
        assertNull(ix.load(txn, key));

        if (txn != null && txn != Transaction.BOGUS) {
            ix.store(txn, key, value);
            txn.commit();
            fastAssertArrayEquals(value, ix.load(txn, key));

            ix.store(txn, key, value2);
            txn.commit();
            fastAssertArrayEquals(value2, ix.load(txn, key));

            ix.store(txn, key, value);
            txn.exit();
            fastAssertArrayEquals(value2, ix.load(txn, key));
            txn.exit();
        }
    }

    @Test
    public void testUpdateLarger() throws Exception {
        // Special regresion test, discovered when updating a fragmented value
        // to be slightly larger. The inline content length was between 1 and
        // 128 before and after. The node was split to make room, but the split
        // direction guess was wrong. This caused updateLeafValue to be called
        // twice, and the second time it computed the encoded length without
        // considering that the value is fragmented. The value header must be
        // at least 2 bytes for fragmented values, but the 1 byte format was
        // assumed instead. This corrupted the node.

        Index ix = mDb.openIndex("test");

        int c1 = 200;
        int c2 = 150;

        for (int i=0; i<c1; i++) {
            ix.store(Transaction.BOGUS, key(i), new byte[0]);
        }

        byte[] k = key(c1);
        // Assuming 4096 byte nodes, the inline content length is between 1 and 128.
        ix.store(Transaction.BOGUS, k, new byte[4111]);

        for (int i=0; i<c2; i++) {
            int n = c1 + i + 1;
            ix.store(Transaction.BOGUS, key(n), new byte[10]);
        }

        // Assuming 4096 byte nodes, the inline content length is still between 1 and 128.
        ix.store(Transaction.BOGUS, k, new byte[4121]);

        assertTrue(ix.verify(null));
    }

    private static byte[] key(int i) {
        byte[] key = new byte[4];
        Utils.encodeIntBE(key, 0, i);
        return key;
    }

    @Test
    public void largePageRecycle() throws Exception {
        // Tests that as large pages with user content are recycled, that the header fields of
        // new tree nodes are defined properly.

        Index ix = mDb.openIndex("test");

        {
            byte[] value = new byte[4_000_000];
            Arrays.fill(value, (byte) 0x55);
            ix.store(Transaction.BOGUS, "hello".getBytes(), value);
        }

        for (int i=0; i<1_000_000; i++) {
            ix.store(Transaction.BOGUS, ("key-" + i).getBytes(), (("value-" + i).getBytes()));
        }

        for (int i=0; i<1_000_000; i++) {
            byte[] value = ix.load(Transaction.BOGUS, ("key-" + i).getBytes());
            fastAssertArrayEquals(("value-" + i).getBytes(), value);
        }

        // Now test undo log nodes.

        {
            byte[] value = new byte[4_000_000];
            Arrays.fill(value, (byte) 0x55);
            ix.store(Transaction.BOGUS, "world".getBytes(), value);
        }

        Transaction txn = mDb.newTransaction();

        for (int i=0; i<1_000_000; i++) {
            ix.store(txn, ("akey-" + i).getBytes(), (("avalue-" + i).getBytes()));
        }

        txn.exit();

        for (int i=0; i<1_000_000; i++) {
            byte[] value = ix.load(Transaction.BOGUS, ("akey-" + i).getBytes());
            assertNull(value);
        }
    }
}
