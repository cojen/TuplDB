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
        mDb = newTempDatabase(getClass());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
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
        // Special regression test, discovered when updating a fragmented value
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

    @Test
    public void testUpdateLargerAgain() throws Exception {
        // Another update regression test. Update must not double split the node.

        Index ix = mDb.openIndex("test");

        Random rnd = new Random(123456);

        byte[][] keys = new byte[4][];
        byte[][] values = new byte[keys.length][];

        for (int i=0; i<keys.length; i++) {
            keys[i] = new byte[300];
            rnd.nextBytes(keys[i]);
            // Define key ordering.
            keys[i][0] = (byte) i;
        }

        // Store carefully crafted entries, to force an incorrectly balanced split.
        int[] sizes = {2200, 10, 300, 300};
        assertEquals(keys.length, sizes.length);
        for (int i=0; i<keys.length; i++) { 
            values[i] = new byte[sizes[i]];
            rnd.nextBytes(values[i]);
            ix.store(Transaction.BOGUS, keys[i], values[i]);
        }

        // Update a value to be larger, forcing a split.
        values[1] = new byte[2570];
        rnd.nextBytes(values[1]);
        ix.store(Transaction.BOGUS, keys[1], values[1]);

        // Verify that all the entries are correct.
        assertEquals(keys.length, ix.count(null, null));
        for (int i=0; i<keys.length; i++) {
            byte[] actual = ix.load(Transaction.BOGUS, keys[i]);
            fastAssertArrayEquals(values[i], actual);
        }
    }

    @Test
    public void testUpdateLargerAgainAgain() throws Exception {
        // Yet another update regression test. Update of large value into a split node must
        // consider the key size when fragmenting.

        Index ix = mDb.openIndex("test");

        Random rnd = new Random(123456);

        byte[][] keys = new byte[4][];
        byte[][] values = new byte[keys.length][];

        for (int i=0; i<keys.length; i++) {
            keys[i] = new byte[300];
            rnd.nextBytes(keys[i]);
            // Define key ordering.
            keys[i][0] = (byte) i;
        }

        // Store carefully crafted entries, to force an incorrectly balanced split.
        int[] sizes = {1720, 0, 840, 300};
        assertEquals(keys.length, sizes.length);
        for (int i=0; i<keys.length; i++) { 
            values[i] = new byte[sizes[i]];
            rnd.nextBytes(values[i]);
            ix.store(Transaction.BOGUS, keys[i], values[i]);
        }

        // Update a value to be larger, forcing a split. Value will be inline encoded if key
        // size isn't considered, and it won't fit.
        values[1] = new byte[2030];
        rnd.nextBytes(values[1]);
        ix.store(Transaction.BOGUS, keys[1], values[1]);

        // Verify that all the entries are correct.
        assertEquals(keys.length, ix.count(null, null));
        for (int i=0; i<keys.length; i++) {
            byte[] actual = ix.load(Transaction.BOGUS, keys[i]);
            fastAssertArrayEquals(values[i], actual);
        }
    }

    @Test
    public void testInsertLargeKeyOverflow() throws Exception {
        // Regression test. Insert a large key/value pair which causes an imbalanced split and
        // also forces the value to be fragmented. Due to a bug, the fragmented key was not
        // accounted for properly, and so the allocated space for the new entry was calculated
        // incorrectly. It was one byte smaller, and the value overflowed the entry slot. This
        // caused an IndexOutOfBoundsException or it would corrupt the node.

        Index ix = mDb.openIndex("test");

        ix.store(null, key(1), new byte[2005]);
        ix.store(null, key(3), new byte[2000]);
        ix.store(null, key(2500, 2), new byte[3000]);

        // Without the fix, this would fail because the garbage field didn't match actual usage.
        assertTrue(ix.verify(new VerificationObserver()));
    }

    @Test
    public void testInsertLargeKeyNoFit() throws Exception {
        // Regression test. Insert a large key/value pair which causes an imbalanced split and
        // also forces the value to be fragmented. The fragmented value should be placed into
        // the node that has more space.

        Index ix = mDb.openIndex("test");

        ix.store(null, key(1), new byte[11]);
        ix.store(null, key(2), new byte[3010]);
        ix.store(null, key(4), new byte[1020]);

        // Without the fix, this would cause an assertion error to be thrown.
        ix.store(null, key(1030, 3), new byte[2025]);
    }

    @Test
    public void testInsertLargeKeyLateFragment() throws Exception {
        // Create a split imbalance which causes the inserted value to be stored into the
        // original left node, and it must be fragmented to fit.

        Index ix = mDb.openIndex("test");

        ix.store(null, key(500, 1), new byte[10]);
        ix.store(null, key(500, 2), new byte[10]);
        ix.store(null, key(1500, 4), new byte[10]);
        ix.store(null, key(1500, 5), new byte[10]);

        ix.store(null, key(3), new byte[3050]);

        assertTrue(ix.verify(new VerificationObserver()));
    }

    @Test
    public void testInsertLargeKeyLateFragment2() throws Exception {
        // Create a split imbalance which causes the inserted value to be stored into the
        // original right node, and it must be fragmented to fit.

        Index ix = mDb.openIndex("test");

        ix.store(null, key(1020, 0), new byte[10]);
        ix.store(null, key(800, 2), new byte[10]);
        ix.store(null, key(800, 4), new byte[10]);
        ix.store(null, key(800, 6), new byte[10]);
        ix.store(null, key(800, 8), new byte[10]);

        ix.store(null, key(1), new byte[3050]);

        assertTrue(ix.verify(new VerificationObserver()));
    }

    @Test
    public void testUpdateLargeKeyLateFragment() throws Exception {
        // Create a split imbalance which causes the updated value to be stored into the
        // original left node, and it must be fragmented to fit.

        Index ix = mDb.openIndex("test");

        ix.store(null, key(1500, 1), new byte[10]);
        ix.store(null, key(1500, 2), new byte[10]);
        ix.store(null, key(3), new byte[0]);
        ix.store(null, key(500, 4), new byte[10]);
        ix.store(null, key(500, 5), new byte[10]);

        ix.store(null, key(3), new byte[3050]);

        assertTrue(ix.verify(new VerificationObserver()));
    }

    @Test
    public void testUpdateLargeKeyLateFragment2() throws Exception {
        // Create a split imbalance which causes the updated value to be stored into the
        // original right node, and it must be fragmented to fit.

        Index ix = mDb.openIndex("test");

        ix.store(null, key(1500, 2), new byte[10]);
        ix.store(null, key(3), new byte[0]);
        ix.store(null, key(500, 4), new byte[10]);
        ix.store(null, key(1500, 5), new byte[10]);

        ix.store(null, key(3), new byte[3050]);

        assertTrue(ix.verify(new VerificationObserver()));
    }

    @Test
    public void testUpdateLargeKeyLateFragment3() throws Exception {
        // Create a split imbalance which causes the updated value to be stored into the
        // original right node, and it must be fragmented to fit. This is a regression test
        // which ensures that some entries remain in the original node.

        Index ix = mDb.openIndex("test");

        // First fill the node with non-zeros and delete the entries. If an illegal region of
        // the node is read, it will cause an ArrayIndexOutOfBoundsException to be thrown.
        for (int i=0; i<4; i++) {
            ix.store(null, key(i), filledValue(1020));
        }
        for (int i=0; i<4; i++) {
            ix.store(null, key(i), null);
        }

        ix.store(null, filledKey(943, 0), filledValue(944));
        ix.store(null, filledKey(332, 1), filledValue(12));
        ix.store(null, filledKey(597, 2), filledValue(592));

        ix.store(null, filledKey(332, 1), filledValue(2672));

        assertTrue(ix.verify(new VerificationObserver()));
    }

    @Test
    public void testInsertLargeValueEarlyFragment() throws Exception {
        // Ensure that loop which moves entries into a newly split node moves as much as
        // possible and not too much.

        Index ix = mDb.openIndex("test");

        ix.store(null, key(1000, 0), new byte[1060]);
        ix.store(null, key(2), new byte[20]);

        ix.store(null, key(2026, 1), new byte[1060]);

        assertTrue(ix.verify(new VerificationObserver()));
    }

    private static byte[] key(int i) {
        return key(4, i);
    }

    private static byte[] key(int size, int i) {
        byte[] key = new byte[size];
        Utils.encodeIntBE(key, 0, i);
        return key;
    }

    private static byte[] filledKey(int size, int i) {
        byte[] key = filledValue(size);
        Utils.encodeIntBE(key, 0, i);
        return key;
    }

    private static byte[] filledValue(int size) {
        byte[] value = new byte[size];
        Arrays.fill(value, (byte) -1);
        return value;
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

    private static void shuffle(Random rnd, int[] keys) {
        for (int i = 0; i < keys.length; i++) {
            int exchangeIndex = rnd.nextInt(keys.length - i) + i;
            int temp = keys[i];
            keys[i] = keys[exchangeIndex];
            keys[exchangeIndex] = temp;
        }
    }

    @Test
    public void testFragmentRollback() throws Exception {
        mDb.suspendCheckpoints();
        Index ix = mDb.openIndex("test");

        Random rnd = new Random(1234L);

        final int keyCount = 10000;
        // List of numbers [0, keyCount) in a random order
        int keys[] = new int[keyCount];

        // Initialize the list of keys as sequential
        for (int i = 0; i < keyCount; i++) {
            keys[i] = i;
        }
        // Now make the list of keys random
        shuffle(rnd, keys);

        // For each of those keys, insert a random value of a random length
        for (int i = 0; i < keyCount; i++) {
            // Pick a length for the value, in the range [1B, 12.5KB]. The distribution is
            // biased so that half of the values are less than 117.
            int valueLength = (int)Math.pow(1.1, rnd.nextDouble() * 100);
            byte[] value = new byte[valueLength];
            rnd.nextBytes(value);
            ix.store(null, key(keys[i]), value);
        }

        // Update all of the keys (in a different order) with new values of different sizes.
        // Do not commit the transactions.
        shuffle(rnd, keys);
        Transaction[] txns = new Transaction[keyCount];
        for (int i = 0; i < keyCount; i++) {
            int valueLength = (int)Math.pow(1.1, rnd.nextInt(100));
            byte[] value = new byte[valueLength];
            rnd.nextBytes(value);
            Transaction txn = ix.newTransaction(DurabilityMode.NO_FLUSH);
            ix.store(txn, key(keys[i]), value);
            txns[i] = txn;
        }

        shuffle(rnd, keys);
        // Rollback all of the transactions. This forces non-fragmented values to get restored
        // to fragmented values, and vice versa.
        for (int i = 0; i < keyCount; i++) {
            txns[i].reset();
        }

        assertTrue(ix.verify(new VerificationObserver()));
    }
}
