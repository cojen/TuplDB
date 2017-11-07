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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.util.Arrays;
import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class ValueAccessorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ValueAccessorTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        DatabaseConfig config = new DatabaseConfig().directPageAccess(false).pageSize(512);
        config = decorate(config);
        mDb = newTempDatabase(getClass(), config);
    }

    protected DatabaseConfig decorate(DatabaseConfig config) {
        return config;
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
    }

    protected Database mDb;

    @Test
    public void readMissing() throws Exception {
        Index ix = mDb.openIndex("test");

        ValueAccessor accessor = ix.newAccessor(null, "key".getBytes());
        assertEquals(-1, accessor.valueLength());

        try {
            accessor.valueRead(0, null, 0, 0);
            fail();
        } catch (NullPointerException e) {
        }

        try {
            accessor.valueRead(0, new byte[0], 0, 10);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }

        assertEquals(-1, accessor.valueRead(0, new byte[0], 0, 0));
        assertEquals(-1, accessor.valueRead(0, new byte[10], 0, 5));
        assertEquals(-1, accessor.valueRead(0, new byte[10], 1, 5));
        assertEquals(-1, accessor.valueRead(10, new byte[10], 1, 5));

        accessor.close();

        try {
            accessor.valueLength();
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            accessor.valueRead(0, new byte[0], 0, 0);
            fail();
        } catch (IllegalStateException e) {
        }
    }

    @Test
    public void readEmpty() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(null, "key".getBytes(), new byte[0]);
        ValueAccessor accessor = ix.newAccessor(null, "key".getBytes());
        assertEquals(0, accessor.valueLength());

        byte[] buf = new byte[5];
        assertEquals(0, accessor.valueRead(0, buf, 0, 5));
        assertEquals(0, accessor.valueRead(1, buf, 2, 3));
        assertEquals(0, accessor.valueRead(5, buf, 0, 5));
        assertEquals(0, accessor.valueRead(500, buf, 0, 5));

        accessor.close();
    }

    @Test
    public void readSmall() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(null, "key".getBytes(), "value".getBytes());
        ValueAccessor accessor = ix.newAccessor(null, "key".getBytes());
        assertEquals(5, accessor.valueLength());

        byte[] buf = new byte[5];
        assertEquals(5, accessor.valueRead(0, buf, 0, 5));
        fastAssertArrayEquals("value".getBytes(), buf);

        assertEquals(3, accessor.valueRead(1, buf, 2, 3));
        assertEquals('a', (char) buf[2]);
        assertEquals('l', (char) buf[3]);
        assertEquals('u', (char) buf[4]);

        assertEquals(0, accessor.valueRead(5, buf, 0, 5));
        assertEquals(0, accessor.valueRead(500, buf, 0, 5));

        accessor.close();
    }

    @Test
    public void readFragmented1() throws Exception {
        readFragmented(false, false);
    }

    @Test
    public void readFragmented2() throws Exception {
        readFragmented(false, true);
    }

    @Test
    public void readFragmented3() throws Exception {
        readFragmented(true, false);
    }

    private void readFragmented(boolean useWrite, boolean setLength) throws Exception {
        Index ix = mDb.openIndex("test");

        final long seed = 3984574;
        Random rnd = new Random(seed);

        for (int i=1; i<=100; i++) {
            byte[] key = ("key" + i).getBytes();
            int length = 50 * i;
            byte[] value = randomStr(rnd, length);

            if (useWrite) {
                ValueAccessor accessor = ix.newAccessor(null, key);
                if (setLength) {
                    accessor.setValueLength(length);
                }
                for (int j=0; j<length; j+=50) {
                    accessor.valueWrite(j, value, j, 50);
                }
                accessor.close();
            } else {
                if (setLength) {
                    ValueAccessor accessor = ix.newAccessor(null, key);
                    accessor.setValueLength(length);
                    accessor.close();
                }
                ix.store(null, key, value);
            }

            ValueAccessor accessor = ix.newAccessor(null, key);

            assertEquals(length, accessor.valueLength());

            byte[] buf = new byte[length + 10];

            // Attempt to read nothing past the end.
            assertEquals(0, accessor.valueRead(length, buf, 10, 0));

            // Attempt to read past the end.
            assertEquals(0, accessor.valueRead(length, buf, 10, 10));

            // Read many different slices, extending beyond as well.

            for (int start=0; start<length; start += 3) {
                for (int end=start; end<length+2; end += 7) {
                    int amt = accessor.valueRead(start, buf, 1, end - start);
                    int expected = Math.min(end - start, length - start);
                    assertEquals(expected, amt);
                    int cmp = Utils.compareUnsigned(value, start, amt, buf, 1, amt);
                    assertEquals(0, cmp);
                }
            }

            accessor.close();

            ix.delete(null, key);
        }
    }

    @Test
    public void readLargeFragmented() throws Exception {
        Index ix = mDb.openIndex("test");

        final long seed = 3984574;
        Random rnd = new Random(seed);

        for (int i=1; i<=30; i++) {
            byte[] key = ("key" + i).getBytes();
            int length = 5000 * i;
            byte[] value = randomStr(rnd, length);
            ix.store(null, key, value);

            ValueAccessor accessor = ix.newAccessor(null, key);

            byte[] buf = new byte[length + 10];

            // Attempt to read nothing past the end.
            assertEquals(0, accessor.valueRead(length, buf, 10, 0));

            // Attempt to read past the end.
            assertEquals(0, accessor.valueRead(length, buf, 10, 10));

            // Read many different slices, extending beyond as well.

            for (int start=0; start<length; start += 311) {
                for (int end=start; end<length+2; end += (512 + 256)) {
                    int amt = accessor.valueRead(start, buf, 1, end - start);
                    int expected = Math.min(end - start, length - start);
                    assertEquals(expected, amt);
                    int cmp = Utils.compareUnsigned(value, start, amt, buf, 1, amt);
                    assertEquals(0, cmp);
                }
            }

            accessor.close();

            ix.delete(null, key);
        }
    }

    @Test
    public void extendBlank1() throws Exception {
        extendBlank(false);
    }

    @Test
    public void extendBlank2() throws Exception {
        extendBlank(true);
    }

    private void extendBlank(boolean useWrite) throws Exception {
        Index ix = mDb.openIndex("test");

        byte[] buf = new byte[102];

        for (int i=0; i<100000; i+=100) {
            byte[] key = "key".getBytes();
            ValueAccessor accessor = ix.newAccessor(null, key);

            if (useWrite) {
                accessor.valueWrite(i, key, 0, 0);
            } else {
                accessor.setValueLength(i);
            }

            assertEquals(i, accessor.valueLength());

            byte[] value = ix.load(null, key);
            assertNotNull(value);
            assertEquals(i, value.length);
            for (int j=0; j<i; j++) {
                assertEquals(0, value[j]);
            }

            Arrays.fill(buf, 0, buf.length, (byte) 55);

            if (i == 0) {
                int amt = accessor.valueRead(0, buf, 1, 100);
                assertEquals(0, amt);
            } else {
                if (i == 100) {
                    int amt = accessor.valueRead(0, buf, 1, 100);
                    assertEquals(100, amt);
                } else {
                    int amt = accessor.valueRead(100, buf, 1, 100);
                    assertEquals(100, amt);
                }
                assertEquals(55, buf[0]);
                for (int j=1; j<100; j++) {
                    assertEquals(0, buf[j]);
                }
                assertEquals(55, buf[buf.length - 1]);
            }

            ix.delete(null, key);
            assertEquals(-1, accessor.valueLength());
            assertNull(ix.load(null, key));
        }
    }

    @Test
    public void extendExisting() throws Exception {
        int[] from = {
            0, 100, 200, 1000, 2000, 10000, 100_000, 10_000_000, 100_000_000
        };

        long[] to = {
            0, 101, 201, 1001, 5000, 10001, 100_001, 10_000_001, 100_000_001, 10_000_000_000L
        };

        for (int fromLen : from) {
            for (long toLen : to) {
                if (toLen > fromLen) {
                    try {
                        extendExisting(fromLen, toLen, true);
                    } catch (Throwable e) {
                        e.printStackTrace(System.out);
                        throw e;
                    }
                }
            }
        }
    }

    @Test
    public void extendExistingInlcudingInline() throws Exception {
        // Test with various initial sizes which have some inline content encoded.

        extendExisting(513, 514, true);
        extendExisting(513, 1000, true);
        extendExisting(513, 10_000, true);
        extendExisting(513, 100_000, true);
        extendExisting(600, 100_000, true);
    }

    @Test
    public void extendExistingInlcudingInlineCheckpoint() throws Exception {
        // Test with various initial sizes which have some inline content encoded.

        extendExisting(513, 514, true, true);
        extendExisting(513, 1000, true, true);
        extendExisting(513, 10_000, true, true);
        extendExisting(513, 100_000, true, true);
        extendExisting(600, 100_000, true, true);
    }

    private void extendExisting(int fromLen, long toLen, boolean fullCheck) throws Exception {
        extendExisting(fromLen, toLen, fullCheck, false);
    }

    private void extendExisting(int fromLen, long toLen, boolean fullCheck, boolean checkpoint)
        throws Exception
    {
        Index ix = mDb.openIndex("test");

        final long seed = 8675309 + fromLen + toLen;
        Random rnd = new Random(seed);

        byte[] key = "hello".getBytes();

        byte[] initial = new byte[fromLen];

        for (int i=0; i<fromLen; i++) {
            initial[i] = (byte) rnd.nextInt();
        }

        ix.store(Transaction.BOGUS, key, initial);
        initial = null;

        if (checkpoint) {
            mDb.checkpoint();
        }

        ValueAccessor accessor = ix.newAccessor(Transaction.BOGUS, key);

        accessor.setValueLength(toLen);

        assertEquals(toLen, accessor.valueLength());

        if (toLen < 10_000_000) {
            byte[] resulting = ix.load(Transaction.BOGUS, key);
            assertEquals(toLen, resulting.length);

            rnd = new Random(seed);

            for (int i=0; i<fromLen; i++) {
                byte expect = (byte) rnd.nextInt();
                if (resulting[i] != expect) {
                    fail("fail: " + i + ", " + expect + ", " + resulting[i]);
                }
            }

            for (int i=fromLen; i<toLen; i++) {
                if (resulting[i] != 0) {
                    fail();
                }
            }
        } else {
            InputStream in = accessor.newValueInputStream(0, 10_000);

            rnd = new Random(seed);

            for (int i=0; i<fromLen; i++) {
                int a = in.read();
                if (a < 0 || ((byte) a) != (byte) rnd.nextInt()) {
                    fail(i + ", " + a);
                }
            }

            if (!fullCheck) {
                if (in.read() == -1) {
                    fail();
                }
            } else {
                byte[] buf = new byte[101];

                for (long i=fromLen; i<toLen; ) {
                    Arrays.fill(buf, (byte) 1);

                    int amt = in.read(buf);

                    if (amt <= 0) {
                        fail(i + ", " + amt);
                    }

                    for (int j=0; j<amt; j++) {
                        if (buf[j] != 0) {
                            fail(i + ", " + j + ", " + buf[j]);
                        }
                    }

                    i += amt;
                }

                if (in.read() != -1) {
                    fail();
                }
            }
        }

        accessor.close();

        assertTrue(ix.verify(null));

        ix.delete(Transaction.BOGUS, key);
    }

    @Test
    public void superExtendExisting() throws Exception {
        // Original code would erroneously cast a very large long to an int.
        extendExisting(1000, 185_000_000_000L, false);

        // Original code would overflow.
        extendExisting(1000, Long.MAX_VALUE, false);
    }

    @Test
    public void truncateNonFragmented() throws Exception {
        // Use large page to test 3-byte value header encoding.
        DatabaseConfig config = new DatabaseConfig().directPageAccess(false).pageSize(32768);
        Database db = newTempDatabase(getClass(), decorate(config));

        truncate(db, 10, 5);     // 1-byte header to 1
        truncate(db, 200, 50);   // 2-byte header to 1
        truncate(db, 10000, 50); // 3-byte header to 1

        truncate(db, 200, 150);   // 2-byte header to 2
        truncate(db, 10000, 200); // 3-byte header to 2

        truncate(db, 20000, 10000); // 3-byte header to 3
    }

    @Test
    public void truncateFragmentedDirect() throws Exception {
        // Test truncation of fragmented value which uses direct pointer encoding.

        // Use large page to test 3-byte value header encoding.
        DatabaseConfig config = new DatabaseConfig().directPageAccess(false).pageSize(32768);
        Database db = newTempDatabase(getClass(), decorate(config));

        truncate(db, 65536, 10);       // no inline content; two pointers to one
        truncate(db, 65537, 10);       // 1-byte inline content; two pointers to one

        truncate(db, 100_000, 99_999); // slight truncation
        truncate(db, 100_000, 1696);   // only inline content remains
        truncate(db, 100_000, 1695);   // inline content is reduced
        truncate(db, 100_000, 10);     // only inline content remains
        truncate(db, 100_000, 0);      // full truncation

        truncate(db, 108_000, 9000);   // convert to normal value, 3-byte header

        truncate(db, 50_000_000, 1_000_000);   // still fragmented, 2-byte header
        truncate(db, 50_000_000, 45_000_000);  // still fragmented, 3-byte header
    }

    private static void truncate(Database db, int from, int to) throws Exception {
        Index ix = db.openIndex("test");
        Random rnd = new Random(from * 31 + to);

        byte[] key = new byte[30];
        rnd.nextBytes(key);

        byte[] value = new byte[from];
        rnd.nextBytes(value);

        ix.store(null, key, value);

        ValueAccessor accessor = ix.newAccessor(null, key);
        accessor.setValueLength(to);
        accessor.close();

        byte[] truncated = ix.load(null, key);
        assertEquals(to, truncated.length);

        for (int i=0; i<to; i++) {
            assertEquals(value[i], truncated[i]);
        }

        assertTrue(ix.verify(null));
    }

    @Test
    public void truncateFragmentedDirectExtend() throws Exception {
        Index ix = mDb.openIndex("test");
        byte[] key = "hello".getBytes();

        Random rnd = new Random(Arrays.hashCode(key));

        byte[] value = new byte[50 + 512 + 512];
        rnd.nextBytes(value);
        // Should have inline content and two direct pointers.
        ix.store(Transaction.BOGUS, key, value);

        // Truncate: should have inline content and two direct pointers, but only one byte is
        // used in the last page.
        ValueAccessor accessor = ix.newAccessor(Transaction.BOGUS, key);
        accessor.setValueLength(50 + 512 + 1);

        byte[] loaded = ix.load(Transaction.BOGUS, key);
        assertEquals(50 + 512 + 1, loaded.length);
        for (int i=0; i<loaded.length; i++) {
            assertTrue(value[i] == loaded[i]);
        }

        // Extend the value enough to force the inline content to move into the fragment pages.
        int newLen = (250 / 6) * 512;
        accessor.setValueLength(newLen);

        byte[] loaded2 = ix.load(Transaction.BOGUS, key);
        assertEquals(newLen, loaded2.length);
        for (int i=0; i<loaded.length; i++) {
            assertTrue(loaded[i] == loaded2[i]);
        }
        for (int i=loaded.length; i<loaded2.length; i++) {
            assertEquals(0, loaded2[i]);
        }

        assertTrue(ix.verify(null));
    }

    @Test
    public void truncateFragmentedIndirect() throws Exception {
        // Test truncation of fragmented values which use indirect pointer encoding.

        truncateFragmentedIndirect(40_000, 30_000);
        truncateFragmentedIndirect(40_000, 29_696); // newLen fully fits in pages
        truncateFragmentedIndirect(40_448, 30_000); // oldLen fully fits
        truncateFragmentedIndirect(40_448, 29_696);

        truncateFragmentedIndirect(50_000, 40_000); // lose one indirect level
        truncateFragmentedIndirect(50_000, 513);    // lose one indirect level
        truncateFragmentedIndirect(50_000, 512);    // convert to direct format
        truncateFragmentedIndirect(50_000, 1);      // convert to direct format
        truncateFragmentedIndirect(50_000, 0);      // full truncation

        truncateFragmentedIndirect(50_000, 50_000); // no truncation
    }

    @Test
    public void truncateFragmentedIndirectSparse() throws Exception {
        // Test truncation of sparse fragmented values which use indirect pointer encoding.

        truncateFragmentedIndirect(100_000, 99_999, true);
        truncateFragmentedIndirect(100_000, 29_696, true); // newLen fully fits in pages
        truncateFragmentedIndirect(100_000, 1, true);      // convert to direct format
        truncateFragmentedIndirect(100_000, 0, true);      // full truncation
    }

    @Test
    public void truncateFragmentedIndirectSparseWithCheckpoint() throws Exception {
        // Test truncation of sparse fragmented values which use indirect pointer encoding.

        truncateFragmentedIndirect(100_000, 99_999, true, true);
        truncateFragmentedIndirect(100_000, 29_696, true, true); // newLen fully fits in pages
        truncateFragmentedIndirect(100_000, 1, true, true);      // convert to direct format
        truncateFragmentedIndirect(100_000, 0, true, true);      // full truncation
    }

    private void truncateFragmentedIndirect(int oldLen, int newLen) throws Exception {
        truncateFragmentedIndirect(oldLen, newLen, false, false);
    }

    private void truncateFragmentedIndirect(int oldLen, int newLen, boolean sparse)
        throws Exception
    {
        truncateFragmentedIndirect(oldLen, newLen, sparse, false);
    }

    private void truncateFragmentedIndirect(int oldLen, int newLen,
                                            boolean sparse, boolean checkpoint)
        throws Exception
    {
        final long seed = 248237410 + oldLen + newLen;
        Random rnd = new Random(seed);

        Index ix = mDb.openIndex("test");
        byte[] key = new byte[20];
        rnd.nextBytes(key);

        ValueAccessor accessor = ix.newAccessor(Transaction.BOGUS, key);

        rnd = new Random(seed);
        byte[] b = new byte[10000];

        for (int i = 0; i < oldLen; i += b.length) {
            rnd.nextBytes(b);

            if (sparse && rnd.nextBoolean()) {
                Arrays.fill(b, (byte) 0);
            } else {
                int amt;
                if (i + b.length > oldLen) {
                    amt = oldLen - i;
                } else {
                    amt = b.length;
                }
                accessor.valueWrite(i, b, 0, amt);
            }
        }

        if (checkpoint) {
            mDb.checkpoint();
        }

        accessor.setValueLength(newLen);

        assertEquals(newLen, accessor.valueLength());

        rnd = new Random(seed);
        byte[] b2 = new byte[b.length];

        int total = 0;

        for (int i = 0; i < oldLen; i += b.length) {
            rnd.nextBytes(b);

            if (sparse && rnd.nextBoolean()) {
                Arrays.fill(b, (byte) 0);
            }

            int amt = accessor.valueRead(i, b2, 0, b2.length);
            total += amt;

            if (amt < b2.length) {
                assertEquals(newLen, total);
                for (int j=0; j<amt; j++) {
                    assertEquals(b[j], b2[j]);
                }
                break;
            } else {
                fastAssertArrayEquals(b, b2);
            }
        }

        // Extending shouldn't reveal old data.
        accessor.setValueLength(newLen + 10);
        assertEquals(10, accessor.valueRead(newLen, b2, 0, b2.length));
        for (int j=0; j<10; j++) {
            assertEquals(0, b2[j]);
        }

        assertTrue(ix.verify(null));
    }

    @Test
    public void truncateFragmentedIndirectBlank() throws Exception {
        // Test truncation of blank fragmented value which uses indirect pointer encoding.

        Index ix = mDb.openIndex("test");
        ValueAccessor accessor = ix.newAccessor(Transaction.BOGUS, "hello".getBytes());

        accessor.setValueLength(100_000_000_000L);
        assertEquals(100_000_000_000L, accessor.valueLength());
        accessor.setValueLength(0);
        assertEquals(0, accessor.valueLength());
        assertEquals(0, ix.load(Transaction.BOGUS, "hello".getBytes()).length);

        accessor.setValueLength(100_000_000_000L);
        assertEquals(100_000_000_000L, accessor.valueLength());
        accessor.setValueLength(1);
        assertEquals(1, accessor.valueLength());
        assertEquals(1, ix.load(Transaction.BOGUS, "hello".getBytes()).length);

        accessor.setValueLength(100_000_000_000L);
        assertEquals(100_000_000_000L, accessor.valueLength());
        accessor.setValueLength(1000);
        assertEquals(1000, accessor.valueLength());
        assertEquals(1000, ix.load(Transaction.BOGUS, "hello".getBytes()).length);
    }

    @Test
    public void writeNonFragmented() throws Exception {
        // Use large page to test 3-byte value header encoding.
        DatabaseConfig config = new DatabaseConfig().directPageAccess(false).pageSize(32768);
        Database db = newTempDatabase(getClass(), decorate(config));

        writeNonFragmented(db, 50);
        writeNonFragmented(db, 200);
        writeNonFragmented(db, 10000);
    }

    private static void writeNonFragmented(Database db, int size) throws Exception {
        Index ix = db.openIndex("test");
        Random rnd = new Random(size);

        byte[] key = new byte[30];
        rnd.nextBytes(key);

        byte[] value = new byte[size];

        int[] offs = {
            0, 0,  // completely replaced
            0, -1, // replace all but one byte on the right
            1, -1, // replace all but one byte on the left and right
            1, 0,  // replace all but one byte on the left
            0, 10, // replace all and extend
            1, 10  // replace all but one and extend
        };

        for (int i=0; i<offs.length; i+=2) {
            rnd.nextBytes(value);
            ix.store(null, key, value);

            int left = offs[i];
            int right = offs[i + 1];

            byte[] sub = new byte[size - left + right];
            rnd.nextBytes(sub);

            ValueAccessor accessor = ix.newAccessor(null, key);
            accessor.valueWrite(left, sub, 0, sub.length);
            accessor.close();

            byte[] expect;
            if (sub.length <= value.length) {
                expect = value.clone();
                System.arraycopy(sub, 0, expect, left, sub.length);
            } else {
                expect = new byte[size + right];
                System.arraycopy(value, 0, expect, 0, size);
                System.arraycopy(sub, 0, expect, left, sub.length);
            }

            fastAssertArrayEquals(expect, ix.load(null, key));
        }

        assertTrue(ix.verify(null));
    }

    @Test
    public void replaceGhost() throws Exception {
        Index ix = mDb.openIndex("test");
        byte[] key = "hello".getBytes();
        byte[] value1 = "world".getBytes();
        byte[] value2 = "world!".getBytes();

        ix.store(null, key, value1);

        Transaction txn = mDb.newTransaction();
        // Creates a ghost, which is deleted after the transaction commits.
        ix.store(txn, key, null);
        assertNull(ix.load(txn, key));

        // Sneakily replace the ghost.
        ValueAccessor accessor = ix.newAccessor(Transaction.BOGUS, key);
        assertEquals(-1, accessor.valueRead(0, value2, 0, value2.length));
        accessor.valueWrite(0, value2, 0, value2.length);

        fastAssertArrayEquals(value2, ix.load(Transaction.BOGUS, key));

        // Commit won't finish the delete, because a value now exists.
        txn.commit();
        txn.reset();

        fastAssertArrayEquals(value2, ix.load(Transaction.BOGUS, key));

        assertTrue(ix.verify(null));
    }

    @Test
    public void fieldIncreaseToIndirect() throws Exception {
        fieldIncreaseToIndirect(false);
    }

    @Test
    public void fieldIncreaseToIndirectCheckpoint() throws Exception {
        fieldIncreaseToIndirect(true);
    }

    private void fieldIncreaseToIndirect(boolean checkpoint) throws Exception {
        // Start with a specially crafted length, which when increased to use a 4-byte field,
        // forces removal of inline content. Since this is a black-box test, one way to be
        // certain that the conversion happened is by running a debugger.

        Index ix = mDb.openIndex("test");
        byte[] key = "hello".getBytes();
        byte[] value = new byte[19460];
        ix.store(Transaction.BOGUS, key, value);

        if (checkpoint) {
            mDb.checkpoint();
        }

        byte[] value2 = new byte[70000];
        new Random(12345678).nextBytes(value2);

        ValueAccessor accessor = ix.newAccessor(Transaction.BOGUS, key);
        accessor.valueWrite(0, value2, 0, value2.length);
        accessor.close();

        fastAssertArrayEquals(value2, ix.load(null, key));

        assertTrue(ix.verify(null));
        ix.store(Transaction.BOGUS, key, null);
        assertTrue(ix.verify(null));
    }

    @Test
    public void convertToIndirectWithLargePages() throws Exception {
        // Use large page to test 3-byte value header encoding.
        DatabaseConfig config = new DatabaseConfig().directPageAccess(false).pageSize(32768);
        Database db = newTempDatabase(getClass(), decorate(config));

        Index ix = db.openIndex("test");
        byte[] key = "hello".getBytes();

        Random rnd = new Random(8675309);
        byte[] value = new byte[20_000_000];
        rnd.nextBytes(value);

        ix.store(Transaction.BOGUS, key, value);

        byte[] extend = new byte[80_000_000]; // too large for direct pointers
        rnd.nextBytes(extend);

        ValueAccessor accessor = ix.newAccessor(Transaction.BOGUS, key);
        accessor.valueWrite(value.length, extend, 0, extend.length);

        assertEquals(value.length + extend.length, accessor.valueLength());
        accessor.close();

        byte[] full = new byte[value.length + extend.length];
        System.arraycopy(value, 0, full, 0, value.length);
        System.arraycopy(extend, 0, full, value.length, extend.length);
        value = null;
        extend = null;

        fastAssertArrayEquals(full, ix.load(Transaction.BOGUS, key));
    }

    @Test
    public void writeInline() throws Exception {
        // Test writing over inline content only.

        Index ix = mDb.openIndex("test");
        byte[] key = "hello".getBytes();

        Random rnd = new Random(8675309);
        byte[] value = new byte[10 + 512 + 512]; // 10 bytes inline
        rnd.nextBytes(value);

        ix.store(Transaction.BOGUS, key, value);

        byte[] value2 = new byte[10];
        rnd.nextBytes(value2);

        ValueAccessor accessor = ix.newAccessor(Transaction.BOGUS, key);
        accessor.valueWrite(0, value2, 0, value2.length);
        accessor.close();

        System.arraycopy(value2, 0, value, 0, value2.length);

        fastAssertArrayEquals(value, ix.load(Transaction.BOGUS, key));
    }

    @Test
    public void fill() throws Exception {
        // Fills an index with random values, in random order. Intended to exercise tree node
        // splitting and merging.

        Index ix = mDb.openIndex("test");

        final long seed = 8675309;
        Random rnd = new Random(seed);

        final byte[] buf = new byte[1000];

        for (int i=0; i<100; i++) {
            byte[] key = new byte[10 + rnd.nextInt(90)];
            rnd.nextBytes(key);

            ValueAccessor accessor = ix.newAccessor(Transaction.BOGUS, key);
            final int length = rnd.nextInt(1_000_000);
            int remaining = length;

            try (OutputStream out = accessor.newValueOutputStream(0, 0)) {
                while (true) {
                    int amt = Math.min(buf.length, remaining);
                    rnd.nextBytes(buf);
                    out.write(buf, 0, amt);
                    remaining -= amt;
                    if (remaining <= 0) {
                        assertEquals(length, accessor.valueLength());
                        break;
                    }
                }
            }
        }

        // Verify data.

        rnd = new Random(seed);

        final byte[] buf2 = new byte[buf.length];

        for (int i=0; i<100; i++) {
            byte[] key = new byte[10 + rnd.nextInt(90)];
            rnd.nextBytes(key);

            ValueAccessor accessor = ix.newAccessor(Transaction.BOGUS, key);
            final int length = rnd.nextInt(1_000_000);

            assertEquals(length, accessor.valueLength());

            int remaining = length;

            try (InputStream in = accessor.newValueInputStream(0, 0)) {
                while (true) {
                    int expect = Math.min(buf.length, remaining);
                    rnd.nextBytes(buf);

                    int amt = in.read(buf2);
                    assertEquals(expect, amt);

                    if (amt == buf.length) {
                        fastAssertArrayEquals(buf, buf2);
                    } else {
                        for (int j=0; j<amt; j++) {
                            assertEquals(buf[j], buf2[j]);
                        }
                    }

                    remaining -= amt;

                    if (remaining <= 0) {
                        assertEquals(-1, in.read(buf2));
                        break;
                    }
                }
            }
        }
    }

    @Test
    public void inputRead() throws Exception {
        Index ix = mDb.openIndex("test");

        final long seed = 1984574;
        Random rnd = new Random(seed);

        byte[] key = "input".getBytes();
        byte[] value = randomStr(rnd, 100000);

        ValueAccessor accessor = ix.newAccessor(null, key);
        InputStream in = accessor.newValueInputStream(0, 101);

        try {
            in.read();
            fail();
        } catch (NoSuchValueException e) {
        }

        try {
            in.read(new byte[1]);
            fail();
        } catch (NoSuchValueException e) {
        }

        in.close();

        try {
            in.read();
            fail();
        } catch (IllegalStateException e) {
        }

        ix.store(null, key, value);

        accessor = ix.newAccessor(null, key);
        in = accessor.newValueInputStream(0, 101);

        for (int i=0; i<value.length; i++) {
            assertEquals(value[i], in.read());
        }

        assertEquals(-1, in.read());

        in.close();

        try {
            in.read();
            fail();
        } catch (IllegalStateException e) {
        }

        assertEquals(0, in.available());

        accessor = ix.newAccessor(null, key);
        in = accessor.newValueInputStream(0, 10);

        byte[] buf = new byte[0];
        assertEquals(0, in.read(buf));

        int i = 0;

        while (true) {
            buf = new byte[rnd.nextInt(21) + 1];
            int amt = in.read(buf);

            assertTrue(amt != 0);

            if (amt < 0) {
                assertEquals(i, value.length);
                break;
            }

            for (int j=0; j<amt; j++) {
                assertEquals(value[i++], buf[j]);
            }
        }
    }

    @Test
    public void outputWrite() throws Exception {
        Index ix = mDb.openIndex("test");

        final long seed = 2984574;
        Random rnd = new Random(seed);

        byte[] key = "output".getBytes();
        byte[] value = randomStr(rnd, 100000);

        ValueAccessor accessor = ix.newAccessor(null, key);
        OutputStream out = accessor.newValueOutputStream(0, 101);

        out.close();

        try {
            out.write(1);
            fail();
        } catch (IllegalStateException e) {
        }

        accessor = ix.newAccessor(null, key);
        accessor.setValueLength(value.length);

        out = accessor.newValueOutputStream(0, 20);

        for (int i=0; i<value.length; ) {
            byte[] buf = new byte[Math.min(value.length - i, rnd.nextInt(21) + 1)];

            for (int j=0; j<buf.length; j++) {
                buf[j] = value[i++];
            }

            out.write(buf);
        }

        out.close();

        try {
            out.write(1);
            fail();
        } catch (IllegalStateException e) {
        }

        accessor = ix.newAccessor(null, key);

        byte[] actual = new byte[value.length];
        int amt = accessor.valueRead(0, actual, 0, actual.length);

        assertEquals(amt, actual.length);

        fastAssertArrayEquals(value, actual);

        // Again, one byte at a time.

        key = "output2".getBytes();
        value = randomStr(rnd, 100000);

        accessor = ix.newAccessor(null, key);
        accessor.setValueLength(value.length);

        out = accessor.newValueOutputStream(0, 20);

        for (int i=0; i<value.length; i++) {
            out.write(value[i]);
        }

        out.flush();

        actual = new byte[value.length];
        amt = accessor.valueRead(0, actual, 0, actual.length);

        assertEquals(amt, actual.length);

        fastAssertArrayEquals(value, actual);
    }
}
