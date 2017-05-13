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
@Ignore
public class StreamTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(StreamTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        DatabaseConfig config = new DatabaseConfig().directPageAccess(false).pageSize(512);
        mDb = newTempDatabase(getClass(), config);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
        mDb = null;
    }

    protected Database mDb;

    static Stream newStream(Index ix) throws Exception {
        //return ix.newStream();
        return null;
    }

    @Test
    public void readMissing() throws Exception {
        Index ix = mDb.openIndex("test");

        Stream s = newStream(ix);

        assertEquals(LockResult.UNOWNED, s.open(null, "key".getBytes()));
        assertEquals(-1, s.length());

        try {
            s.read(0, null, 0, 0);
            fail();
        } catch (NullPointerException e) {
        }

        try {
            s.read(0, new byte[0], 0, 10);
            fail();
        } catch (IndexOutOfBoundsException e) {
        }

        assertEquals(-1, s.read(0, new byte[0], 0, 0));
        assertEquals(-1, s.read(0, new byte[10], 0, 5));
        assertEquals(-1, s.read(0, new byte[10], 1, 5));
        assertEquals(-1, s.read(10, new byte[10], 1, 5));

        s.close();

        try {
            s.length();
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            s.read(0, new byte[0], 0, 0);
            fail();
        } catch (IllegalStateException e) {
        }

        Transaction txn = mDb.newTransaction();
        assertEquals(LockResult.ACQUIRED, s.open(txn, "key".getBytes()));
        assertEquals(-1, s.length());
        txn.reset();
    }

    @Test
    public void readEmpty() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(null, "key".getBytes(), new byte[0]);
        Stream s = newStream(ix);
        s.open(null, "key".getBytes());
        assertEquals(0, s.length());

        byte[] buf = new byte[5];
        assertEquals(0, s.read(0, buf, 0, 5));
        assertEquals(0, s.read(1, buf, 2, 3));
        assertEquals(0, s.read(5, buf, 0, 5));
        assertEquals(0, s.read(500, buf, 0, 5));

        s.close();
    }

    @Test
    public void readSmall() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(null, "key".getBytes(), "value".getBytes());
        Stream s = newStream(ix);
        s.open(null, "key".getBytes());
        assertEquals(5, s.length());

        byte[] buf = new byte[5];
        assertEquals(5, s.read(0, buf, 0, 5));
        fastAssertArrayEquals("value".getBytes(), buf);

        assertEquals(3, s.read(1, buf, 2, 3));
        assertEquals('a', (char) buf[2]);
        assertEquals('l', (char) buf[3]);
        assertEquals('u', (char) buf[4]);

        assertEquals(0, s.read(5, buf, 0, 5));
        assertEquals(0, s.read(500, buf, 0, 5));

        s.close();
    }

    @Test
    public void readFragmented1() throws Exception {
        readFragmented(false, false);
    }

    @Test
    public void readFragmented2() throws Exception {
        readFragmented(false, true);
    }

    @Ignore // FIXME
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
                Stream s = newStream(ix);
                s.open(null, key);
                if (setLength) {
                    s.setLength(length);
                }
                for (int j=0; j<length; j+=50) {
                    s.write(j, value, j, 50);
                }
                s.close();
            } else {
                if (setLength) {
                    Stream s = newStream(ix);
                    s.open(null, key);
                    s.setLength(length);
                    s.close();
                }
                ix.store(null, key, value);
            }

            Stream s = newStream(ix);
            s.open(null, key);

            assertEquals(length, s.length());

            byte[] buf = new byte[length + 10];

            // Attempt to read nothing past the end.
            assertEquals(0, s.read(length, buf, 10, 0));

            // Attempt to read past the end.
            assertEquals(0, s.read(length, buf, 10, 10));

            // Read many different slices, extending beyond as well.

            for (int start=0; start<length; start += 3) {
                for (int end=start; end<length+2; end += 7) {
                    int amt = s.read(start, buf, 1, end - start);
                    int expected = Math.min(end - start, length - start);
                    assertEquals(expected, amt);
                    int cmp = Utils.compareUnsigned(value, start, amt, buf, 1, amt);
                    assertEquals(0, cmp);
                }
            }

            s.close();

            //ix.delete(null, key);
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

            Stream s = newStream(ix);
            s.open(null, key);

            byte[] buf = new byte[length + 10];

            // Attempt to read nothing past the end.
            assertEquals(0, s.read(length, buf, 10, 0));

            // Attempt to read past the end.
            assertEquals(0, s.read(length, buf, 10, 10));

            // Read many different slices, extending beyond as well.

            for (int start=0; start<length; start += 311) {
                for (int end=start; end<length+2; end += (512 + 256)) {
                    int amt = s.read(start, buf, 1, end - start);
                    int expected = Math.min(end - start, length - start);
                    assertEquals(expected, amt);
                    int cmp = Utils.compareUnsigned(value, start, amt, buf, 1, amt);
                    assertEquals(0, cmp);
                }
            }

            s.close();

            //ix.delete(null, key);
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
            Stream s = newStream(ix);

            byte[] key = "key".getBytes();
            s.open(null, key);

            if (useWrite) {
                s.write(i, key, 0, 0);
            } else {
                s.setLength(i);
            }

            assertEquals(i, s.length());

            byte[] value = ix.load(null, key);
            assertNotNull(value);
            assertEquals(i, value.length);
            for (int j=0; j<i; j++) {
                assertEquals(0, value[j]);
            }

            Arrays.fill(buf, 0, buf.length, (byte) 55);

            if (i == 0) {
                int amt = s.read(0, buf, 1, 100);
                assertEquals(0, amt);
            } else {
                if (i == 100) {
                    int amt = s.read(0, buf, 1, 100);
                    assertEquals(100, amt);
                } else {
                    int amt = s.read(100, buf, 1, 100);
                    assertEquals(100, amt);
                }
                assertEquals(55, buf[0]);
                for (int j=1; j<100; j++) {
                    assertEquals(0, buf[j]);
                }
                assertEquals(55, buf[buf.length - 1]);
            }

            ix.delete(null, key);
            assertEquals(-1, s.length());
            assertNull(ix.load(null, key));
        }
    }

    @Test
    public void truncateNonFragmented() throws Exception {
        // Use large page to test 3-byte value header encoding.
        Database db = newTempDatabase
            (getClass(), new DatabaseConfig().directPageAccess(false).pageSize(32768));

        truncate(db, 10, 5);     // 1-byte header to 1
        truncate(db, 200, 50);   // 2-byte header to 1
        truncate(db, 10000, 50); // 3-byte header to 1

        truncate(db, 200, 150);   // 2-byte header to 2
        truncate(db, 10000, 200); // 3-byte header to 2

        truncate(db, 20000, 10000); // 3-byte header to 3
    }

    private static void truncate(Database db, int from, int to) throws Exception {
        Index ix = db.openIndex("test");
        Random rnd = new Random(from * 31 + to);

        byte[] key = new byte[30];
        rnd.nextBytes(key);

        byte[] value = new byte[from];
        rnd.nextBytes(value);

        ix.store(null, key, value);

        Stream s = newStream(ix);
        s.open(null, key);
        s.setLength(to);
        s.close();

        byte[] truncated = ix.load(null, key);
        assertEquals(to, truncated.length);

        for (int i=0; i<to; i++) {
            assertEquals(value[i], truncated[i]);
        }

        ix.verify(null);
    }

    @Test
    public void writeNonFragmented() throws Exception {
        // Use large page to test 3-byte value header encoding.
        Database db = newTempDatabase
            (getClass(), new DatabaseConfig().directPageAccess(false).pageSize(32768));

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

            Stream s = newStream(ix);
            s.open(null, key);
            s.write(left, sub, 0, sub.length);
            s.close();

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

        ix.verify(null);
    }

    @Test
    public void inputRead() throws Exception {
        Index ix = mDb.openIndex("test");

        final long seed = 1984574;
        Random rnd = new Random(seed);

        byte[] key = "input".getBytes();
        byte[] value = randomStr(rnd, 100000);

        Stream s = newStream(ix);
        s.open(null, key);
        InputStream in = s.newInputStream(0, 101);

        try {
            in.read();
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

        s = newStream(ix);
        s.open(null, key);
        in = s.newInputStream(0, 101);

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

        s.open(null, key);
        in = s.newInputStream(0, 10);

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

        Stream s = newStream(ix);
        s.open(null, key);
        OutputStream out = s.newOutputStream(0, 101);

        out.close();

        try {
            out.write(1);
            fail();
        } catch (IllegalStateException e) {
        }

        s.open(null, key);
        s.setLength(value.length);

        out = s.newOutputStream(0, 20);

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

        s.open(null, key);

        byte[] actual = new byte[value.length];
        int amt = s.read(0, actual, 0, actual.length);

        assertEquals(amt, actual.length);

        fastAssertArrayEquals(value, actual);

        // Again, one byte at a time.

        key = "output2".getBytes();
        value = randomStr(rnd, 100000);

        s.open(null, key);
        s.setLength(value.length);

        out = s.newOutputStream(0, 20);

        for (int i=0; i<value.length; i++) {
            out.write(value[i]);
        }

        out.flush();

        actual = new byte[value.length];
        amt = s.read(0, actual, 0, actual.length);

        assertEquals(amt, actual.length);

        fastAssertArrayEquals(value, actual);
    }
}
