/*
 *  Copyright 2013 Brian S O'Neill
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

import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class StreamTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(StreamTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        DatabaseConfig config = new DatabaseConfig().pageSize(512);
        mDb = newTempDatabase(config);
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases();
        mDb = null;
    }

    protected Database mDb;

    @Test
    public void readEmpty() throws Exception {
        Index ix = mDb.openIndex("test");

        Stream s = ix.newStream();

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

        assertEquals(0, s.read(0, new byte[0], 0, 0));
        assertEquals(0, s.read(0, new byte[10], 0, 5));
        assertEquals(0, s.read(0, new byte[10], 1, 5));
        assertEquals(0, s.read(10, new byte[10], 1, 5));

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
    public void readSmall() throws Exception {
        Index ix = mDb.openIndex("test");
        ix.store(null, "key".getBytes(), "value".getBytes());
        Stream s = ix.newStream();
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
    public void readFragmented() throws Exception {
        Index ix = mDb.openIndex("test");

        final long seed = 3984574;
        Random rnd = new Random(seed);

        for (int i=1; i<=100; i++) {
            byte[] key = ("key" + i).getBytes();
            byte[] value = randomStr(rnd, 50 * i);
            ix.store(null, key, value);

            Stream s = ix.newStream();
            s.open(null, key);
            byte[] actual = new byte[value.length];
            assertEquals(value.length, s.read(0, actual, 0, actual.length));
            fastAssertArrayEquals(value, actual);
            s.close();

            //ix.delete(null, key);
        }
    }
}
