/*
 *  Copyright 2014-2015 Cojen.org
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
public class LargeKeyTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LargeKeyTest.class.getName());
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
    public void largeBlanks() throws Exception {
        Database db = Database.open(decorate(new DatabaseConfig().pageSize(4096)));
        Index ix = db.openIndex("test");

        byte[] value = new byte[0];

        byte[][] keys = {
            new byte[2000], new byte[2001], new byte[2002], new byte[2003], new byte[2004]
        };

        for (byte[] key : keys) {
            ix.store(null, key, value);
            assertTrue(ix.verify(null));
        }

        for (byte[] key : keys) {
            byte[] v = ix.load(null, key);
            assertArrayEquals(value, v);
        }
    }

    @Test
    public void storeMaxSize() throws Exception {
        storeMaxSize(512);
        storeMaxSize(1024);
        storeMaxSize(2048);
        storeMaxSize(4096);
        storeMaxSize(8192);
        storeMaxSize(16384);
        storeMaxSize(32768);
        storeMaxSize(65536);
    }

    private void storeMaxSize(final int pageSize) throws Exception {
        Database db = Database.open(decorate(new DatabaseConfig().pageSize(pageSize)));
        Index ix = db.openIndex("test");

        byte[] value = new byte[0];

        final int max = Math.min(16383, (pageSize / 2) - 22);

        byte[][] keys = new byte[1000][];
        Random rnd = new Random(87324);
        for (int i=0; i<keys.length; i++) {
            keys[i] = randomStr(rnd, max, max);
        }

        for (byte[] key : keys) {
            ix.store(null, key, value);
        }

        assertTrue("Verification failed for page size of: " + pageSize, ix.verify(null));

        for (byte[] key : keys) {
            byte[] v = ix.load(null, key);
            fastAssertArrayEquals(value, v);
        }
    }

    @Test
    public void storeMaxSizeFull() throws Exception {
        storeMaxSizeFull(512);
        storeMaxSizeFull(1024);
        storeMaxSizeFull(2048);
        storeMaxSizeFull(4096);
        storeMaxSizeFull(8192);
        storeMaxSizeFull(16384);
        storeMaxSizeFull(32768);
        storeMaxSizeFull(65536);
    }

    private void storeMaxSizeFull(final int pageSize) throws Exception {
        // Keys are constructed such that little or no suffix compression is applied. This
        // reduces the number of keys stored by internal nodes to be at the minimum of 2.

        Database db = Database.open(decorate(new DatabaseConfig().pageSize(pageSize)));
        Index ix = db.openIndex("test");

        byte[] value = new byte[0];

        final int max = Math.min(16383, (pageSize / 2) - 22);

        byte[][] keys = new byte[800][];
        Random rnd = new Random(87324);
        for (int i=0; i<keys.length; i++) {
            byte[] key = new byte[max];
            Arrays.fill(key, (byte) '.');
            byte[] tail = randomStr(rnd, 4, 4);
            System.arraycopy(tail, 0, key, key.length - tail.length, tail.length);
            keys[i] = key;
        }

        for (byte[] key : keys) {
            ix.store(null, key, value);
        }

        assertTrue("Verification failed for page size of: " + pageSize, ix.verify(null));

        for (byte[] key : keys) {
            byte[] v = ix.load(null, key);
            fastAssertArrayEquals(value, v);
        }
    }

    @Test
    public void veryLargeKeys() throws Exception {
        Database db = newTempDatabase(decorate(new DatabaseConfig().checkpointRate(-1, null)));
        Index ix = db.openIndex("test");

        final int seed = 23423;

        int[] prefixes = {0, 10, 40, 100, 400, 1000, 2100, 4096, 10000};

        for (int t=0; t<2; t++) {
            for (int q=0; q<3; q++) {
                for (int p : prefixes) {
                    byte[] prefix = new byte[p];
                    new Random(seed + p).nextBytes(prefix);
                    View view = ix.viewPrefix(prefix, prefix.length);

                    Random rnd = new Random(seed);
                    byte[] value = new byte[4];

                    for (int i=0; i<1000; i++) {
                        int keyLen = rnd.nextInt(10000) + 1;
                        byte[] key = new byte[keyLen];
                        rnd.nextBytes(key);
                        Utils.encodeIntLE(value, 0, hash(key));
                        if (t == 0) {
                            view.store(Transaction.BOGUS, key, value);
                        } else {
                            byte[] found = view.load(Transaction.BOGUS, key);
                            fastAssertArrayEquals(value, found);
                        }
                    }
                }

                if (t == 0) {
                    assertTrue(ix.verify(null));
                }
            }
        }
    }

    static int hash(byte[] b) {
        int hash = 0;
        for (int i=0; i<b.length; i++) {
            hash = hash * 31 + b[i];
        }
        return hash;
    }

    @Test
    public void updateAgainstLargeKeys() throws Exception {
        Database db = newTempDatabase(decorate(new DatabaseConfig()));
        Index ix = db.openIndex("test");

        final int seed = 1234567;

        byte[] value = new byte[0];

        Random rnd = new Random(seed);
        for (int i=0; i<1000; i++) {
            byte[] key = randomStr(rnd, 1000, 4000);
            ix.store(Transaction.BOGUS, key, value);
        }

        // Now update with larger values. This forces leaf nodes to split or compact.

        value = new byte[1000];

        rnd = new Random(seed);
        for (int i=0; i<1000; i++) {
            byte[] key = randomStr(rnd, 1000, 4000);
            int amt = Math.min(key.length, value.length);
            System.arraycopy(key, 0, value, 0, amt);
            ix.store(Transaction.BOGUS, key, value);
        }

        ix.verify(null);

        rnd = new Random(seed);
        for (int i=0; i<1000; i++) {
            byte[] key = randomStr(rnd, 1000, 4000);
            int amt = Math.min(key.length, value.length);
            System.arraycopy(key, 0, value, 0, amt);
            byte[] found = ix.load(Transaction.BOGUS, key);
            fastAssertArrayEquals(value, found);
        }
    }
}
