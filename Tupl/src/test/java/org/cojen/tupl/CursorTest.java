/*
 *  Copyright 2012 Brian S O'Neill
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

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CursorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CursorTest.class.getName());
    }

    @Before
    public void createTempDb() throws Exception {
        mDb = newTempDatabase();
    }

    @After
    public void teardown() {
        deleteTempDatabases();
        mDb = null;
    }

    private Database mDb;

    @Test
    public void stubCursor() throws Exception {
        stubCursor(false);
    }

    @Test
    public void stubEviction() throws Exception {
        stubCursor(true);
    }

    private void stubCursor(boolean eviction) throws Exception {
        Index ix = mDb.openIndex("test");

        for (int i=0; i<1000; i++) {
            ix.store(Transaction.BOGUS, key(i), ("value-" + i).getBytes());
        }

        Cursor c = ix.newCursor(Transaction.BOGUS);
        c.find(key(500));
        assertNotNull(c.key());

        for (int i=0; i<1000; i++) {
            if (i != 500) {
                ix.delete(Transaction.BOGUS, key(i));
            }
        }

        // Cursor is still valid, and it references a stub parent.
        c.load();
        assertArrayEquals(key(500), c.key());
        assertArrayEquals("value-500".getBytes(), c.value());

        Cursor c2 = ix.newCursor(Transaction.BOGUS);
        c2.first();
        assertArrayEquals(key(500), c2.key());
        c2.next();
        assertNull(c2.key());
        c2.last();
        assertArrayEquals(key(500), c2.key());

        if (eviction) {
            // Force eviction of stub. Cannot verify directly, however.
            Index ix2 = mDb.openIndex("test2");
            c.reset();
            c2.reset();

            for (int i=0; i<1000000; i++) {
                ix2.store(Transaction.BOGUS, key(i), ("value-" + i).getBytes());
            }

            c.find(key(500));
            assertArrayEquals(key(500), c.key());

            c2.last();
            assertArrayEquals(key(500), c2.key());
        }

        // Add back missing values, cursors should see them.
        for (int i=0; i<1000; i++) {
            if (i != 500) {
                ix.store(Transaction.BOGUS, key(i), ("value-" + i).getBytes());
            }
        }

        for (int i=501; i<1000; i++) {
            c.next();
            assertArrayEquals(key(i), c.key());
            assertArrayEquals(("value-" + i).getBytes(), c.value());
        }
        c.next();
        assertNull(c.key());

        for (int i=499; i>=0; i--) {
            c2.previous();
            assertArrayEquals(key(i), c2.key());
            assertArrayEquals(("value-" + i).getBytes(), c2.value());
        }
        c2.previous();
        assertNull(c2.key());
    }

    private byte[] key(int i) {
        byte[] key = new byte[4];
        Utils.writeIntBE(key, 0, i);
        return key;
    }
}
