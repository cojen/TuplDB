/*
 *  Copyright 2016 Cojen.org
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
public class CursorDirectTest extends CursorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CursorDirectTest.class.getName());
    }

    @Before
    @Override
    public void createTempDb() throws Exception {
        mDb = newTempDatabase(OpenMode.DIRECT);
    }

    protected _TreeCursor directTreeCursor(Cursor c) {
        return (_TreeCursor) c;
    }

    @Override
    public void findNearby() throws Exception {
        View ix = openIndex("test");

        final int count = 3000;
        final int seed = 3892476;
        Random rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            byte[] key = randomStr(rnd, 100, 500);
            ix.store(Transaction.BOGUS, key, key);
        }

        // Find every key using each key as a starting point.

        Cursor c1 = ix.newCursor(Transaction.BOGUS);
        for (c1.first(); c1.key() != null; c1.next()) {
            _TreeCursor c2 = directTreeCursor(ix.newCursor(Transaction.BOGUS));
            for (c2.first(); c2.key() != null; c2.next()) {
                _TreeCursor ref = directTreeCursor(c1.copy());
                ref.findNearby(c2.key());
                assertTrue(ref.equalPositions(c2));
                ref.reset();
            }
            c2.reset();
        }
        c1.reset();
    }

    @Override
    protected void verifyExtremities(View ix) throws Exception {
        _TreeCursor extremity = directTreeCursor(ix.newCursor(Transaction.BOGUS));
        assertTrue(extremity.verifyExtremities(Node.LOW_EXTREMITY));
        assertTrue(extremity.verifyExtremities(Node.HIGH_EXTREMITY));
    }

    @Override
    protected void verifyPositions(View ix, Cursor[] cursors) throws Exception {
        for (Cursor existing : cursors) {
            Cursor c = ix.newCursor(Transaction.BOGUS);
            byte[] key = existing.key();
            c.find(key);
            assertTrue(directTreeCursor(c).equalPositions(directTreeCursor(existing)));
            c.reset();
            existing.reset();
        }
    }
}
