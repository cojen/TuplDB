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
        mDb = newTempDatabase(getClass(), OpenMode.DIRECT);
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
