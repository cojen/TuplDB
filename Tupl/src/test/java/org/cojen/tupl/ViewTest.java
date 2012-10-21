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
public class ViewTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ViewTest.class.getName());
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
    public void ge_15() throws Exception {
        ge(15);
    }

    @Test
    public void ge_20() throws Exception {
        ge(20);
    }

    @Test
    public void ge_25() throws Exception {
        ge(25);
    }

    @Test
    public void ge_30() throws Exception {
        ge(30);
    }

    private void ge(int start) throws Exception {
        Index ix = fill();

        View view = ix.viewGe(key(start));
        try {
            view.store(null, key(start - 1), key(start - 1));
            fail();
        } catch (IllegalArgumentException e) {
        }

        view.store(null, key(start), key(start));
        if (start % 10 != 0) {
            assertTrue(view.delete(null, key(start)));
        }

        int i = start <= 20 ? 20 : 30;
        Cursor c = view.newCursor(null);
        for (c.first(); c.key() != null; c.next()) {
            fastAssertArrayEquals(key(i), c.key());
            i += 10;
        }
        assertEquals(100, i);

        view = view.viewReverse();

        i = 90;
        c = view.newCursor(null);
        for (c.first(); c.key() != null; c.next()) {
            fastAssertArrayEquals(key(i), c.key());
            i -= 10;
        }
        assertEquals(start <= 20 ? 10 : 20, i);

        view = view.viewReverse();

        c = view.newCursor(null);
        c.first();
        c.skip(start <= 20 ? 3 : 2);
        fastAssertArrayEquals(key(50), c.key());
        c.skip(-2);
        fastAssertArrayEquals(key(30), c.key());

        c.nextLt(key(80));
        fastAssertArrayEquals(key(40), c.key());
        c.nextLe(key(50));
        fastAssertArrayEquals(key(50), c.key());

        c.previousGt(key(15));
        fastAssertArrayEquals(key(40), c.key());
        c.previousGt(key(10));
        fastAssertArrayEquals(key(30), c.key());
        c.previousGe(key(20));
        if (start <= 20) {
            fastAssertArrayEquals(key(20), c.key());
        } else {
            assertNull(c.key());
        }

        c.find(key(10));
        assertNull(c.key());
        c.find(key(35));
        assertNotNull(c.key());
        assertNull(c.value());
        c.find(key(40));
        fastAssertArrayEquals(key(40), c.key());

        c.findGe(key(10));
        fastAssertArrayEquals(key(start <= 20 ? 20 : 30), c.key());
        c.findGe(key(15));
        fastAssertArrayEquals(key(start <= 20 ? 20 : 30), c.key());
        c.findGe(key(20));
        fastAssertArrayEquals(key(start <= 20 ? 20 : 30), c.key());

        c.findGt(key(10));
        fastAssertArrayEquals(key(start <= 20 ? 20 : 30), c.key());
        c.findGt(key(15));
        fastAssertArrayEquals(key(start <= 20 ? 20 : 30), c.key());
        c.findGt(key(20));
        fastAssertArrayEquals(key(30), c.key());

        c.findLe(key(95));
        fastAssertArrayEquals(key(90), c.key());
        c.findLe(key(90));
        fastAssertArrayEquals(key(90), c.key());

        c.findLt(key(95));
        fastAssertArrayEquals(key(90), c.key());
        c.findLt(key(90));
        fastAssertArrayEquals(key(80), c.key());
    }

    @Test
    public void gt_15() throws Exception {
        gt(15);
    }

    @Test
    public void gt_20() throws Exception {
        gt(20);
    }

    @Test
    public void gt_25() throws Exception {
        gt(25);
    }

    @Test
    public void gt_30() throws Exception {
        gt(30);
    }

    private void gt(int start) throws Exception {
        Index ix = fill();

        View view = ix.viewGt(key(start));
        try {
            view.store(null, key(start - 1), key(start - 1));
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            view.store(null, key(start), key(start));
            fail();
        } catch (IllegalArgumentException e) {
        }

        int i = start < 20 ? 20 : (start < 30 ? 30 : 40);
        Cursor c = view.newCursor(null);
        for (c.first(); c.key() != null; c.next()) {
            fastAssertArrayEquals(key(i), c.key());
            i += 10;
        }
        assertEquals(100, i);

        view = view.viewReverse();

        i = 90;
        c = view.newCursor(null);
        for (c.first(); c.key() != null; c.next()) {
            fastAssertArrayEquals(key(i), c.key());
            i -= 10;
        }
        assertEquals(start < 20 ? 10 : (start < 30 ? 20 : 30), i);

        view = view.viewReverse();

        c = view.newCursor(null);
        c.first();
        c.skip(start < 20 ? 4 : (start < 30 ? 3 : 2));
        fastAssertArrayEquals(key(60), c.key());
        c.skip(-2);
        fastAssertArrayEquals(key(40), c.key());

        c.nextLt(key(80));
        fastAssertArrayEquals(key(50), c.key());
        c.nextLe(key(60));
        fastAssertArrayEquals(key(60), c.key());

        c.previousGt(key(15));
        fastAssertArrayEquals(key(50), c.key());
        c.previousGt(key(10));
        fastAssertArrayEquals(key(40), c.key());
        c.previousGe(key(20));
        if (start < 30) {
            fastAssertArrayEquals(key(30), c.key());
        } else {
            assertNull(c.key());
        }

        c.find(key(10));
        assertNull(c.key());
        c.find(key(35));
        assertNotNull(c.key());
        assertNull(c.value());
        c.find(key(40));
        fastAssertArrayEquals(key(40), c.key());

        c.findGe(key(10));
        fastAssertArrayEquals(key(start < 20 ? 20 : (start < 30) ? 30 : 40), c.key());
        c.findGe(key(15));
        fastAssertArrayEquals(key(start < 20 ? 20 : (start < 30) ? 30 : 40), c.key());
        c.findGe(key(20));
        fastAssertArrayEquals(key(start < 20 ? 20 : (start < 30) ? 30 : 40), c.key());

        c.findGt(key(10));
        fastAssertArrayEquals(key(start < 20 ? 20 : (start < 30) ? 30 : 40), c.key());
        c.findGt(key(15));
        fastAssertArrayEquals(key(start < 20 ? 20 : (start < 30) ? 30 : 40), c.key());
        c.findGt(key(20));
        fastAssertArrayEquals(key(start < 30 ? 30 : 40), c.key());

        c.findLe(key(95));
        fastAssertArrayEquals(key(90), c.key());
        c.findLe(key(90));
        fastAssertArrayEquals(key(90), c.key());

        c.findLt(key(95));
        fastAssertArrayEquals(key(90), c.key());
        c.findLt(key(90));
        fastAssertArrayEquals(key(80), c.key());
    }

    @Test
    public void le_95() throws Exception {
        le(95);
    }

    @Test
    public void le_90() throws Exception {
        le(90);
    }

    @Test
    public void le_85() throws Exception {
        le(85);
    }

    @Test
    public void le_80() throws Exception {
        le(80);
    }

    private void le(int end) throws Exception {
        Index ix = fill();

        View view = ix.viewLe(key(end));
        try {
            view.store(null, key(end + 1), key(end + 1));
            fail();
        } catch (IllegalArgumentException e) {
        }

        view.store(null, key(end), key(end));
        if (end % 10 != 0) {
            assertTrue(view.delete(null, key(end)));
        }

        int i = end >= 90 ? 90 : 80;
        Cursor c = view.newCursor(null);
        for (c.last(); c.key() != null; c.previous()) {
            fastAssertArrayEquals(key(i), c.key());
            i -= 10;
        }
        assertEquals(10, i);

        view = view.viewReverse();

        i = 20;
        c = view.newCursor(null);
        for (c.last(); c.key() != null; c.previous()) {
            fastAssertArrayEquals(key(i), c.key());
            i += 10;
        }
        assertEquals(end >= 90 ? 100 : 90, i);

        view = view.viewReverse();

        c = view.newCursor(null);
        c.last();
        c.skip(end >= 90 ? -3 : -2);
        fastAssertArrayEquals(key(60), c.key());
        c.skip(2);
        fastAssertArrayEquals(key(80), c.key());

        c.previousGt(key(30));
        fastAssertArrayEquals(key(70), c.key());
        c.previousGe(key(60));
        fastAssertArrayEquals(key(60), c.key());

        c.nextLt(key(75));
        fastAssertArrayEquals(key(70), c.key());
        c.nextLt(key(90));
        fastAssertArrayEquals(key(80), c.key());
        c.nextLe(key(90));
        if (end >= 90) {
            fastAssertArrayEquals(key(90), c.key());
        } else {
            assertNull(c.key());
        }

        c.find(key(96));
        assertNull(c.key());
        c.find(key(35));
        assertNotNull(c.key());
        assertNull(c.value());
        c.find(key(40));
        fastAssertArrayEquals(key(40), c.key());

        c.findLe(key(96));
        fastAssertArrayEquals(key(end >= 90 ? 90 : 80), c.key());
        c.findLe(key(95));
        fastAssertArrayEquals(key(end >= 90 ? 90 : 80), c.key());
        c.findLe(key(90));
        fastAssertArrayEquals(key(end >= 90 ? 90 : 80), c.key());

        c.findLt(key(96));
        fastAssertArrayEquals(key(end >= 90 ? 90 : 80), c.key());
        c.findLt(key(95));
        fastAssertArrayEquals(key(end >= 90 ? 90 : 80), c.key());
        c.findLt(key(90));
        fastAssertArrayEquals(key(80), c.key());

        c.findGe(key(15));
        fastAssertArrayEquals(key(20), c.key());
        c.findGe(key(20));
        fastAssertArrayEquals(key(20), c.key());

        c.findGt(key(15));
        fastAssertArrayEquals(key(20), c.key());
        c.findGt(key(20));
        fastAssertArrayEquals(key(30), c.key());
    }

    @Test
    public void lt_95() throws Exception {
        lt(95);
    }

    @Test
    public void lt_90() throws Exception {
        lt(90);
    }

    @Test
    public void lt_85() throws Exception {
        lt(85);
    }

    @Test
    public void lt_80() throws Exception {
        lt(80);
    }

    private void lt(int end) throws Exception {
        Index ix = fill();

        View view = ix.viewLt(key(end));
        try {
            view.store(null, key(end + 1), key(end + 1));
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            view.store(null, key(end), key(end));
            fail();
        } catch (IllegalArgumentException e) {
        }

        int i = end > 90 ? 90 : (end > 80 ? 80 : 70);
        Cursor c = view.newCursor(null);
        for (c.last(); c.key() != null; c.previous()) {
            fastAssertArrayEquals(key(i), c.key());
            i -= 10;
        }
        assertEquals(10, i);

        view = view.viewReverse();

        i = 20;
        c = view.newCursor(null);
        for (c.last(); c.key() != null; c.previous()) {
            fastAssertArrayEquals(key(i), c.key());
            i += 10;
        }
        assertEquals(end > 90 ? 100 : (end > 80 ? 90 : 80), i);

        view = view.viewReverse();

        c = view.newCursor(null);
        c.last();
        c.skip(end > 90 ? -4 : (end > 80 ? -3 : -2));
        fastAssertArrayEquals(key(50), c.key());
        c.skip(2);
        fastAssertArrayEquals(key(70), c.key());

        c.previousGt(key(30));
        fastAssertArrayEquals(key(60), c.key());
        c.previousGe(key(50));
        fastAssertArrayEquals(key(50), c.key());

        c.nextLt(key(75));
        fastAssertArrayEquals(key(60), c.key());
        c.nextLt(key(80));
        fastAssertArrayEquals(key(70), c.key());
        c.nextLe(key(80));
        if (end > 80) {
            fastAssertArrayEquals(key(80), c.key());
        } else {
            assertNull(c.key());
        }

        c.find(key(96));
        assertNull(c.key());
        c.find(key(35));
        assertNotNull(c.key());
        assertNull(c.value());
        c.find(key(40));
        fastAssertArrayEquals(key(40), c.key());

        c.findLe(key(96));
        fastAssertArrayEquals(key(end > 90 ? 90 : (end > 80 ? 80 : 70)), c.key());
        c.findLe(key(95));
        fastAssertArrayEquals(key(end > 90 ? 90 : (end > 80 ? 80 : 70)), c.key());
        c.findLe(key(90));
        fastAssertArrayEquals(key(end > 90 ? 90 : (end > 80 ? 80 : 70)), c.key());

        c.findLt(key(96));
        fastAssertArrayEquals(key(end > 90 ? 90 : (end > 80 ? 80 : 70)), c.key());
        c.findLt(key(95));
        fastAssertArrayEquals(key(end > 90 ? 90 : (end > 80 ? 80 : 70)), c.key());
        c.findLt(key(90));
        fastAssertArrayEquals(key(end > 80 ? 80 : 70), c.key());

        c.findGe(key(15));
        fastAssertArrayEquals(key(20), c.key());
        c.findGe(key(20));
        fastAssertArrayEquals(key(20), c.key());

        c.findGt(key(15));
        fastAssertArrayEquals(key(20), c.key());
        c.findGt(key(20));
        fastAssertArrayEquals(key(30), c.key());
    }

    private Index fill() throws Exception {
        Index ix = mDb.openIndex("views");
        for (int i=20; i<=90; i+=10) {
            byte[] key = key(i);
            ix.store(null, key, key);
        }
        return ix;
    }

    private byte[] key(int n) {
        return ("key-" + n).getBytes();
    }
}
