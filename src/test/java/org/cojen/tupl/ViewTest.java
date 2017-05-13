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
        mDb = newTempDatabase(getClass());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
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
        } catch (ViewConstraintException e) {
        }

        // Allowed.
        view.store(null, key(start - 1), null);
        assertNull(view.exchange(null, key(start - 1), null));

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
        fastAssertArrayEquals(key(10), c.key());
        assertNull(c.value());
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
        } catch (ViewConstraintException e) {
        }

        try {
            view.store(null, key(start), key(start));
            fail();
        } catch (ViewConstraintException e) {
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
        fastAssertArrayEquals(key(10), c.key());
        assertNull(c.value());
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
        } catch (ViewConstraintException e) {
        }

        // Allowed.
        view.store(null, key(end + 1), null);
        assertNull(view.exchange(null, key(end + 1), null));

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
        fastAssertArrayEquals(key(96), c.key());
        assertNull(c.value());
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
        } catch (ViewConstraintException e) {
        }

        try {
            view.store(null, key(end), key(end));
            fail();
        } catch (ViewConstraintException e) {
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
        fastAssertArrayEquals(key(96), c.key());
        assertNull(c.value());
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

    @Test
    public void subReverse() throws Exception {
        Index ix = fill();

        View view = ix.viewReverse().viewGe(key(80)).viewLt(key(30));
        Cursor c = view.newCursor(null);
        int i = 80;
        for (c.first(); c.key() != null; c.next()) {
            fastAssertArrayEquals(key(i), c.key());
            i -= 10;
        }
        assertEquals(30, i);

        c = view.newCursor(null);
        i = 40;
        for (c.last(); c.key() != null; c.previous()) {
            fastAssertArrayEquals(key(i), c.key());
            i += 10;
        }
        assertEquals(90, i);

        view = ix.viewGt(key(30)).viewLe(key(80)).viewReverse();
        c = view.newCursor(null);
        i = 80;
        for (c.first(); c.key() != null; c.next()) {
            fastAssertArrayEquals(key(i), c.key());
            i -= 10;
        }
        assertEquals(30, i);

        c = view.newCursor(null);
        i = 40;
        for (c.last(); c.key() != null; c.previous()) {
            fastAssertArrayEquals(key(i), c.key());
            i += 10;
        }
        assertEquals(90, i);
    }

    @Test
    public void unmodifiable() throws Exception {
        for (int i=0; i<8; i++) {
            unmodifiable(i);
        }
    }

    /**
     * @param mode bit 0: flip construction order; 1: sub view; 2: reverse view
     */
    private void unmodifiable(int mode) throws Exception {
        Index ix = fill();

        View view = ix;
        if ((mode & 4) == 0) {
            view = ix.viewUnmodifiable();
        }
        if ((mode & 2) != 0) {
            view = view.viewGe(key(1));
        }
        if ((mode & 1) != 0) {
            view = view.viewReverse();
        }
        if ((mode & 4) != 0) {
            view = ix.viewUnmodifiable();
        }

        fastAssertArrayEquals(key(20), view.load(null, key(20)));

        try {
            view.store(null, key(1), key(1));
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try {
            view.exchange(null, key(1), key(1));
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try {
            view.insert(null, key(1), key(1));
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try {
            view.replace(null, key(1), key(1));
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try {
            view.update(null, key(1), key(1), key(2));
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try {
            view.delete(null, key(1));
            fail();
        } catch (UnmodifiableViewException e) {
        }

        try {
            view.remove(null, key(1), key(1));
            fail();
        } catch (UnmodifiableViewException e) {
        }

        Cursor c = view.newCursor(null);

        c.find(key(20));
        fastAssertArrayEquals(key(20), c.key());

        try {
            c.store(key(20));
            fail();
        } catch (UnmodifiableViewException e) {
        }

        c.reset();
    }

    @Test
    public void prefix() throws Exception {
        Index ix = fill();

        try {
            View view = ix.viewPrefix("key-".getBytes(), -1);
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            View view = ix.viewPrefix("key-".getBytes(), 5);
            fail();
        } catch (IllegalArgumentException e) {
        }

        View view = ix.viewPrefix("key-".getBytes(), 4);

        int i = 20;
        Cursor c = view.newCursor(null);
        for (c.first(); c.key() != null; c.next()) {
            assertEquals(String.valueOf(i), new String(c.key()));
            fastAssertArrayEquals(key(i), c.value());
            i += 10;
        }
        c.reset();
        assertEquals(100, i);

        {
            c = view.newCursor(null);

            c.findGe(String.valueOf(20).getBytes());
            fastAssertArrayEquals(key(20), c.value());
            c.findGt(String.valueOf(20).getBytes());
            fastAssertArrayEquals(key(30), c.value());
            c.findLe(String.valueOf(90).getBytes());
            fastAssertArrayEquals(key(90), c.value());
            c.findLt(String.valueOf(90).getBytes());
            fastAssertArrayEquals(key(80), c.value());

            c.reset();
        }

        view.store(null, "hello".getBytes(), "world".getBytes());

        fastAssertArrayEquals("world".getBytes(), view.load(null, "hello".getBytes()));
        fastAssertArrayEquals("world".getBytes(), ix.load(null, "key-hello".getBytes()));

        view = ix.viewPrefix("key-".getBytes(), 0);
        try {
            view.store(null, "hello".getBytes(), "world".getBytes());
            fail();
        } catch (ViewConstraintException e) {
            // Key is outside allowed range.
        }

        // Allowed.
        view.store(null, "hello".getBytes(), null);
        assertNull(view.exchange(null, "hello".getBytes(), null));

        view.store(null, "key-hello".getBytes(), "world".getBytes());
        fastAssertArrayEquals("world".getBytes(), view.load(null, "key-hello".getBytes()));
        fastAssertArrayEquals("world".getBytes(), ix.load(null, "key-hello".getBytes()));

        ix.store(null, "hello".getBytes(), "world".getBytes());

        assertNull(view.load(null, "hello".getBytes()));

        c = view.newCursor(null);
        for (c.first(); c.key() != null; c.next()) {
            assertFalse(new String(c.key()).equals("hello"));
        }
        c.reset();

        i = 100;
        c = view.viewReverse().newCursor(null);
        for (c.first(); c.key() != null; c.next()) {
            String strKey = new String(c.key());
            if (strKey.equals("key-hello")) {
                continue;
            }
            i -= 10;
            fastAssertArrayEquals(key(i), c.key());
            fastAssertArrayEquals(key(i), c.value());
        }
        assertEquals(20, i);

        view = ix.viewPrefix("key-5".getBytes(), 4);

        i = 50;
        c = view.newCursor(null);
        for (c.first(); c.key() != null; c.next()) {
            assertEquals(String.valueOf(i), new String(c.key()));
            fastAssertArrayEquals(key(i), c.value());
            i += 10;
        }
        c.reset();
        assertEquals(60, i);
    }

    @Test
    public void prefixCompare() throws Exception {
        Index ix = fill();

        View view = ix.viewPrefix("key-".getBytes(), 4);

        {
            byte[] lastKey = null;

            Cursor c = view.newCursor(null);
            for (c.first(); c.key() != null; c.next()) {
                if (lastKey != null) {
                    int result = c.compareKeyTo(lastKey);
                    assertTrue(result > 0);
                }
                lastKey = c.key();
            }
        }

        {
            byte[] lastKey = null;

            Cursor c = view.newCursor(null);
            for (c.last(); c.key() != null; c.previous()) {
                if (lastKey != null) {
                    int result = c.compareKeyTo(lastKey);
                    assertTrue(result < 0);
                }
                lastKey = c.key();
            }
        }

        view = view.viewReverse();

        {
            byte[] lastKey = null;

            Cursor c = view.newCursor(null);
            for (c.first(); c.key() != null; c.next()) {
                if (lastKey != null) {
                    int result = c.compareKeyTo(lastKey);
                    assertTrue(result > 0);
                }
                lastKey = c.key();
            }
        }

        {
            byte[] lastKey = null;

            Cursor c = view.newCursor(null);
            for (c.last(); c.key() != null; c.previous()) {
                if (lastKey != null) {
                    int result = c.compareKeyTo(lastKey);
                    assertTrue(result < 0);
                }
                lastKey = c.key();
            }
        }
    }

    @Test
    public void counts() throws Exception {
        Index ix = mDb.openIndex("test");

        for (int i=100; i<150; i++) {
            byte[] key = key(i);
            ix.store(null, key, key);
        }

        View view = ix;
        assertEquals(50, view.count(null, null));
        assertEquals(50, view.count(key(100), null));
        assertEquals(50, view.count(null, key(150)));
        assertEquals(50, view.count(key(100), key(150)));

        view = ix.viewGe(key(110));
        assertEquals(40, view.count(null, null));
        assertEquals(40, view.count(key(109), null));
        assertEquals(40, view.count(key(110), null));
        assertEquals(39, view.count(key(111), null));
        assertEquals(38, view.count(key(112), null));

        view = ix.viewGt(key(110));
        assertEquals(39, view.count(null, null));
        assertEquals(39, view.count(key(109), null));
        assertEquals(39, view.count(key(110), null));
        assertEquals(39, view.count(key(111), null));
        assertEquals(38, view.count(key(112), null));

        view = ix.viewLe(key(140));
        assertEquals(41, view.count(null, null));
        assertEquals(41, view.count(null, key(141)));
        assertEquals(40, view.count(null, key(140)));
        assertEquals(39, view.count(null, key(139)));
        assertEquals(38, view.count(null, key(138)));

        view = ix.viewLt(key(140));
        assertEquals(40, view.count(null, null));
        assertEquals(40, view.count(null, key(141)));
        assertEquals(40, view.count(null, key(140)));
        assertEquals(39, view.count(null, key(139)));
        assertEquals(38, view.count(null, key(138)));

        view = ix.viewGe(key(110)).viewLe(key(140));
        assertEquals(31, view.count(null, null));
        assertEquals(31, view.count(null, key(142)));
        assertEquals(31, view.count(null, key(141)));
        assertEquals(30, view.count(null, key(140)));
        assertEquals(29, view.count(null, key(139)));
        assertEquals(31, view.count(key(109), null));
        assertEquals(31, view.count(key(109), key(142)));
        assertEquals(31, view.count(key(109), key(141)));
        assertEquals(30, view.count(key(109), key(140)));
        assertEquals(29, view.count(key(109), key(139)));
        assertEquals(31, view.count(key(110), null));
        assertEquals(31, view.count(key(110), key(142)));
        assertEquals(31, view.count(key(110), key(141)));
        assertEquals(30, view.count(key(110), key(140)));
        assertEquals(29, view.count(key(110), key(139)));
        assertEquals(30, view.count(key(111), null));
        assertEquals(30, view.count(key(111), key(142)));
        assertEquals(30, view.count(key(111), key(141)));
        assertEquals(29, view.count(key(111), key(140)));
        assertEquals(28, view.count(key(111), key(139)));
        assertEquals(29, view.count(key(112), null));
        assertEquals(29, view.count(key(112), key(142)));
        assertEquals(29, view.count(key(112), key(141)));
        assertEquals(28, view.count(key(112), key(140)));
        assertEquals(27, view.count(key(112), key(139)));

        view = ix.viewGe(key(110)).viewLt(key(140));
        assertEquals(30, view.count(null, null));
        assertEquals(30, view.count(null, key(142)));
        assertEquals(30, view.count(null, key(141)));
        assertEquals(30, view.count(null, key(140)));
        assertEquals(29, view.count(null, key(139)));
        assertEquals(30, view.count(key(109), null));
        assertEquals(30, view.count(key(109), key(142)));
        assertEquals(30, view.count(key(109), key(141)));
        assertEquals(30, view.count(key(109), key(140)));
        assertEquals(29, view.count(key(109), key(139)));
        assertEquals(30, view.count(key(110), null));
        assertEquals(30, view.count(key(110), key(142)));
        assertEquals(30, view.count(key(110), key(141)));
        assertEquals(30, view.count(key(110), key(140)));
        assertEquals(29, view.count(key(110), key(139)));
        assertEquals(29, view.count(key(111), null));
        assertEquals(29, view.count(key(111), key(142)));
        assertEquals(29, view.count(key(111), key(141)));
        assertEquals(29, view.count(key(111), key(140)));
        assertEquals(28, view.count(key(111), key(139)));
        assertEquals(28, view.count(key(112), null));
        assertEquals(28, view.count(key(112), key(142)));
        assertEquals(28, view.count(key(112), key(141)));
        assertEquals(28, view.count(key(112), key(140)));
        assertEquals(27, view.count(key(112), key(139)));

        view = ix.viewGt(key(110)).viewLe(key(140));
        assertEquals(30, view.count(null, null));
        assertEquals(30, view.count(null, key(142)));
        assertEquals(30, view.count(null, key(141)));
        assertEquals(29, view.count(null, key(140)));
        assertEquals(28, view.count(null, key(139)));
        assertEquals(30, view.count(key(109), null));
        assertEquals(30, view.count(key(109), key(142)));
        assertEquals(30, view.count(key(109), key(141)));
        assertEquals(29, view.count(key(109), key(140)));
        assertEquals(28, view.count(key(109), key(139)));
        assertEquals(30, view.count(key(110), null));
        assertEquals(30, view.count(key(110), key(142)));
        assertEquals(30, view.count(key(110), key(141)));
        assertEquals(29, view.count(key(110), key(140)));
        assertEquals(28, view.count(key(110), key(139)));
        assertEquals(30, view.count(key(111), null));
        assertEquals(30, view.count(key(111), key(142)));
        assertEquals(30, view.count(key(111), key(141)));
        assertEquals(29, view.count(key(111), key(140)));
        assertEquals(28, view.count(key(111), key(139)));
        assertEquals(29, view.count(key(112), null));
        assertEquals(29, view.count(key(112), key(142)));
        assertEquals(29, view.count(key(112), key(141)));
        assertEquals(28, view.count(key(112), key(140)));
        assertEquals(27, view.count(key(112), key(139)));

        view = ix.viewGt(key(110)).viewLt(key(140));
        assertEquals(29, view.count(null, null));
        assertEquals(29, view.count(null, key(142)));
        assertEquals(29, view.count(null, key(141)));
        assertEquals(29, view.count(null, key(140)));
        assertEquals(28, view.count(null, key(139)));
        assertEquals(29, view.count(key(109), null));
        assertEquals(29, view.count(key(109), key(142)));
        assertEquals(29, view.count(key(109), key(141)));
        assertEquals(29, view.count(key(109), key(140)));
        assertEquals(28, view.count(key(109), key(139)));
        assertEquals(29, view.count(key(110), null));
        assertEquals(29, view.count(key(110), key(142)));
        assertEquals(29, view.count(key(110), key(141)));
        assertEquals(29, view.count(key(110), key(140)));
        assertEquals(28, view.count(key(110), key(139)));
        assertEquals(29, view.count(key(111), null));
        assertEquals(29, view.count(key(111), key(142)));
        assertEquals(29, view.count(key(111), key(141)));
        assertEquals(29, view.count(key(111), key(140)));
        assertEquals(28, view.count(key(111), key(139)));
        assertEquals(28, view.count(key(112), null));
        assertEquals(28, view.count(key(112), key(142)));
        assertEquals(28, view.count(key(112), key(141)));
        assertEquals(28, view.count(key(112), key(140)));
        assertEquals(27, view.count(key(112), key(139)));
    }

    @Test
    public void reverseCounts() throws Exception {
        Index ix = mDb.openIndex("test");

        for (int i=101; i<=150; i++) {
            byte[] key = key(i);
            ix.store(null, key, key);
        }

        View view = ix.viewReverse();

        assertEquals(50, ViewUtils.count(view, false, null, null));
        assertEquals(50, ViewUtils.count(view, false, key(150), null));
        assertEquals(50, ViewUtils.count(view, false, null, key(100)));
        assertEquals(50, ViewUtils.count(view, false, key(150), key(100)));

        assertEquals(50, view.count(null, null));
        assertEquals(50, view.count(key(150), null));
        assertEquals(50, view.count(null, key(100)));
        assertEquals(50, view.count(key(150), key(100)));

        view = ix.viewReverse().viewGe(key(120)).viewLt(key(110));

        assertEquals(10, ViewUtils.count(view, false, null, null));
        assertEquals(10, view.count(null, null));
    }

    @Test
    public void reverseRandom() throws Exception {
        Index ix = mDb.openIndex("test");

        for (int i=101; i<=110; i++) {
            byte[] key = key(i);
            ix.store(null, key, key);
        }

        View view = ix.viewReverse();

        Cursor c = view.newCursor(null);
        c.random(key(108), key(107));
        assertArrayEquals(key(108), c.key());
        c.reset();

        view = ix.viewReverse().viewGe(key(108)).viewLt(key(107));

        c = view.newCursor(null);
        c.random(null, null);
        assertArrayEquals(key(108), c.key()); 
        c.reset();
    }

    @Test
    public void registry() throws Exception {
        Index ix1 = mDb.openIndex("ix1");
        Index ix2 = mDb.openIndex("ix2");

        {
            View registry = mDb.indexRegistryByName();

            byte[] idValue = registry.load(null, "ix1".getBytes());
            assertNotNull(idValue);
            assertEquals(ix1.getId(), Utils.decodeLongBE(idValue, 0));

            idValue = registry.load(null, "ix2".getBytes());
            assertNotNull(idValue);
            assertEquals(ix2.getId(), Utils.decodeLongBE(idValue, 0));

            try {
                registry.store(null, "hello".getBytes(), "world".getBytes());
                fail();
            } catch (UnmodifiableViewException e) {
            }

            Cursor c = registry.newCursor(null);
            c.first();
            fastAssertArrayEquals("ix1".getBytes(), c.key());
            c.next();
            fastAssertArrayEquals("ix2".getBytes(), c.key());
            c.next();
            assertNull(c.key());
        }

        {
            View registry = mDb.indexRegistryById();

            byte[] idKey = new byte[8];
            Utils.encodeLongBE(idKey, 0, ix1.getId());
            byte[] name = registry.load(null, idKey);
            assertNotNull(name);
            assertEquals(ix1.getNameString(), new String(name));

            Utils.encodeLongBE(idKey, 0, ix2.getId());
            name = registry.load(null, idKey);
            assertNotNull(name);
            assertEquals(ix2.getNameString(), new String(name));

            try {
                registry.store(null, "hello".getBytes(), "world".getBytes());
                fail();
            } catch (UnmodifiableViewException e) {
            }

            int compare;
            {
                // Unsigned comparison.
                long a = ix1.getId() + Long.MIN_VALUE;
                long b = ix2.getId() + Long.MIN_VALUE;
                compare = (a < b) ? -1 : ((a == b) ? 0 : 1);
            }

            Cursor c = registry.newCursor(null);
            c.first();
            if (compare < 0) {
                Utils.encodeLongBE(idKey, 0, ix1.getId());
            } else {
                Utils.encodeLongBE(idKey, 0, ix2.getId());
            }
            fastAssertArrayEquals(idKey, c.key());
            c.next();
            if (compare < 0) {
                Utils.encodeLongBE(idKey, 0, ix2.getId());
            } else {
                Utils.encodeLongBE(idKey, 0, ix1.getId());
            }
            fastAssertArrayEquals(idKey, c.key());
            c.next();
            assertNull(c.key());
        }
    }

    @Test
    public void keyOnlyView() throws Exception {
        Index ix = fill();

        long count = ix.count(null, null);
        View view = ix.viewKeys();
        assertEquals(count, view.count(null, null));

        Cursor c = view.newCursor(null);
        long actual = 0;
        for (c.first(); c.key() != null; c.next()) {
            assertTrue(c.value() == Cursor.NOT_LOADED);
            actual++;
        }

        assertEquals(count, actual);

        c.reset();
        assertFalse(c.autoload(true));
        assertFalse(c.autoload(true));
        assertFalse(c.autoload());

        c.first();
        byte[] key = c.key();

        assertTrue(view.load(null, "foo".getBytes()) == null);
        assertTrue(view.load(null, key) == Cursor.NOT_LOADED);

        try {
            view.exchange(null, "foo".getBytes(), "bar".getBytes());
            fail();
        } catch (ViewConstraintException e) {
            // Expected.
        }

        assertNull(view.exchange(null, "foo".getBytes(), null));
        assertTrue(view.exchange(null, key, null) == Cursor.NOT_LOADED);
        assertNull(view.exchange(null, key, null));

        c.next();
        key = c.key();
        c.load();
        assertTrue(c.value() == Cursor.NOT_LOADED);

        try {
            c.store("value".getBytes());
            fail();
        } catch (ViewConstraintException e) {
            // Expected.
        }

        try {
            c.commit("value".getBytes());
            fail();
        } catch (ViewConstraintException e) {
            // Expected.
        }

        try {
            view.store(null, key, "value".getBytes());
            fail();
        } catch (ViewConstraintException e) {
            // Expected.
        }

        try {
            view.insert(null, key, "value".getBytes());
            fail();
        } catch (ViewConstraintException e) {
            // Expected.
        }

        assertFalse(view.insert(null, key, null));

        try {
            view.replace(null, key, "value".getBytes());
            fail();
        } catch (ViewConstraintException e) {
            // Expected.
        }

        assertTrue(view.replace(null, key, null));

        c.load();
        assertNull(c.value());

        c.next();
        key = c.key();

        try {
            view.update(null, key, null, "value".getBytes());
            fail();
        } catch (ViewConstraintException e) {
            // Expected.
        }

        assertFalse(view.update(null, key, null, null));
        assertFalse(view.update(null, key, "old".getBytes(), null));
        assertFalse(view.update(null, key, new byte[0], null));
        assertTrue(view.update(null, key, Cursor.NOT_LOADED, null));

        c.load();
        assertNull(c.value());
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
