/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class LCacheTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LCacheTest.class.getName());
    }

    @Test
    public void singleton() {
        var cache = new LCache<TestEntry, Object>(1);

        var e1 = new TestEntry(1, 1);
        var e2 = new TestEntry(2, 2);

        assertNull(cache.remove(1, this));
        assertNull(cache.add(e1));
        assertNull(cache.remove(2, this));
        assertSame(e1, cache.remove(1, this));
        assertNull(cache.remove(1, this));

        assertNull(cache.add(e1));
        assertSame(e1, cache.add(e2));
        assertNull(cache.remove(1, this));
        assertSame(e2, cache.remove(2, this));
        assertNull(cache.remove(2, this));

        assertNull(cache.add(e1));
        assertNull(cache.add(e1));
        assertSame(e1, cache.remove(1, this));

        TestEntry alt1 = new TestEntry(1, 1);
        assertNull(cache.add(e1));
        assertSame(alt1, cache.add(alt1));
        assertNull(cache.add(e1));
        assertSame(e1, cache.remove(1, this));
    }

    @Test
    public void evictLRU() {
        var cache = new LCache<TestEntry, Object>(3);

        var e1 = new TestEntry(1, 1);
        var e2 = new TestEntry(2, 2);
        var e3 = new TestEntry(3, 3);
        var e4 = new TestEntry(4, 4);

        assertNull(cache.add(e1));
        assertNull(cache.add(e2));
        assertNull(cache.add(e3));
        assertSame(e1, cache.add(e4));
        assertNull(cache.remove(1, this));
        assertSame(e2, cache.remove(2, this));
        assertSame(e3, cache.remove(3, this));
        assertSame(e4, cache.remove(4, this));

        assertNull(cache.add(e1));
        assertNull(cache.add(e2));
        assertNull(cache.add(e3));
        assertSame(e1, cache.remove(1, this));
        assertNull(cache.add(e4));
        assertSame(e2, cache.add(e1));
        assertSame(e1, cache.remove(1, this));
        assertNull(cache.remove(2, this));
        assertSame(e3, cache.remove(3, this));
        assertSame(e4, cache.remove(4, this));

        assertNull(cache.add(e1));
        assertNull(cache.add(e2));
        assertNull(cache.add(e3));
        assertSame(e2, cache.remove(2, this));
        assertNull(cache.add(e4));
        assertSame(e1, cache.add(e2));
        assertNull(cache.remove(1, this));
        assertSame(e2, cache.remove(2, this));
        assertSame(e3, cache.remove(3, this));
        assertSame(e4, cache.remove(4, this));

        assertNull(cache.add(e1));
        assertNull(cache.add(e2));
        assertNull(cache.add(e3));
        assertSame(e3, cache.remove(3, this));
        assertNull(cache.add(e4));
        assertSame(e1, cache.add(e3));
        assertNull(cache.remove(1, this));
        assertSame(e2, cache.remove(2, this));
        assertSame(e3, cache.remove(3, this));
        assertSame(e4, cache.remove(4, this));
    }

    @Test
    public void cacheCheck() {
        var cache = new LCache<TestEntry, Object>(3);

        var e1 = new TestEntry(1, 1);
        var e2 = new TestEntry(2, 2);

        assertNull(cache.add(e1));
        assertNull(cache.add(e2));

        assertNull(cache.remove(1, "foo"));
        assertSame(e2, cache.remove(2, this));
        assertSame(e1, cache.remove(1, this));
    }

    @Test
    public void growMax() {
        growMax(4);
    }

    @Test
    public void growMaxRehash() {
        growMax(100);
    }

    private void growMax(int newMax) {
        var cache = new LCache<TestEntry, Object>(3);

        var e1 = new TestEntry(1, 1);
        var e2 = new TestEntry(2, 2);
        var e3 = new TestEntry(3, 3);
        var e4 = new TestEntry(4, 4);

        assertNull(cache.add(e1));
        assertNull(cache.add(e2));
        assertNull(cache.add(e3));
        assertSame(e1, cache.add(e4));
        assertEquals(3, cache.size());

        assertNull(cache.maxSize(newMax));

        assertNull(cache.add(e1));
        assertEquals(4, cache.size());

        assertSame(e1, cache.remove(1, this));
        assertSame(e2, cache.remove(2, this));
        assertSame(e3, cache.remove(3, this));
        assertSame(e4, cache.remove(4, this));
        assertEquals(0, cache.size());

        for (int i=1; i<=newMax; i++) {
            assertNull(cache.add(new TestEntry(i, i)));
        }
        assertEquals(newMax, cache.size());

        TestEntry e = cache.add(new TestEntry(1000, 1000));
        assertNotNull(e);
        assertEquals(1, e.mKey);
        assertEquals(newMax, cache.size());
    }

    @Test
    public void reduceMax() {
        reduceMax(999);
    }

    @Test
    public void reduceMaxRehash() {
        reduceMax(3);
    }

    private void reduceMax(int newMax) {
        var cache = new LCache<TestEntry, Object>(1000);

        for (int i=1; i<=1000; i++) {
            assertNull(cache.add(new TestEntry(i, i)));
        }

        assertEquals(1000, cache.size());

        int expect = 1;

        TestEntry e = cache.maxSize(newMax);

        assertNotNull(e);
        assertEquals(e.mKey, expect);
        expect++;
        assertEquals(999, cache.size());

        while (true) {
            e = cache.maxSize(newMax);
            if (e == null) {
                break;
            }
            assertEquals(e.mKey, expect);
            expect++;
        }

        assertEquals(newMax, cache.size());

        for (int i=0; i<newMax; i++) {
            e = cache.remove(expect, this);
            assertNotNull(e);
            assertEquals(e.mKey, expect);
            expect++;
        }

        assertEquals(0, cache.size());
    }

    static class TestEntry implements LCache.Entry<TestEntry, Object> {
        final long mKey;
        final Object mData;

        TestEntry mNext, mMore, mLess;

        TestEntry(long key, Object data) {
            mKey = key;
            mData = data;
        }

        @Override
        public long cacheKey() {
            return mKey;
        }

        @Override
        public boolean cacheCheck(Object check) {
            return check instanceof LCacheTest;
        }

        @Override
        public TestEntry cacheNext() {
            return mNext;
        }

        @Override
        public void cacheNext(TestEntry next) {
            mNext = next;
        }

        @Override
        public TestEntry cacheMoreUsed() {
            return mMore;
        }

        @Override
        public void cacheMoreUsed(TestEntry more) {
            mMore = more;
        }

        @Override
        public TestEntry cacheLessUsed() {
            return mLess;
        }

        @Override
        public void cacheLessUsed(TestEntry less) {
            mLess = less;
        }
    }
}
