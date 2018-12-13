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
        LCache<TestEntry, Object> cache = new LCache<>(1);

        TestEntry e1 = new TestEntry(1, 1);
        TestEntry e2 = new TestEntry(2, 2);

        assertNull(cache.remove(1, this));
        assertNull(cache.add(e1));
        assertNull(cache.remove(2, this));
        assertTrue(e1 == cache.remove(1, this));
        assertNull(cache.remove(1, this));

        assertNull(cache.add(e1));
        assertTrue(e1 == cache.add(e2));
        assertNull(cache.remove(1, this));
        assertTrue(e2 == cache.remove(2, this));
        assertNull(cache.remove(2, this));

        assertNull(cache.add(e1));
        assertNull(cache.add(e1));
        assertTrue(e1 == cache.remove(1, this));

        TestEntry alt1 = new TestEntry(1, 1);
        assertNull(cache.add(e1));
        assertTrue(alt1 == cache.add(alt1));
        assertNull(cache.add(e1));
        assertTrue(e1 == cache.remove(1, this));
    }

    @Test
    public void evictLRU() {
        LCache<TestEntry, Object> cache = new LCache<>(3);

        TestEntry e1 = new TestEntry(1, 1);
        TestEntry e2 = new TestEntry(2, 2);
        TestEntry e3 = new TestEntry(3, 3);
        TestEntry e4 = new TestEntry(4, 4);

        assertNull(cache.add(e1));
        assertNull(cache.add(e2));
        assertNull(cache.add(e3));
        assertTrue(e1 == cache.add(e4));
        assertNull(cache.remove(1, this));
        assertTrue(e2 == cache.remove(2, this));
        assertTrue(e3 == cache.remove(3, this));
        assertTrue(e4 == cache.remove(4, this));

        assertNull(cache.add(e1));
        assertNull(cache.add(e2));
        assertNull(cache.add(e3));
        assertTrue(e1 == cache.remove(1, this));
        assertNull(cache.add(e4));
        assertTrue(e2 == cache.add(e1));
        assertTrue(e1 == cache.remove(1, this));
        assertNull(cache.remove(2, this));
        assertTrue(e3 == cache.remove(3, this));
        assertTrue(e4 == cache.remove(4, this));

        assertNull(cache.add(e1));
        assertNull(cache.add(e2));
        assertNull(cache.add(e3));
        assertTrue(e2 == cache.remove(2, this));
        assertNull(cache.add(e4));
        assertTrue(e1 == cache.add(e2));
        assertNull(cache.remove(1, this));
        assertTrue(e2 == cache.remove(2, this));
        assertTrue(e3 == cache.remove(3, this));
        assertTrue(e4 == cache.remove(4, this));

        assertNull(cache.add(e1));
        assertNull(cache.add(e2));
        assertNull(cache.add(e3));
        assertTrue(e3 == cache.remove(3, this));
        assertNull(cache.add(e4));
        assertTrue(e1 == cache.add(e3));
        assertNull(cache.remove(1, this));
        assertTrue(e2 == cache.remove(2, this));
        assertTrue(e3 == cache.remove(3, this));
        assertTrue(e4 == cache.remove(4, this));
    }

    @Test
    public void cacheCheck() {
        LCache<TestEntry, Object> cache = new LCache<>(3);

        TestEntry e1 = new TestEntry(1, 1);
        TestEntry e2 = new TestEntry(2, 2);

        assertNull(cache.add(e1));
        assertNull(cache.add(e2));

        assertNull(cache.remove(1, "foo"));
        assertTrue(e2 == cache.remove(2, this));
        assertTrue(e1 == cache.remove(1, this));
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
