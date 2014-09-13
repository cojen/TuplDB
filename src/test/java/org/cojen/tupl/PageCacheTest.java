/*
 *  Copyright 2014 Brian S O'Neill
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
public class PageCacheTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(PageCacheTest.class.getName());
    }

    @Test
    public void fill() {
        fill(false);
    }

    @Test
    public void fillScrambled() {
        fill(true);
    }

    private void fill(boolean scramble) {
        PageCache cache = new DirectPageCache(1_000_000, 4096);
        assertTrue(cache.capacity() > 0);
        assertTrue(cache.capacity() <= 1_000_000);
        assertTrue(cache.maxEntryCount() > 0);
        assertTrue(cache.maxEntryCount() < 1_000_000);

        final long seed = System.nanoTime();
        final byte[] page = new byte[4096];
        Random rnd = new Random(seed);

        for (int i = 0; i < cache.maxEntryCount(); i++) {
            long pageId = i;
            if (scramble) {
                pageId = Utils.scramble(pageId);
            }
            rnd.nextBytes(page);
            cache.add(pageId, page, 0, page.length, true);
        }

        final byte[] actual = new byte[4096];
        rnd = new Random(seed);

        for (int i = 0; i < cache.maxEntryCount(); i++) {
            long pageId = i;
            if (scramble) {
                pageId = Utils.scramble(pageId);
            }
            rnd.nextBytes(page);
            assertTrue(cache.copy(pageId, 0, actual, 0, actual.length));
            fastAssertArrayEquals(page, actual);
        }

        rnd = new Random(seed);

        for (int i = 0; i < cache.maxEntryCount(); i++) {
            long pageId = i;
            if (scramble) {
                pageId = Utils.scramble(pageId);
            }
            rnd.nextBytes(page);
            assertTrue(cache.remove(pageId, actual, 0, actual.length));
            fastAssertArrayEquals(page, actual);
        }

        assertFalse(cache.remove(1, actual, 0, actual.length));

        cache.close();
    }

    @Test
    public void evict() {
        evict(false);
    }

    @Test
    public void evictScrambled() {
        evict(true);
    }

    private void evict(boolean scramble) {
        PageCache cache = new DirectPageCache(100_000, 100);

        final long seed = System.nanoTime();
        final byte[] page = new byte[100];
        Random rnd = new Random(seed);

        for (int i = 0; i < cache.maxEntryCount() * 2; i++) {
            long pageId = i + 1;
            if (scramble) {
                pageId = Utils.scramble(pageId);
            }
            rnd.nextBytes(page);
            cache.add(pageId, page, 0, page.length, true);
        }

        final byte[] actual = new byte[100];
        rnd = new Random(seed);

        for (int i = 0; i < cache.maxEntryCount(); i++) {
            rnd.nextBytes(page);
        }

        for (long i = cache.maxEntryCount(); i < cache.maxEntryCount() * 2; i++) {
            long pageId = i + 1;
            if (scramble) {
                pageId = Utils.scramble(pageId);
            }
            rnd.nextBytes(page);
            assertTrue(cache.remove(pageId, actual, 0, actual.length));
            fastAssertArrayEquals(page, actual);
        }

        assertFalse(cache.remove(1, actual, 0, actual.length));

        cache.close();
    }

    @Test
    public void closed() {
        PageCache cache = new DirectPageCache(256, 4);
        cache.close();

        cache.add(1, new byte[4], 0, 4, true);
        assertFalse(cache.remove(1, new byte[4], 0, 4));

        cache.close();
    }

    @Test
    public void partitions() {
        PageCache cache = new PartitionedPageCache(1_000_000, 4096);
        assertTrue(cache.capacity() > 0);
        assertTrue(cache.capacity() <= 1_000_000);
        assertTrue(cache.maxEntryCount() > 0);
        assertTrue(cache.maxEntryCount() < 1_000_000);

        final long seed = System.nanoTime();
        final byte[] page = new byte[4096];
        Random rnd = new Random(seed);

        for (int i = 0; i < cache.maxEntryCount(); i++) {
            long pageId = i + 1;
            rnd.nextBytes(page);
            cache.add(pageId, page, 0, page.length, true);
        }

        final byte[] actual = new byte[4096];
        rnd = new Random(seed);

        // Might have evicted some, due to uneven distribution.
        int removedCount = 0;

        for (int i = 0; i < cache.maxEntryCount(); i++) {
            long pageId = i + 1;
            rnd.nextBytes(page);
            boolean removed = cache.remove(pageId, actual, 0, actual.length);
            if (removed) {
                fastAssertArrayEquals(page, actual);
                removedCount++;
            }
        }

        assertTrue(removedCount >= cache.maxEntryCount() * 0.9);

        cache.close();
    }
}
