/*
 *  Copyright (C) 2018 Cojen.org
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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;

import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;

import java.util.function.BiConsumer;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * Tests for the TreeMergerClass.
 *
 * @author Brian S O'Neill
 */
public class TreeMergerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(TreeMergerTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mDatabase = (LocalDatabase) TestUtils.newTempDatabase(getClass());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
    }

    private LocalDatabase mDatabase;

    @Test
    public void mergeNothing() throws Exception {
        // numSources = 1, countPerSource = 0, rangePerSource = 100_000, numThreads = 1
        merge(1, 0, 100_000, 1);
    }

    @Test
    public void mergeTiny() throws Exception {
        // numSources = 1, countPerSource = 1, rangePerSource = 100_000, numThreads = 1
        merge(1, 1, 100_000, 1);
    }

    @Test
    public void mergeWithDups1() throws Exception {
        // numSources = 100, countPerSource = 1000, rangePerSource = 100_000, numThreads = 1
        merge(100, 1000, 100_000, 1);
    }

    @Test
    public void mergeWithDups2() throws Exception {
        // numSources = 100, countPerSource = 1000, rangePerSource = 100_000, numThreads = 2
        merge(100, 1000, 100_000, 2);
    }

    @Test
    public void mergeWithDups4() throws Exception {
        // numSources = 1, countPerSource = 100_000, rangePerSource = 200_000, numThreads = 4
        merge(1, 100_000, 200_000, 4);
    }

    @Test
    public void mergeWithDupsStopAndResume() throws Exception {
        // numSources = 1, countPerSource = 100_000, rangePerSource = 200_000, numThreads = 4
        merge(1, 100_000, 200_000, 4, true);
    }

    private void merge(int numSources, int countPerSource, int rangePerSource, int numThreads)
        throws Exception
    {
        merge(numSources, countPerSource, rangePerSource, numThreads, false);
    }

    private void merge(int numSources, int countPerSource, int rangePerSource, int numThreads,
                       boolean stopAndResume)
        throws Exception
    {
        final long seed = 123 + numSources + countPerSource + rangePerSource + numThreads;
        Random rnd = new Random(seed);

        Tree[] sources = new Tree[numSources];
        for (int i=0; i<numSources; i++) {
            Tree source = (Tree) mDatabase.openIndex("test-" + i);
            sources[i] = source;

            byte[] value = ("value-" + i).getBytes();

            for (int j=0; j<countPerSource; j++) {
                byte[] key = String.valueOf(rnd.nextInt(rangePerSource)).getBytes();
                source.store(Transaction.BOGUS, key, value);
            }
        }

        ExecutorService executor;
        if (numThreads == 1) {
            executor = null;
        } else {
            executor = Executors.newCachedThreadPool();
        }

        final List<Tree> results = new ArrayList<>();

        class Merger extends TreeMerger {
            Merger(LocalDatabase db, Tree[] sources, Executor executor, int workerCount) {
                super(db, sources, executor, workerCount);
            }

            @Override
            protected void merged(Tree tree) {
                synchronized (results) {
                    results.add(tree);
                }
            }

            @Override
            protected void remainder(Tree tree) {
                synchronized (results) {
                    results.add(tree);
                    if (tree == null) {
                        results.notifyAll();
                }
                }
            }
        }

        TreeMerger merger = new Merger(mDatabase, sources, executor, numThreads);

        if (stopAndResume) {
            new Thread(() -> {
                sleep(1);
                merger.stop();
            }).start();
        }

        merger.start();

        synchronized (results) {
            while (results.isEmpty()) {
                results.wait();
            }
        }

        assertNull(results.get(results.size() - 1));

        if (countPerSource == 0) {
            assertEquals(1, results.size());
            return;
        }

        if (stopAndResume && results.size() > 2) {
            // Resume the sort with a new merger.

            Tree[] remaining = new Tree[results.size() - 1];
            for (int i=0; i<remaining.length; i++) {
                remaining[i] = results.get(i);
            }

            results.clear();

            TreeMerger remainingMerger = new Merger(mDatabase, remaining, executor, numThreads);
            remainingMerger.start();

            synchronized (results) {
                while (results.isEmpty()) {
                    results.wait();
                }
            }
        }

        assertEquals(2, results.size());
        Tree result = results.get(0);

        long count = result.count(null, null);
        assertTrue(count <= rangePerSource);

        // Verify entries.

        rnd = new Random(seed);
        TreeMap<byte[], byte[]> expected = new TreeMap<>(KeyComparator.THE);

        for (int i=0; i<numSources; i++) {
            byte[] value = ("value-" + i).getBytes();

            for (int j=0; j<countPerSource; j++) {
                byte[] key = String.valueOf(rnd.nextInt(rangePerSource)).getBytes();
                expected.put(key, value);
            }
        }

        assertEquals(expected.size(), count);

        Iterator<Map.Entry<byte[], byte[]>> it = expected.entrySet().iterator();
        try (Cursor c = result.newCursor(null)) {
            c.first();
            while (it.hasNext()) {
                Map.Entry<byte[], byte[]> e = it.next();
                fastAssertArrayEquals(e.getKey(), c.key());
                fastAssertArrayEquals(e.getValue(), c.value());
                c.next();
            }
            assertNull(c.key());
        }
    }
}
