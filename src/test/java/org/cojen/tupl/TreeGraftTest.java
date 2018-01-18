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

import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * Tests for the Tree.graftTempTree method.
 *
 * @author Brian S O'Neill
 */
public class TreeGraftTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(TreeGraftTest.class.getName());
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
    public void basic() throws Exception {
        Tree t1 = (Tree) mDatabase.newTemporaryIndex();
        Tree t2 = (Tree) mDatabase.newTemporaryIndex();

        byte[] k = "hello".getBytes();
        byte[] v = "world".getBytes();

        t1.store(null, k, v);
        t2.store(null, v, k);

        Tree survivior = Tree.graftTempTree(t1, t2);
        assertEquals(t1, survivior);

        assertEquals(t1, mDatabase.indexById(t1.getId()));
        assertNull(mDatabase.indexById(t2.getId()));

        fastAssertArrayEquals(v, t1.load(null, k));
        fastAssertArrayEquals(k, t1.load(null, v));

        assertEquals(2, t1.count(null, null));

        assertEquals(1, height(t1));
    }

    @Test
    public void heightImbalance() throws Exception {
        heightImbalance(1, 2);
        heightImbalance(1, 2000);
        heightImbalance(40, 70);
        heightImbalance(2000, 1);
        heightImbalance(20000, 1000);
        heightImbalance(1000, 300000);

        // Cannot graft empty trees.
        try {
            heightImbalance(0, 10);
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof UnpositionedCursorException);
        }
        try {
            heightImbalance(1000, 0);
            fail();
        } catch (AssertionError e) {
            assertTrue(e.getCause() instanceof UnpositionedCursorException);
        }
    }

    private void heightImbalance(int count1, int count2) throws Exception {
        try {
            final long seed = 123 + count1 + count2;
            Random rnd = new Random(seed);

            Tree t1 = (Tree) mDatabase.newTemporaryIndex();
            Tree t2 = (Tree) mDatabase.newTemporaryIndex();

            fillTree(t1, count1, rnd, (byte) 0);
            fillTree(t2, count2, rnd, (byte) 1);

            Tree survivor = Tree.graftTempTree(t1, t2);

            if (survivor == t1) {
                assertEquals(t1, mDatabase.indexById(t1.getId()));
                assertNull(mDatabase.indexById(t2.getId()));
            } else {
                assertNull(mDatabase.indexById(t1.getId()));
                assertEquals(t2, mDatabase.indexById(t2.getId()));
            }

            assertEquals(count1 + count2, survivor.count(null, null));

            int maxHeight = Math.max(height(t1), height(t2));
            int actualHeight = height(survivor);
            assertTrue("actualHeight", actualHeight == maxHeight || actualHeight == maxHeight + 1);

            // Verify contents.
            rnd = new Random(seed);
            findInFilledTree(survivor, count1, rnd, (byte) 0);
            findInFilledTree(survivor, count2, rnd, (byte) 1);
        } catch (Throwable e) {
            throw new AssertionError("counts: " + count1 + ", " + count2, e);
        }
    }

    @Test
    public void chain() throws Exception {
        final long seed = 5224555;
        Random rnd = new Random(seed);

        Tree[] trees = new Tree[256];
        for (int i=0; i<trees.length; i++) {
            trees[i] = (Tree) mDatabase.newTemporaryIndex();
            fillTree(trees[i], 100, rnd, (byte) i);
        }

        Tree survivor = trees[0];
        for (int i=1; i<trees.length; i++) {
            survivor = Tree.graftTempTree(survivor, trees[i]);
        }

        assertEquals(100 * trees.length, survivor.count(null, null));

        // Tree height remains reasonable.
        assertEquals(3, height(survivor));

        // Verify contents.
        rnd = new Random(seed);
        for (int i=0; i<trees.length; i++) {
            findInFilledTree(survivor, 100, rnd, (byte) i);
        }
    }

    private static void fillTree(Tree tree, int count, Random rnd, byte keyPrefix)
        throws Exception
    {
        for (int i=0; i<count; i++) {
            byte[] key = TestUtils.randomStr(rnd, 10, 30);
            key[0] = keyPrefix;
            byte[] value = TestUtils.randomStr(rnd, 10, 30);
            tree.store(Transaction.BOGUS, key, value);
        }
    }

    private static void findInFilledTree(Tree tree, int count, Random rnd, byte keyPrefix)
        throws Exception
    {
        TreeCursor tc = tree.newCursor(Transaction.BOGUS);

        for (int i=0; i<count; i++) {
            byte[] key = TestUtils.randomStr(rnd, 10, 30);
            key[0] = keyPrefix;
            byte[] value = TestUtils.randomStr(rnd, 10, 30);
            // Use findNearby to verify that tree extremities are set correctly.
            tc.findNearby(key);
            fastAssertArrayEquals(key, tc.key());
            fastAssertArrayEquals(value, tc.value());
        }

        tc.reset();
    }

    private static int height(Tree tree) throws Exception {
        HeightObserver obs = new HeightObserver();
        assertTrue(tree.verify(obs));
        return obs.height;
    }

    static class HeightObserver extends VerificationObserver {
        @Override
        public boolean indexComplete(Index index, boolean passed, String message) {
            // Don't clear the inherited index and height fields.
            return true;
        }
    }
}
