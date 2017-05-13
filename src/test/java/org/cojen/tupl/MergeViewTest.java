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

package org.cojen.tupl;

import java.util.AbstractMap.SimpleEntry;
import java.util.Iterator;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Stack;
import java.util.TreeMap;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Tests basic behavior of union, intersection, and difference views.
 *
 * @author Brian S O'Neill
 */
public class MergeViewTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(MergeViewTest.class.getName());
    }

    private Database mDb;

    private View mFirstView;
    private View mSecondView;
    private View mMergeView;

    private NavigableMap<String, String> mFirstMap;
    private NavigableMap<String, String> mSecondMap;
    private NavigableMap<String, String> mMergeMap;

    @Before
    public void createTempDb() throws Exception {
        mDb = Database.open(new DatabaseConfig().directPageAccess(false));
        mFirstView = mDb.openIndex("first");
        mSecondView = mDb.openIndex("second");
        mFirstMap = new TreeMap<>();
        mSecondMap = new TreeMap<>();
    }

    @After
    public void teardown() throws Exception {
        mDb.close();
    }

    @Test
    public void emptyUnion() throws Exception {
        buildUnion();
        verifyFull();
    }

    @Test
    public void emptyIntersection() throws Exception {
        buildIntersection();
        verifyFull();
    }

    @Test
    public void emptyDifference() throws Exception {
        buildDifference();
        verifyFull();
    }

    @Test
    public void emptySymmetricDifference() throws Exception {
        buildSymmetricDifference();
        verifyFull();
    }

    @Test
    public void unionBasics() throws Exception {
        basics(this::buildUnion);
    }

    @Test
    public void intersectionBasics() throws Exception {
        basics(this::buildIntersection);
    }

    @Test
    public void differenceBasics() throws Exception {
        basics(this::buildDifference);
    }

    @Test
    public void symmetricDifferenceBasics() throws Exception {
        basics(this::buildSymmetricDifference);
    }

    @Test
    public void intersectionBasicsAlt1() throws Exception {
        basics(this::buildIntersectionAlt1);
    }

    @Test
    public void symmetricDifferenceBasicsAlt1() throws Exception {
        basics(this::buildSymmetricDifferenceAlt1);
    }

    @FunctionalInterface
    static interface Builder {
        public void run() throws Exception;
    }

    private void basics(Builder builder) throws Exception {
        // First only.
        storeFirst("a-key", "a-value");
        storeFirst("b-key", "b-value");
        builder.run(); verifyFull(); clearAll();

        // Second only.
        storeSecond("c-key", "c-value");
        storeSecond("d-key", "d-value");
        builder.run(); verifyFull(); clearAll();

        // First and second, where all of the first are lower than the second.
        storeFirst ("a-key", "a-value");
        storeFirst ("b-key", "b-value");
        storeSecond("c-key", "c-value");
        storeSecond("d-key", "d-value");
        builder.run(); verifyFull(); clearAll();

        // First and second, where all of the second are lower than the first.
        storeSecond("a-key", "a-value");
        storeSecond("b-key", "b-value");
        storeFirst ("c-key", "c-value");
        storeFirst ("d-key", "d-value");
        builder.run(); verifyFull(); clearAll();

        // First and second, with some mingling.
        storeFirst ("a-key", "a-value");
        storeSecond("b-key", "b-value");
        storeFirst ("c-key", "c-value");
        storeSecond("d-key", "d-value");
        storeSecond("e-key", "e-value");
        builder.run(); verifyFull(); clearAll();

        // First and second, with partial duplication.
        storeFirst ("a-key", "a-value");
        storeFirst ("b-key", "b-value");
        storeSecond("b-key", "b-value");
        storeSecond("c-key", "c-value");
        builder.run(); verifyFull(); clearAll();

        // First and second, with full duplication.
        storeFirst ("a-key", "a-value");
        storeSecond("a-key", "a-value");
        storeFirst ("b-key", "b-value");
        storeSecond("b-key", "b-value");
        storeFirst ("c-key", "c-value");
        storeSecond("c-key", "c-value");
        builder.run(); verifyFull(); clearAll();

        // First and second, with partial duplication and mixed values.
        storeFirst ("a-key", "a-value");
        storeFirst ("b-key", "b-value");
        storeSecond("b-key", "b-value2");
        storeSecond("c-key", "c-value");
        builder.run(); verifyFull(); clearAll();
    }

    private void buildUnion() throws Exception {
        mMergeView = mFirstView.viewUnion(null, mSecondView);
        mMergeMap = new TreeMap<>(mSecondMap);
        mMergeMap.putAll(mFirstMap);
    }

    private void buildIntersection() throws Exception {
        buildIntersection(0);
    }

    private void buildIntersectionAlt1() throws Exception {
        buildIntersection(1);
    }

    private void buildIntersection(int variant) throws Exception {
        if (variant == 0) {
            mMergeView = mFirstView.viewIntersection(null, mSecondView);
        } else if (variant == 1) {
            View v1 = mSecondView.viewDifference(Combiner.discard(), mFirstView);
            View v2 = mSecondView.viewDifference(Combiner.second(), mFirstView);
            mMergeView = v2.viewDifference(null, v1);
        } else {
            fail();
        }

        mMergeMap = new TreeMap<>(mFirstMap);
        mMergeMap.keySet().retainAll(mSecondMap.keySet());
    }

    private void buildDifference() throws Exception {
        mMergeView = mFirstView.viewDifference(null, mSecondView);
        mMergeMap = new TreeMap<>(mFirstMap);
        mMergeMap.keySet().removeAll(mSecondMap.keySet());
    }

    private void buildSymmetricDifference() throws Exception {
        buildSymmetricDifference(0);
    }

    private void buildSymmetricDifferenceAlt1() throws Exception {
        buildSymmetricDifference(1);
    }

    private void buildSymmetricDifference(int variant) throws Exception {
        if (variant == 0) {
            mMergeView = mFirstView.viewUnion(Combiner.discard(), mSecondView);
        } else if (variant == 1) {
            View v1 = mFirstView.viewDifference(null, mSecondView);
            View v2 = mSecondView.viewDifference(null, mFirstView);
            mMergeView = v1.viewUnion(null, v2);
        } else {
            fail();
        }

        mMergeMap = new TreeMap<>(mFirstMap);
        mMergeMap.putAll(mSecondMap);
        NavigableMap<String, String> intersection = new TreeMap<>(mFirstMap);
        intersection.keySet().retainAll(mSecondMap.keySet());
        mMergeMap.keySet().removeAll(intersection.keySet());
    }

    private void clearAll() throws Exception {
        mFirstView.viewKeys().newUpdater(null).updateAll((k, v) -> null);
        mSecondView.viewKeys().newUpdater(null).updateAll((k, v) -> null);
        mFirstMap.clear();
        mSecondMap.clear();
    }

    private void storeFirst(String key, String value) throws Exception {
        mFirstView.store(null, toBytes(key), toBytes(value));
        mFirstMap.put(key, value);
    }

    private void storeSecond(String key, String value) throws Exception {
        mSecondView.store(null, toBytes(key), toBytes(value));
        mSecondMap.put(key, value);
    }

    private static byte[] toBytes(String str) {
        return str.getBytes();
    }

    private static String toString(byte[] bytes) {
        return new String(bytes);
    }

    private void verifyFull() throws Exception {
        verifySize();
        verifyIteration();
        verifyReverseIteration();
        verifyLookup();
        verifyDiscarded();
        verifyDirectionReversal(false);
        verifyDirectionReversal(true);
    }

    private void verifySize() throws Exception {
        assertEquals(mMergeView.count(null, null), mMergeMap.size());
    }

    private void verifyIteration() throws Exception {
        Cursor c = mMergeView.newCursor(null);
        Iterator<Map.Entry<String, String>> it = mMergeMap.entrySet().iterator();
        for (c.first(); c.key() != null; c.next()) {
            Map.Entry<String, String> e = it.next();
            assertEquals(e.getKey(), toString(c.key()));
            assertEquals(e.getValue(), toString(c.value()));
        }
        assertFalse(it.hasNext());
    }

    private void verifyReverseIteration() throws Exception {
        Cursor c = mMergeView.newCursor(null);
        Iterator<Map.Entry<String, String>> it = mMergeMap.descendingMap().entrySet().iterator();
        for (c.last(); c.key() != null; c.previous()) {
            Map.Entry<String, String> e = it.next();
            assertEquals(e.getKey(), toString(c.key()));
            assertEquals(e.getValue(), toString(c.value()));
        }
        assertFalse(it.hasNext());
    }

    private void verifyLookup() throws Exception {
        Cursor c = mMergeView.newCursor(null);
        for (c.first(); c.key() != null; c.next()) {
            assertArrayEquals(c.value(), mMergeView.load(null, c.key()));
        }
    }

    private void verifyDiscarded() throws Exception {
        verifyDiscarded(mFirstMap);
        verifyDiscarded(mSecondMap);
    }

    private void verifyDiscarded(NavigableMap<String, String> source) throws Exception {
        for (String key : source.keySet()) {
            String v1 = mMergeMap.get(key);
            byte[] v2 = mMergeView.load(null, toBytes(key));
            if (v1 == null) {
                assertNull(v2);
            } else {
                assertTrue(v2 != null);
                assertEquals(v1, toString(v2));
            }
        }
    }

    private void verifyDirectionReversal(boolean reverse) throws Exception {
        for (int limit = 1; ; limit++) {
            Stack<Map.Entry<String, String>> s = new Stack<>();

            View view = mMergeView;

            if (reverse) {
                view = view.viewReverse();
            }

            Cursor c = view.newCursor(null);

            while (true) {
                if (c.key() == null) {
                    return;
                }

                if (s.size() >= limit) {
                    while (true) {
                        c.previous();
                        if (c.key() == null) {
                            break;
                        }
                        Map.Entry<String, String> e = s.pop();
                        assertEquals(e.getKey(), toString(c.key()));
                        assertEquals(e.getValue(), toString(c.value()));
                    }
                    break;
                }

                s.add(new SimpleEntry<>(toString(c.key()), toString(c.value())));
                c.next();
            }
        }
    }
}
