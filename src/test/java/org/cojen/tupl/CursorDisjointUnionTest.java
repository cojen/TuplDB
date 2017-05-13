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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.*;

/**
 * Tests operations against a union which partitions the entries into separate views.
 *
 * @author Brian S O'Neill
 */
public class CursorDisjointUnionTest extends CursorNonDurableTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CursorDisjointUnionTest.class.getName());
    }

    private final Map<View, Index[]> mViews = new HashMap<>();

    @Override
    protected View openIndex(String name) throws Exception {
        Index ix0 = mDb.openIndex(name + "-0");
        Index ix1 = mDb.openIndex(name + "-1");
        View view = ix0.viewUnion(new CrudDisjointUnionTest.Disjoint(), ix1);
        mViews.put(view, new Index[] {ix0, ix1});
        return view;
    }

    @Override
    protected boolean verify(View view) throws Exception {
        boolean result = true;
        for (Index ix : mViews.get(view)) {
            result &= ix.verify(null);
        }
        return result;
    }

    @Override
    protected void verifyExtremities(View ix) throws Exception {
        // Skip.
    }

    @Override
    protected boolean equalPositions(Cursor a, Cursor b) throws Exception {
        // Exact positions of source cursors can be different, due to read-ahead. Instead, just
        // verify that the top-level key and value match.
        return Arrays.equals(a.key(), b.key()) && Arrays.equals(a.value(), b.value());
    }

    @Override
    protected long cursorCount() {
        // Two actual cursors for every user-level cursor.
        return mDb.stats().cursorCount() / 2;
    }

    @Test
    @Ignore
    @Override
    public void randomLock() throws Exception {
        // Random search isn't supported.
    }

    @Test
    @Ignore
    @Override
    public void random() throws Exception {
        // Random search isn't supported.
    }

    @Test
    @Ignore
    @Override
    public void randomNotGhost() throws Exception {
        // Random search isn't supported.
    }

    @Test
    @Ignore
    @Override
    public void randomRange() throws Exception {
        // Random search isn't supported.
    }

    @Test
    @Ignore
    @Override
    public void randomNonRange() throws Exception {
        // Random search isn't supported.
    }
}
