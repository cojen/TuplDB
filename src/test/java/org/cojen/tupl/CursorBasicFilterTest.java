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

import java.util.HashMap;
import java.util.Map;

import org.junit.*;

/**
 * Tests that operations against a basic filtered view still work.
 *
 * @author Brian S O'Neill
 */
public class CursorBasicFilterTest extends CursorNonDurableTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CursorBasicFilterTest.class.getName());
    }

    private final Map<View, Index> mViews = new HashMap<>();

    @Override
    protected View openIndex(String name) throws Exception {
        Index ix = mDb.findIndex(name);
        boolean isNew = ix == null;
        ix = mDb.openIndex(name);

        if (isNew) {
            // Fill the index with entries which should be filtered out.
            CrudBasicFilterTest.fillNew(ix);
        }

        // Only values that don't start with the magic text are allowed by the filter.
        View view = ix.viewTransformed(new CrudBasicFilterTest.BasicFilter());

        mViews.put(view, ix);
        return view;
    }

    @Override
    protected boolean verify(View view) throws Exception {
        return mViews.get(view).verify(null);
    }

    @Override
    protected TreeCursor treeCursor(Cursor c) {
        return (TreeCursor) ((TransformedCursor) c).source();
    }

    @Test
    @Ignore
    @Override
    public void randomRange() throws Exception {
        // Filtering throws off the random distribution too much.
    }

    @Test
    @Ignore
    @Override
    public void lockNoLoad() throws Exception {
        // Transformer requires value to be loaded.
    }
}
