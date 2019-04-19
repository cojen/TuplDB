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
 * Tests that operations against a basic transformed view still work.
 *
 * @author Brian S O'Neill
 */
public class CursorBasicTransformTest extends CursorNonDurableTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CursorBasicTransformTest.class.getName());
    }

    private final Map<View, Index> mViews = new HashMap<>();

    @Override
    protected View openIndex(String name) throws Exception {
        Index ix = mDb.openIndex(name);
        View view = ix.viewTransformed(new CrudBasicTransformTest.BasicTransform());
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

    @Override
    public void lockNoLoad() throws Exception {
        // Transformer requires value to be loaded.
    }
}
