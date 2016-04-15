/*
 *  Copyright 2015 Brian S O'Neill
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
