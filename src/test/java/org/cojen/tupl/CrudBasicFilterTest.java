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
import java.util.Random;

/**
 * Tests that operations against a basic filtered view still work.
 *
 * @author Brian S O'Neill
 */
public class CrudBasicFilterTest extends CrudNonDurableTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CrudBasicFilterTest.class.getName());
    }

    private final Map<View, Index> mViews = new HashMap<>();

    @Override
    protected View openIndex(String name) throws Exception {
        Index ix = mDb.findIndex(name);
        boolean isNew = ix == null;
        ix = mDb.openIndex(name);

        if (isNew) {
            // Fill the index with entries which should be filtered out.
            fillNew(ix);
        }

        // Only values that don't start with the magic text are allowed by the filter.
        View view = ix.viewTransformed(new BasicFilter());

        mViews.put(view, ix);
        return view;
    }

    @Override
    protected boolean verify(View view) throws Exception {
        return mViews.get(view).verify(null);
    }

    static void fillNew(Index ix) throws java.io.IOException {
        Random rnd = new Random(9821314);
        for (int i=0; i<100; i++) {
            byte[] key = new byte[4];
            rnd.nextBytes(key);
            byte[] value = new byte[10];
            rnd.nextBytes(value);
            value[0] = 'm';
            value[1] = 'a';
            value[2] = 'G';
            value[3] = '{';
            ix.store(null, key, value);
        }
    }

    static class BasicFilter implements Filter {
        @Override
        public boolean isAllowed(byte[] key, byte[] value) {
            if (value == null || value.length < 4) {
                return true;
            }
            return value[0] != 'm' || value[1] != 'a' || value[2] != 'G' || value[3] != '{';
        }
    }
}
