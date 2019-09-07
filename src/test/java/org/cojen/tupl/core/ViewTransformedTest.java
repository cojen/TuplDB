/*
 *  Copyright (C) 2019 Cojen.org
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

package org.cojen.tupl.core;

import org.cojen.tupl.*;

/**
 * Tests that view operations on a basic transformed view still work.
 *
 * @author Brian S O'Neill
 */
public class ViewTransformedTest extends ViewTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ViewTransformedTest.class.getName());
    }

    @Override
    protected View openIndex(String name) throws Exception {
        return mDb.openIndex(name).viewTransformed(new BasicTransform());
    }

    static class BasicTransform implements Transformer {
        @Override
        public byte[] transformValue(byte[] value, byte[] key, byte[] tkey) {
            if (value == null || value == Cursor.NOT_LOADED) {
                return value;
            }
            byte[] copy = value.clone();
            for (int i=0; i<copy.length; i++) {
                copy[i] += 1;
            }
            return copy;
        }

        @Override
        public byte[] inverseTransformValue(byte[] tvalue, byte[] key, byte[] tkey) {
            if (tvalue == null || tvalue == Cursor.NOT_LOADED) {
                return tvalue;
            }
            byte[] copy = tvalue.clone();
            for (int i=0; i<copy.length; i++) {
                copy[i] -= 1;
            }
            return copy;
        }
    }
}
