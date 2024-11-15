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

package org.cojen.tupl.core;

import org.cojen.tupl.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CursorDefaultTest extends CursorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(CursorDefaultTest.class.getName());
    }

    @Override
    protected View openIndex(String name) throws Exception {
        return new DefaultView(mDb.openIndex(name));
    }

    @Override
    protected boolean verify(View ix) throws Exception {
        return ((Index) (((DefaultView) ix).mSource)).verify(null, 1);
    }

    @Override
    protected BTreeCursor treeCursor(Cursor c) {
        return (BTreeCursor) (((DefaultCursor) c).mSource);
    }
}
