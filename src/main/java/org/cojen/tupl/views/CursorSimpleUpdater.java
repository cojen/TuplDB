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

package org.cojen.tupl.views;

import java.io.IOException;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.UnpositionedCursorException;
import org.cojen.tupl.Updater;

import org.cojen.tupl.core.Utils;

/**
 * Simple updater which doesn't do anything special with the cursor's transaction.
 *
 * @author Brian S O'Neill
 */
public class CursorSimpleUpdater extends CursorScanner implements Updater {
    /**
     * @param cursor unpositioned cursor
     */
    public CursorSimpleUpdater(Cursor cursor) throws IOException {
        super(cursor);
        cursor.first();
        cursor.register();
    }

    @Override
    public boolean update(byte[] value) throws IOException {
        try {
            mCursor.store(value);
        } catch (UnpositionedCursorException e) {
            return false;
        } catch (Throwable e) {
            throw Utils.fail(this, e);
        }
        return step();
    }
}
