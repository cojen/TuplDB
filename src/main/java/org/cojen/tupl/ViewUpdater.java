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

import java.io.IOException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ViewUpdater extends ViewScanner implements Updater {
    /**
     * @param cursor unpositioned cursor
     */
    ViewUpdater(View view, Cursor cursor) throws IOException {
        super(view, cursor);
    }

    private ViewUpdater(Cursor cursor, View view) {
        super(cursor, view);
    }

    @Override
    public boolean update(byte[] value) throws IOException {
        try {
            mCursor.store(value);
        } catch (IllegalStateException e) {
            if (mCursor.key() == null) {
                return false;
            }
            throw e;
        }
        return step();
    }

    @Override
    public Updater trySplit() throws IOException {
        return (Updater) super.trySplit();
    }

    @Override
    protected Updater newScanner(Cursor cursor, View view) {
        return new ViewUpdater(cursor, view);
    }
}
