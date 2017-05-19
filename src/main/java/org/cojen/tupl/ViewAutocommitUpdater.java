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

import java.io.IOException;

/**
 * Commits the transaction at every step, and exits the scope when closed.
 *
 * @author Brian S O'Neill
 */
class ViewAutocommitUpdater extends ViewUpdater implements Updater {
    /**
     * @param cursor unpositioned cursor with a non-null transaction
     */
    ViewAutocommitUpdater(View view, Cursor cursor) throws IOException {
        super(view, cursor);
    }

    private ViewAutocommitUpdater(Cursor cursor, View view) {
        super(cursor, view);
    }

    @Override
    public boolean step() throws IOException {
        Cursor c = mCursor;
        c.link().commit();
        c.next();
        return c.key() != null;
    }

    @Override
    public boolean step(long amount) throws IOException {
        Cursor c = mCursor;
        if (amount > 0) {
            c.link().commit();
            c.skip(amount);
        } else if (amount < 0) {
            throw ViewUtils.fail(this, new IllegalArgumentException());
        }
        return c.key() != null;
    }

    @Override
    public void close() throws IOException {
        Cursor c = mCursor;
        c.reset();
        c.link().exit();
    }
}
