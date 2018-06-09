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
 * Commits every transactional update, and exits the scope when closed. For any entry stepped
 * over, acquired locks are released.
 *
 * @author Brian S O'Neill
 */
class CursorAutoCommitUpdater extends CursorNonRepeatableUpdater {
    /**
     * @param cursor unpositioned cursor
     */
    CursorAutoCommitUpdater(Cursor cursor) throws IOException {
        super(cursor);
    }

    @Override
    protected void postUpdate() throws IOException {
        mCursor.link().commit();
    }

    @Override
    protected void finished() throws IOException {
        mCursor.link().exit();
    }
}
