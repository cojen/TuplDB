/*
 *  Copyright (C) 2021 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.rows;

import java.io.IOException;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

/**
 * Updater which uses the {@link LockMode#UPGRADABLE_READ} mode.
 *
 * @author Brian S O'Neill
 */
class UpgradableRowUpdater<R> extends BasicRowUpdater<R> {
    LockMode mOriginalMode;

    /**
     * @param cursor linked transaction must not be null
     */
    UpgradableRowUpdater(View view, Cursor cursor, RowDecoderEncoder<R> decoder) {
        super(view, cursor, decoder);
    }

    @Override
    protected LockResult toFirst(Cursor c) throws IOException {
        Transaction txn = c.link();
        mOriginalMode = txn.lockMode();
        txn.lockMode(LockMode.UPGRADABLE_READ);
        return super.toFirst(c);
    }

    @Override
    protected void finished() throws IOException {
        LockMode original = mOriginalMode;
        if (original != null) {
            mOriginalMode = null;
            mCursor.link().lockMode(original);
        }
        super.finished();
    }
}
