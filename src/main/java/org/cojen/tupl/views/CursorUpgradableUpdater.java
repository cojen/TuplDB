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
import org.cojen.tupl.LockMode;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnpositionedCursorException;
import org.cojen.tupl.Updater;

import org.cojen.tupl.core.Utils;

/**
 * Updater which uses the {@link LockMode#UPGRADABLE_READ} mode.
 *
 * @author Brian S O'Neill
 */
public final class CursorUpgradableUpdater extends CursorScanner implements Updater {
    /**
     * @param cursor unpositioned cursor; must be linked to a non-null transaction
     */
    public CursorUpgradableUpdater(Cursor cursor) throws IOException {
        super(cursor);
        Transaction txn = cursor.link();
        LockMode original = txn.lockMode();
        txn.lockMode(LockMode.UPGRADABLE_READ);
        try {
            cursor.first();
            cursor.register();
        } finally {
            txn.lockMode(original);
        }
    }

    @Override
    public boolean step() throws IOException {
        tryStep: {
            Cursor c = mCursor;
            try {
                Transaction txn = c.link();
                LockMode original = txn.lockMode();
                txn.lockMode(LockMode.UPGRADABLE_READ);
                try {
                    c.next();
                } finally {
                    txn.lockMode(original);
                }
            } catch (UnpositionedCursorException e) {
                break tryStep;
            } catch (Throwable e) {
                throw Utils.fail(this, e);
            }
            if (c.key() != null) {
                return true;
            }
        }
        return false;
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
