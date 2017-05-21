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
 * Updater which uses the {@link LockMode#UPGRADABLE_READ} mode.
 *
 * @author Brian S O'Neill
 */
class ViewUpgradableUpdater extends ViewScanner implements Updater {
    private LockMode mOriginalMode;

    /**
     * @param cursor unpositioned cursor; must be linked to a non-null transaction
     */
    ViewUpgradableUpdater(View view, Cursor cursor) throws IOException {
        super(view, cursor);
        Transaction txn = cursor.link();
        mOriginalMode = txn.lockMode();
        txn.lockMode(LockMode.UPGRADABLE_READ);
    }

    @Override
    public boolean step() throws IOException {
        tryStep: {
            Cursor c = mCursor;
            try {
                c.next();
            } catch (UnpositionedCursorException e) {
                break tryStep;
            } catch (Throwable e) {
                throw ViewUtils.fail(this, e);
            }
            if (c.key() != null) {
                return true;
            }
        }
        resetTxnMode();
        return false;
    }

    @Override
    public boolean step(long amount) throws IOException {
        if (amount < 0) {
            throw new IllegalArgumentException();
        }
        tryStep: {
            Cursor c = mCursor;
            if (amount > 0) {
                try {
                    c.skip(amount);
                } catch (UnpositionedCursorException e) {
                    break tryStep;
                } catch (Throwable e) {
                    throw ViewUtils.fail(this, e);
                }
            }
            if (c.key() != null) {
                return true;
            }
        }
        resetTxnMode();
        return false;
    }

    @Override
    public boolean update(byte[] value) throws IOException {
        try {
            mCursor.store(value);
        } catch (UnpositionedCursorException e) {
            resetTxnMode();
            return false;
        } catch (Throwable e) {
            throw ViewUtils.fail(this, e);
        }
        return step();
    }

    @Override
    public void close() throws IOException {
        resetTxnMode();
        mCursor.reset();
    }

    private void resetTxnMode() {
        LockMode original = mOriginalMode;
        if (original != null) {
            mOriginalMode = null;
            mCursor.link().lockMode(original);
        }
    }
}
