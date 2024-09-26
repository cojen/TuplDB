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

package org.cojen.tupl.table;

import java.io.IOException;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;

/**
 * Updater which uses the {@link LockMode#UPGRADABLE_READ} mode.
 *
 * @author Brian S O'Neill
 */
final class UpgradableUpdater<R> extends BasicUpdater<R> {
    UpgradableUpdater(StoredTable<R> table, ScanController<R> controller) {
        super(table, controller);
    }

    @Override
    protected LockResult toFirst(Cursor c) throws IOException {
        Transaction txn = c.link();
        LockMode original = txn.lockMode();
        txn.lockMode(LockMode.UPGRADABLE_READ);
        try {
            return super.toFirst(c);
        } finally {
            txn.lockMode(original);
        }
    }

    @Override
    protected LockResult toNext(Cursor c) throws IOException {
        Transaction txn = c.link();
        LockMode original = txn.lockMode();
        txn.lockMode(LockMode.UPGRADABLE_READ);
        try {
            return super.toNext(c);
        } finally {
            txn.lockMode(original);
        }
    }
}
