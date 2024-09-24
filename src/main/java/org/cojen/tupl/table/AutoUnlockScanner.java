/*
 *  Copyright (C) 2022 Cojen.org
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
import org.cojen.tupl.LockResult;

/**
 * Unlocks every row acquired; must only be used with a fresh REPEATABLE_READ transaction.
 *
 * @author Brian S O'Neill
 */
final class AutoUnlockScanner<R> extends TxnResetScanner<R> {
    AutoUnlockScanner(StoredTable<R> table, ScanController<R> controller) {
        super(table, controller);
    }

    @Override
    protected R evalRow(Cursor c, LockResult result, R row) throws IOException {
        R decoded = mEvaluator.evalRow(c, result, row);
        // Always release the lock, which when joined, combines the secondary and primary locks.
        // When decoded is null, the caller (BasicScanner) releases the lock(s).
        if (decoded != null) {
            c.link().unlock();
        }
        return decoded;
    }
}
