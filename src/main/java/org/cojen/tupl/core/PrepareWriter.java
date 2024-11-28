/*
 *  Copyright 2019 Cojen.org
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

import java.io.IOException;

import org.cojen.tupl.Transaction;

import org.cojen.tupl.ext.PrepareHandler;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class PrepareWriter extends HandlerWriter<PrepareHandler> implements PrepareHandler {
    PrepareWriter(LocalDatabase db, int handlerId, PrepareHandler recoveryHandler) {
        super(db, handlerId, recoveryHandler);
    }

    @Override
    public void prepare(Transaction txn, byte[] message) throws IOException {
        prepare(txn, message, false);
    }

    @Override
    public void prepareCommit(Transaction txn, byte[] message) throws IOException {
        prepare(txn, message, true);
    }

    private void prepare(Transaction txn, byte[] message, boolean commit) throws IOException {
        if (txn instanceof LocalTransaction local && local.mDatabase == mDatabase) {
            local.prepare(mHandlerId, message, commit);
            return;
        }

        Utils.invalidTransaction(txn);
    }
}
