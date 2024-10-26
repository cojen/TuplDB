/*
 *  Copyright (C) 2023 Cojen.org
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

package org.cojen.tupl.remote;

import java.io.IOException;

import org.cojen.tupl.Transaction;

import org.cojen.tupl.ext.CustomHandler;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ClientCustomHandler implements CustomHandler {
    private final ClientDatabase mDb;
    private final RemoteCustomHandler mRemote;

    ClientCustomHandler(ClientDatabase db, RemoteCustomHandler remote) {
        mDb = db;
        mRemote = remote;
    }

    @Override
    public void redo(Transaction txn, byte[] message) throws IOException {
        mRemote.redo(mDb.remoteTransaction(txn), message);
    }

    @Override
    public void redo(Transaction txn, byte[] message, long indexId, byte[] key)
        throws IOException
    {
        mRemote.redo(mDb.remoteTransaction(txn), message, indexId, key);
    }

    @Override
    public void undo(Transaction txn, byte[] message) throws IOException {
        mRemote.undo(mDb.remoteTransaction(txn), message);
    }
}
