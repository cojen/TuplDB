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

import org.cojen.tupl.ext.CustomHandler;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ServerCustomHandler implements RemoteCustomHandler {
    private final CustomHandler mHandler;

    ServerCustomHandler(CustomHandler handler) {
        mHandler = handler;
    }

    @Override
    public void redo(RemoteTransaction txn, byte[] message) throws IOException {
        mHandler.redo(ServerTransaction.txn(txn), message);
    }

    @Override
    public void redo(RemoteTransaction txn, byte[] message, long indexId, byte[] key)
        throws IOException
    {
        mHandler.redo(ServerTransaction.txn(txn), message, indexId, key);
    }

    @Override
    public void undo(RemoteTransaction txn, byte[] message) throws IOException {
        mHandler.undo(ServerTransaction.txn(txn), message);
    }

    @Override
    public void dispose() {
    }
}
