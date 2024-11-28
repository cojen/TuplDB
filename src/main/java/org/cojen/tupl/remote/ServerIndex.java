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

package org.cojen.tupl.remote;

import java.io.IOException;

import org.cojen.dirmi.Session;

import org.cojen.tupl.Filter;
import org.cojen.tupl.Index;

import org.cojen.tupl.diag.IndexStats;

import org.cojen.tupl.table.StoredTable;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ServerIndex extends ServerView<Index> implements RemoteIndex {
    ServerIndex(Index ix) {
        super(ix);
    }

    @Override
    public long id() {
        return mView.id();
    }

    @Override
    public byte[] name() {
        return mView.name();
    }

    @Override
    public String nameString() {
        return mView.nameString();
    }

    @Override
    public RemoteTable asTable(String typeName) throws IOException {
        Class<?> clazz;
        try {
            clazz = Session.current().resolveClass(typeName);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }

        return new ServerTable<>((StoredTable<?>) mView.asTable(clazz));
    }

    @Override
    public long evict(RemoteTransaction txn, byte[] lowKey, byte[] highKey,
                      Filter evictionFilter, boolean autoload)
        throws IOException
    {
        return mView.evict(ServerTransaction.txn(txn), lowKey, highKey, evictionFilter, autoload);
    }

    @Override
    public IndexStats analyze(byte[] lowKey, byte[] highKey) throws IOException {
        return mView.analyze(lowKey, highKey);
    }

    @Override
    public boolean verify(int flags, RemoteVerificationObserver remote, int numThreads)
        throws IOException
    {
        return VerificationObserverRelay.verify
            (flags, remote, obs -> mView.verify(obs, numThreads));
    }

    @Override
    public void close() throws IOException {
        mView.close();
    }

    @Override
    public boolean isClosed() {
        return mView.isClosed();
    }

    @Override
    public void drop() throws IOException {
        mView.drop();
    }
}
