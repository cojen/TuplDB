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

import org.cojen.dirmi.RemoteException;

import org.cojen.tupl.Filter;
import org.cojen.tupl.Index;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.diag.IndexStats;
import org.cojen.tupl.diag.VerificationObserver;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ClientIndex extends ClientView<RemoteIndex> implements Index {
    ClientIndex(ClientDatabase db, RemoteIndex remote) {
        super(db, remote);
    }

    @Override
    public long id() {
        return mRemote.id();
    }

    @Override
    public byte[] name() {
        return mRemote.name();
    }

    @Override
    public String nameString() {
        return mRemote.nameString();
    }

    @Override
    public <R> Table<R> asTable(Class<R> type) throws IOException {
        return ClientCache.get(TupleKey.make.with(this, type), key -> {
            RemoteTable rtable;
            try {
                rtable = mRemote.asTable(type.getName());
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }

            return new ClientTable<>(mDb, rtable, type);
        });
    }

    @Override
    public long evict(Transaction txn, byte[] lowKey, byte[] highKey,
                      Filter evictionFilter, boolean autoload)
        throws IOException
    {
        return mRemote.evict(mDb.remoteTransaction(txn), lowKey, highKey, evictionFilter, autoload);
    }

    @Override
    public IndexStats analyze(byte[] lowKey, byte[] highKey) throws IOException {
        return mRemote.analyze(lowKey, highKey);
    }

    @Override
    public boolean verify(VerificationObserver observer, int numThreads) throws IOException {
        var server = ServerVerificationObserver.make(mDb, observer);
        return server.check(mRemote.verify(server.flags(), server, numThreads));
    }

    @Override
    public void close() {
        close(true, false);
    }

    @Override
    public boolean isClosed() {
        try {
            return mClosed || mRemote.isClosed();
        } catch (RemoteException e) {
            close();
            return true;
        }
    }

    @Override
    public void drop() throws IOException {
        try {
            mRemote.drop();
        } catch (IOException e) {
            checkClosed();
            throw e;
        }
        close();
    }

    protected void close(boolean removeFromCache, boolean remoteClose) {
        if (!mClosed) {
            if (removeFromCache) {
                ClientCache.remove(this);
            }
            mClosed = true;
            try {
                if (remoteClose) {
                    mRemote.close();
                } else {
                    mRemote.dispose();
                }
            } catch (IOException e) {
                // Ignore.
            }
        }
    }

    final static class Temp extends ClientIndex {
        Temp(ClientDatabase db, RemoteIndex remote) {
            super(db, remote);
        }

        @Override
        public void close() {
            close(false, true);
        }
    }
}
