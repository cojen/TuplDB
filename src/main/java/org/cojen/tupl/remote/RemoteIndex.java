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

import org.cojen.dirmi.AutoDispose;
import org.cojen.dirmi.Data;
import org.cojen.dirmi.Disposer;
import org.cojen.dirmi.RemoteException;
import org.cojen.dirmi.RemoteFailure;
import org.cojen.dirmi.Restorable;
import org.cojen.dirmi.Serialized;

import org.cojen.tupl.Filter;

import org.cojen.tupl.diag.IndexStats;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@AutoDispose
public interface RemoteIndex extends RemoteView {
    @Data
    public long id();

    @RemoteFailure(declared=false)
    public byte[] name();

    @RemoteFailure(declared=false)
    public String nameString();

    @Restorable
    public RemoteTable asTable(String typeName) throws IOException;

    public long evict(RemoteTransaction txn, byte[] lowKey, byte[] highKey,
                      Filter evictionFilter, boolean autoload)
        throws IOException;

    @Serialized(filter="java.base/*;org.cojen.tupl.**")
    public IndexStats analyze(byte[] lowKey, byte[] highKey) throws IOException;

    /**
     * @param flags bit 1: provide indexNodePassed messages
     */
    public boolean verify(int flags, RemoteVerificationObserver observer, int numThreads)
        throws IOException;

    @Disposer
    public void close() throws IOException;

    public boolean isClosed() throws RemoteException;

    public void drop() throws IOException;
}
