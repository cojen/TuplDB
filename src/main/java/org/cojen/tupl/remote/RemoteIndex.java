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

import org.cojen.dirmi.Disposer;
import org.cojen.dirmi.RemoteFailure;
import org.cojen.dirmi.Restorable;

import org.cojen.tupl.Filter;

import org.cojen.tupl.diag.IndexStats;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RemoteIndex extends RemoteView {
    @RemoteFailure(declared=false)
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

    public IndexStats analyze(byte[] lowKey, byte[] highKey) throws IOException;

    public boolean verify(RemoteVerificationObserver observer) throws IOException;

    @Disposer
    public void close() throws IOException;

    @RemoteFailure(declared=false)
    public boolean isClosed();

    @Disposer
    public void drop() throws IOException;
}
