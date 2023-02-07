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

import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.RemoteException;

import org.cojen.tupl.Database;
import org.cojen.tupl.Index;

import org.cojen.tupl.diag.CompactionObserver;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class CompactionObserverRelay extends CompactionObserver {
    /**
     * @param flags bit 1: provide indexNodeVisited messages
     * @param remote provided by the client
     */
    public static boolean compactFile(int flags, Database db,
                                      RemoteCompactionObserver remote, double target)
        throws IOException
    {
        if (remote == null) {
            return db.compactFile(new CompactionObserver(), target);
        }

        var relay = new CompactionObserverRelay(flags, remote);

        try {
            return db.compactFile(relay, target);
        } finally {
            relay.finished();
        }
    }

    private final boolean mPassMessages;
    private final RemoteCompactionObserver mRemote;

    private Pipe mPipe;

    private CompactionObserverRelay(int flags, RemoteCompactionObserver remote) {
        mPassMessages = (flags & 1) != 0;
        mRemote = remote;
    }

    /**
     * Must call when verification finishes to flush the pipe and dispose the remote object.
     */
    private void finished() {
        try {
            try {
                if (mPipe != null) {
                    mPipe.flush();
                    // Note that the pipe is closed instead of recycled. The client might be
                    // stalled reading from the pipe, and so recycling it might cause a new
                    // request using the same pipe to stall as well.
                    mPipe.close();
                    mPipe = null;
                }
            } finally {
                mRemote.finished();
            }
        } catch (IOException e) {
            // Ignore.
        }
    }

    @Override
    public boolean indexBegin(Index index) {
        try {
            return mRemote.indexBegin(index.id());
        } catch (RemoteException e) {
            return false;
        }
    }

    @Override
    public boolean indexComplete(Index index) {
        if (mPipe != null) {
            try {
                mPipe.writeLong(0); // node id 0 is the terminator
                mPipe.flush();
            } catch (IOException e) {
                // Ignore.
            }
        }

        try {
            return mRemote.indexComplete(index.id());
        } catch (RemoteException e) {
            return false;
        }
    }

    @Override
    public boolean indexNodeVisited(long id) {
        if (!mPassMessages) {
            return true;
        }

        try {
            Pipe pipe = mPipe;

            if (pipe == null) {
                mPipe = pipe = mRemote.indexNodeVisited(null);
                // Notify the remote side to expect terminators and wait for an ack.
                pipe.write(1);
                pipe.flush();
                if (pipe.read() < 0) {
                    return false;
                }
            }

            pipe.writeLong(id);

            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
