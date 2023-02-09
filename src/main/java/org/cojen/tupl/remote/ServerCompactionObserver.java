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

import org.cojen.tupl.Database;
import org.cojen.tupl.Index;

import org.cojen.tupl.diag.CompactionObserver;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.Runner;

/**
 * Although this object is used by the client, it acts as a server.
 *
 * @author Brian S O'Neill
 */
final class ServerCompactionObserver implements RemoteCompactionObserver {
    static ServerCompactionObserver make(Database db, CompactionObserver observer) {
        int flags;
        if (observer == null) {
            flags = 0;
            observer = new CompactionObserver();
        } else {
            flags = 1; // bit 1: provide indexNodeVisited messages

            try {
                var method = observer.getClass().getMethod("indexNodeVisited", long.class);
                if (method.getDeclaringClass() == CompactionObserver.class) {
                    // Not overriding the default, so no need to provide the messages.
                    flags = 0;
                }
            } catch (Exception e) {
                throw Utils.rethrow(e);
            }
        }

        return new ServerCompactionObserver(flags, db, observer);
    }

    private final int mFlags;
    private final Database mDb;
    private final CompactionObserver mObserver;

    private boolean mExpectTerminators;
    private boolean mTerminated;
    private Throwable mException;

    /**
     * @param observer non-null local observer
     */
    private ServerCompactionObserver(int flags, Database db, CompactionObserver observer) {
        mFlags = flags;
        mDb = db;
        mObserver = observer;
    }

    /**
     * @return the flags that should be passed to the remote verify method
     */
    int flags() {
        return mFlags;
    }

    /**
     * Throws an exception (which came from the local observer) or else returns the result.
     */
    synchronized boolean check(boolean result) {
        Throwable ex = mException;
        if (ex != null) {
            throw Utils.rethrow(ex);
        }
        return result;
    }

    @Override
    public synchronized boolean indexBegin(long indexId) {
        return mObserver.indexBegin(indexFor(indexId));
    }

    @Override
    public synchronized boolean indexComplete(long indexId) {
        if (mExpectTerminators) {
            try {
                while (!mTerminated) {
                    wait();
                }
            } catch (InterruptedException e) {
            }

            mTerminated = false;
        }

        return mObserver.indexComplete(indexFor(indexId));
    }

    @Override
    public Pipe indexNodeVisited(Pipe pipe) {
        Runner.start(() -> readIndexNodeVisited(pipe));
        return null;
    }

    @Override
    public void finished() {
        // Nothing to do.
    }

    private void readIndexNodeVisited(Pipe pipe) {
        synchronized (this) {
            mExpectTerminators = true;
        }

        try {
            pipe.read();
            pipe.write(1); // ack
            pipe.flush();

            CompactionObserver observer = mObserver;

            while (true) {
                long id = pipe.readLong();

                if (id == 0) {
                    synchronized (this) {
                        mTerminated = true;
                        notify();
                    }
                    continue;
                }

                boolean keepGoing;

                synchronized (this) {
                    try {
                        keepGoing = observer.indexNodeVisited(id);
                    } catch (Throwable e) {
                        mException = e;
                        break;
                    }
                }

                if (!keepGoing) {
                    break;
                }
            }
        } catch (Throwable e) {
            // Ignore.
        }

        Utils.closeQuietly(pipe);
    }

    private Index indexFor(long indexId) {
        try {
            return mDb.indexById(indexId);
        } catch (IllegalArgumentException | IOException e) {
            return null;
        }
    }
}
