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

import org.cojen.tupl.diag.VerificationObserver;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.Runner;

/**
 * Although this object is used by the client, it acts as a server.
 *
 * @author Brian S O'Neill
 */
final class ServerVerificationObserver implements RemoteVerificationObserver {
    static ServerVerificationObserver make(Database db, VerificationObserver observer) {
        int flags;
        if (observer == null) {
            flags = 0;
            observer = new VerificationObserver();
        } else {
            flags = 1; // bit 1: provide indexNodePassed messages

            try {
                var method = observer.getClass().getMethod
                    ("indexNodePassed", long.class, int.class, int.class, int.class, int.class);
                if (method.getDeclaringClass() == VerificationObserver.class) {
                    // Not overriding the default, so no need to provide the messages.
                    flags = 0;
                }
            } catch (Exception e) {
                throw Utils.rethrow(e);
            }
        }

        return new ServerVerificationObserver(flags, db, observer);
    }

    private final int mFlags;
    private final Database mDb;
    private final VerificationObserver mObserver;

    private boolean mExpectTerminators;
    private boolean mTerminated;
    private Throwable mException;

    /**
     * @param observer non-null local observer
     */
    private ServerVerificationObserver(int flags, Database db, VerificationObserver observer) {
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
    public synchronized boolean indexBegin(long indexId, int height) {
        return mObserver.indexBegin(indexFor(indexId), height);
    }

    @Override
    public synchronized boolean indexComplete(long indexId, boolean passed, String message) {
        if (mExpectTerminators) {
            try {
                while (!mTerminated) {
                    wait();
                }
            } catch (InterruptedException e) {
            }

            mTerminated = false;
        }

        return mObserver.indexComplete(indexFor(indexId), passed, message);
    }

    @Override
    public Pipe indexNodePassed(Pipe pipe) {
        Runner.start(() -> readIndexNodePassed(pipe));
        return null;
    }

    @Override
    public synchronized boolean indexNodeFailed(long id, int level, String message) {
        return mObserver.indexNodeFailed(id, level, message);
    }

    @Override
    public void finished() {
        // Nothing to do.
    }

    private void readIndexNodePassed(Pipe pipe) {
        synchronized (this) {
            mExpectTerminators = true;
        }

        try {
            pipe.read();
            pipe.write(1); // ack
            pipe.flush();

            VerificationObserver observer = mObserver;

            while (true) {
                long id = pipe.readLong();

                if (id == 0) {
                    synchronized (this) {
                        mTerminated = true;
                        notify();
                    }
                    continue;
                }

                int level = pipe.readInt();
                int entryCount = pipe.readInt();
                int freeBytes = pipe.readInt();
                int largeValueCount = pipe.readInt();

                boolean keepGoing;

                synchronized (this) {
                    try {
                        keepGoing = observer.indexNodePassed
                            (id, level, entryCount, freeBytes, largeValueCount);
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
