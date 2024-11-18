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

import org.cojen.tupl.Index;

import org.cojen.tupl.diag.VerificationObserver;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class VerificationObserverRelay extends VerificationObserver {
    /**
     * @param flags bit 1: provide indexNodePassed messages
     * @param remote provided by the client
     * @param upon calls the verify method upon a local index or database instance
     */
    public static boolean verify(int flags, RemoteVerificationObserver remote,
                                 ObserverCallback<VerificationObserver, IOException> upon)
        throws IOException
    {
        if (remote == null) {
            return upon.run(new VerificationObserver() {
                @Override
                public boolean indexNodeFailed(long id, int level, String message) {
                    return false;
                }
            });
        }

        var relay = new VerificationObserverRelay(flags, remote);

        try {
            return upon.run(relay);
        } finally {
            relay.finished();
        }
    }

    private final boolean mPassMessages;
    private final RemoteVerificationObserver mRemote;

    private volatile Pipe mPipe;

    private VerificationObserverRelay(int flags, RemoteVerificationObserver remote) {
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
    public boolean indexBegin(Index index, int height) {
        try {
            return mRemote.indexBegin(index.id(), height);
        } catch (RemoteException e) {
            return false;
        }
    }

    @Override
    public boolean indexComplete(Index index, boolean passed, String message) {
        if (mPipe != null) {
            try {
                mPipe.writeLong(0); // node id 0 is the terminator
                mPipe.flush();
            } catch (IOException e) {
                // Ignore.
            }
        }

        try {
            return mRemote.indexComplete(index.id(), passed, message);
        } catch (RemoteException e) {
            return false;
        }
    }

    @Override
    public boolean indexNodePassed(long id, int level,
                                   int entryCount, int freeBytes, int largeValueCount)
    {
        if (!mPassMessages) {
            return true;
        }

        synchronized (this) {
            try {
                Pipe pipe = mPipe;

                if (pipe == null) {
                    mPipe = pipe = mRemote.indexNodePassed(null);
                    // Notify the remote side to expect terminators and wait for an ack.
                    pipe.write(1);
                    pipe.flush();
                    if (pipe.read() < 0) {
                        return false;
                    }
                }

                pipe.writeLong(id);
                pipe.writeInt(level);
                pipe.writeInt(entryCount);
                pipe.writeInt(freeBytes);
                pipe.writeInt(largeValueCount);

                return true;
            } catch (IOException e) {
                return false;
            }
        }
    }

    @Override
    public boolean indexNodeFailed(long id, int level, String message) {
        try {
            return mRemote.indexNodeFailed(id, level, message);
        } catch (RemoteException e) {
            return false;
        }
    }
}
