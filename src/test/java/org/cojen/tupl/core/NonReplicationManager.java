/*
 *  Copyright (C) 2011-2017 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.core;

import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

import org.cojen.tupl.*;

import org.cojen.tupl.ext.ReplicationManager;

/**
 * Implementation fails to write anything.
 *
 * @author Brian S O'Neill
 */
class NonReplicationManager implements ReplicationManager {
    private static final int REPLICA = 0, LEADER = 1, CLOSED = 2;

    private int mState;
    private NonWriter mWriter;

    synchronized void asReplica() throws InterruptedException {
        mState = REPLICA;
        if (mWriter != null) {
            mWriter.close();
            while (mWriter != null) {
                wait();
            }
        }
        notifyAll();
    }

    synchronized void asLeader() {
        mState = LEADER;
        notifyAll();
    }

    @Override
    public long encoding() {
        return 1;
    }

    @Override
    public void start(long position) {
    }

    @Override
    public long readPosition() {
        return 0;
    }

    @Override
    public synchronized int read(byte[] b, int off, int len) throws IOException {
        try {
            while (mState == REPLICA) {
                wait();
            }
            mWriter = new NonWriter();
            return -1;
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    @Override
    public synchronized Writer writer() throws IOException {
        return mWriter;
    }

    @Override
    public void sync() {
    }

    @Override
    public void syncConfirm(long position, long timeoutNanos) {
    }

    @Override
    public boolean failover() throws IOException {
        return mState == REPLICA;
    }

    @Override
    public void checkpointed(long position) {
    }

    @Override
    public synchronized void close() {
        mState = CLOSED;
        notifyAll();
    }

    synchronized void toReplica() {
        mState = REPLICA;
        if (mWriter != null) {
            mWriter.close();
        }
        mWriter = null;
        notifyAll();
    }

    synchronized void toReplica(long position) {
        mState = REPLICA;
        if (mWriter != null) {
            mWriter.close(position);
        }
        mWriter = null;
        notifyAll();
    }

    /**
     * @param only suspend for given thread
     */
    void suspendConfirmation(Thread t) {
        NonWriter writer;
        synchronized (this) {
            writer = mWriter;
        }
        writer.suspendConfirmation(t);
    }

    /**
     * @param only suspend for given thread
     * @param position confirmEnd position to rollback to
     */
    void suspendConfirmation(Thread t, long position) {
        NonWriter writer;
        synchronized (this) {
            writer = mWriter;
        }
        writer.suspendConfirmation(t, position);
    }

    private class NonWriter implements Writer {
        private final List<Runnable> mCallbacks = new ArrayList<>();

        private Thread mSuspend;
        private long mSuspendPosition;
        private boolean mClosed;
        private long mPosition;

        @Override
        public synchronized long position() {
            return mPosition;
        }

        @Override
        public synchronized long confirmedPosition() {
            return mSuspendPosition == 0 ? mPosition : mSuspendPosition;
        }

        @Override
        public synchronized boolean leaderNotify(Runnable callback) {
            if (mClosed) {
                return false;
            } else {
                mCallbacks.add(callback);
                return true;
            }
        }

        @Override
        public synchronized boolean write(byte[] b, int off, int len, long commitPos) {
            if (mClosed) {
                return false;
            }
            mPosition += len;
            // Confirm method is called by checkpointer and main thread.
            notifyAll();
            return true;
        }

        @Override
        public synchronized boolean confirm(long position, long timeoutNanos) {
            while (true) {
                if (mSuspendPosition == 0 || position <= mSuspendPosition) {
                    if (mSuspend != Thread.currentThread()) {
                        if (mPosition >= position) {
                            return true;
                        }
                        if (mClosed) {
                            return false;
                        }
                    }
                }
                try {
                    wait();
                } catch (InterruptedException e) {
                }
                if (mClosed) {
                    return false;
                }
            }
        }

        @Override
        public synchronized long confirmEnd(long timeoutNanos)
            throws ConfirmationFailureException
        {
            if (!mClosed) {
                throw new ConfirmationFailureException("Not closed");
            }
            toReplica();
            return mPosition;
        }

        synchronized void suspendConfirmation(Thread t) {
            mSuspend = t;
            mSuspendPosition = 0;
            if (t == null) {
                notifyAll();
            }
        }

        synchronized void suspendConfirmation(Thread t, long position) {
            mSuspendPosition = position;
            mSuspend = t;
            mPosition = position;
            if (t == null) {
                notifyAll();
            }
        }

        synchronized void close() {
            mClosed = true;
            for (Runnable r : mCallbacks) {
                r.run();
            }
            mSuspend = null;
            notifyAll();
        }

        synchronized void close(long position) {
            close();
            mPosition = position;
        }
    }
}
