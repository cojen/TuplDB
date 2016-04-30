/*
 *  Copyright 2016 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;

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

    synchronized void asReplica() {
        mState = REPLICA;
        if (mWriter != null) {
            mWriter.close();
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
    public void recover(EventListener listener) {
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
            return -1;
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    @Override
    public synchronized void flip() {
        mWriter = mState == LEADER ? new NonWriter() : null;
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
    public void checkpointed(long position) {
    }

    @Override
    public synchronized void close() {
        mState = CLOSED;
        notifyAll();
    }

    private class NonWriter implements Writer {
        private final List<Runnable> mCallbacks = new ArrayList<>();

        private boolean mClosed;

        @Override
        public long position() {
            return 0;
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
        public synchronized long write(byte[] b, int off, int len) {
            return mClosed ? -1 : 0;
        }

        @Override
        public synchronized boolean confirm(long position, long timeoutNanos) {
            return !mClosed;
        }

        synchronized void close() {
            mClosed = true;
            for (Runnable r : mCallbacks) {
                r.run();
            }
        }
    }
}
