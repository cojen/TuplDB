/*
 *  Copyright 2017 Cojen.org
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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.net.Socket;
import java.net.ServerSocket;

import org.cojen.tupl.ext.ReplicationManager;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class SocketReplicationManager implements ReplicationManager {
    private final String mReplicaHost;
    private final int mPort;

    private volatile InputStream mReader;
    private volatile StreamWriter mWriter;

    private long mPos;

    /**
     * @param replicaHost replica to connect to; pass null if local host is the replica
     * @param port replica port for connecting or listening
     */
    public SocketReplicationManager(String replicaHost, int port) {
        mReplicaHost = replicaHost;
        mPort = port;
    }

    @Override
    public long encoding() {
        return 2267011754526215480L;
    }

    @Override
    public void start(long position) throws IOException {
        mPos = position;
        if (mReplicaHost == null) {
            // Local host is the replica. Wait for leader to connect.
            ServerSocket ss = new ServerSocket(mPort);
            Socket s = ss.accept();
            mReader = s.getInputStream();
        } else {
            // Local host is the leader. Wait to connect to replica.
            Socket s = new Socket(mReplicaHost, mPort);
            mWriter = new StreamWriter(s.getOutputStream());
        }
    }

    @Override
    public void recover(EventListener listener) throws IOException {
    }

    @Override
    public long readPosition() {
        return mPos;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        InputStream in = mReader;
        return in == null ? -1 : in.read(b, off, len);
    }

    @Override
    public void flip() {
    }

    @Override
    public Writer writer() throws IOException {
        return mWriter;
    }

    @Override
    public void sync() throws IOException {
    }

    @Override
    public void syncConfirm(long position, long timeoutNanos) throws IOException {
    }

    @Override
    public void checkpointed(long position) {
    }

    @Override
    public void close() throws IOException {
        if (mReader != null) {
            mReader.close();
        }
        if (mWriter != null) {
            mWriter.mOut.close();
        }
    }

    public synchronized void waitForLeadership() throws InterruptedException {
        StreamWriter writer = mWriter;
        if (writer == null) {
            throw new IllegalStateException();
        }
        while (!writer.mNotified) {
            wait();
        }
    }

    private class StreamWriter implements Writer {
        private final OutputStream mOut;
        private boolean mNotified;

        StreamWriter(OutputStream out) throws IOException {
            mOut = out;
        }

        @Override
        public long position() {
            return mPos;
        }

        @Override
        public boolean leaderNotify(Runnable callback) {
            // Leadership is never lost, so no need to register the callback.
            mNotified = true;
            synchronized (SocketReplicationManager.this) {
                SocketReplicationManager.this.notify();
            }
            return true;
        }

        @Override
        public boolean write(byte[] b, int off, int len, long commitPos) {
            try {
                mOut.write(b, off, len);
                mPos += len;
                return true;
            } catch (IOException e) {
                return false;
            }
        }

        @Override
        public boolean confirm(long position, long timeoutNanos) {
            // Implicitly confirmed.
            return true;
        }
    }
}
