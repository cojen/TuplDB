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
    private final ServerSocket mServerSocket;
    private final String mReplicaHost;
    private final int mPort;

    private volatile InputStream mReader;
    private volatile StreamWriter mWriter;

    private volatile long mPos;

    private long mFencedPos;

    /**
     * @param replicaHost replica to connect to; pass null if local host is the replica
     * @param port replica port for connecting or listening
     */
    public SocketReplicationManager(String replicaHost, int port) throws IOException {
        if (replicaHost != null) {
            mServerSocket = null;
        } else {
            mServerSocket = new ServerSocket(port);
            port = mServerSocket.getLocalPort();
        }

        mReplicaHost = replicaHost;
        mPort = port;
    }

    int getPort() {
        return mPort;
    }

    @Override
    public long encoding() {
        return 2267011754526215480L;
    }

    @Override
    public void start(long position) throws IOException {
        mPos = position;
        if (mServerSocket != null) {
            // Local host is the replica. Wait for leader to connect.
            Socket s = mServerSocket.accept();
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
        if (in == null) {
            return -1;
        }
        int amt = in.read(b, off, len);
        if (amt > 0) {
            mPos += amt;
        }
        return amt;
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
    public synchronized void fenced(long position) throws IOException {
        mFencedPos = position;
        notifyAll();
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

    public void disableWrites() {
        mWriter.mDisabled = true;
    }

    public synchronized long waitForFence(long position) throws InterruptedException {
        while (true) {
            long current = mFencedPos;
            if (current >= position) {
                return current;
            }
            wait();
        }
    }

    private class StreamWriter implements Writer {
        private final OutputStream mOut;
        private boolean mNotified;
        private volatile boolean mDisabled;

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
                SocketReplicationManager.this.notifyAll();
            }
            return true;
        }

        @Override
        public boolean write(byte[] b, int off, int len, long commitPos) {
            try {
                if (!mDisabled) {
                    mOut.write(b, off, len);
                    mPos += len;
                }
                return true;
            } catch (IOException e) {
                return false;
            }
        }

        @Override
        public boolean confirm(long position, long timeoutNanos)
            throws ConfirmationFailureException
        {
            if (mDisabled) {
                throw new ConfirmationFailureException();
            }
            return true;
        }
    }
}
