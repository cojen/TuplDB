/*
 *  Copyright 2020 Cojen.org
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
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.OutputStream;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.Runner;

import org.cojen.tupl.repl.StreamReplicator;
import org.cojen.tupl.repl.SnapshotReceiver;
import org.cojen.tupl.repl.SnapshotSender;
import org.cojen.tupl.repl.Role;

/**
 * Just writes over a local socket, for testing.
 *
 * @author Brian S O'Neill
 */
public class SocketReplicator implements StreamReplicator {
    private final ServerSocket mServerSocket;
    private final String mReplicaHost;
    private final int mReplicaPort;

    private volatile InetSocketAddress mLocalAddress;

    volatile InputStream mInput;
    volatile OutputStream mOutput;

    private volatile Consumer<byte[]> mControlMessageAcceptor;

    private volatile long mInitPos;

    private long mPos;

    private byte[] mControlMessage;

    private ArrayList<LongConsumer> mListeners;
    private long mLastListenPos;

    private TreeMap<Long, LongConsumer> mTasks;

    private volatile boolean mSuspendCommit;

    private final Set<Thread> mSuspended = new HashSet<>();
    private long mSuspendPos;

    private final Set<Thread> mWaiting = new HashSet<>();

    private boolean mClosed;

    /**
     * @param replicaHost replica to connect to; pass null if local host is the replica
     * @param port replica port for connecting or listening
     */
    public SocketReplicator(String replicaHost, int port) throws IOException {
        if (replicaHost == null) {
            // Local host is the replica.
            mServerSocket = new ServerSocket(port);
            // FIXME: On MacOS, sometimes not bound.
            mLocalAddress = (InetSocketAddress) mServerSocket.getLocalSocketAddress();
        } else {
            // Local host is the leader.
            mServerSocket = null;
        }

        mReplicaHost = replicaHost;
        mReplicaPort = port;
    }

    protected SocketReplicator() {
        mServerSocket = null;
        mReplicaHost = null;
        mReplicaPort = 0;
    }

    @Override
    public long encoding() {
        return 8675309;
    }

    @Override
    public void start() throws IOException {
        if (mInput != null || mOutput != null) {
            // Already started.
            return;
        }

        if (mServerSocket != null) {
            // Local host is the replica. Wait for leader to connect.
            mServerSocket.setSoTimeout(60);
            Socket s = mServerSocket.accept();
            mLocalAddress = (InetSocketAddress) s.getLocalSocketAddress();
            mInput = s.getInputStream();
        } else {
            // Local host is the leader. Wait to connect to replica.
            mOutput = new Socket(mReplicaHost, mReplicaPort).getOutputStream();
        }
    }

    @Override
    public SnapshotReceiver restore(Map<String, String> options) throws IOException {
        return null;
    }

    @Override
    public SnapshotReceiver requestSnapshot(Map<String, String> options) throws IOException {
        return null;
    }

    @Override
    public void snapshotRequestAcceptor(Consumer<SnapshotSender> acceptor) {
    }

    @Override
    public boolean isReadable(long position) {
        return mInitPos <= position;
    }

    @Override
    public synchronized Reader newReader(long position, boolean follow) {
        if (mInput == null) {
            if (mOutput == null || follow) {
                throw new IllegalStateException();
            }
            return null;
        }

        mPos = positionCheck(position);

        return new Reader();
    }

    protected long positionCheck(long position) {
        if (position != mPos) {
            throw new IllegalStateException("position: " + position + " != " + mPos);
        }
        return mPos;
    }

    @Override
    public synchronized Writer newWriter() {
        return newWriter(mPos);
    }

    @Override
    public synchronized Writer newWriter(long position) {
        mPos = positionCheck(position);

        return mOutput == null ? null : new Writer();
    }

    @Override
    public boolean syncCommit(long position, long nanosTimeout) throws IOException {
        return true;
    }

    @Override
    public void compact(long position) throws IOException {
    }

    @Override
    public long localMemberId() {
        return 0;
    }

    @Override
    public SocketAddress localAddress() {
        return mLocalAddress;
    }

    @Override
    public Role localRole() {
        return mServerSocket == null ? Role.OBSERVER : Role.NORMAL;
    }

    @Override
    public Socket connect(SocketAddress addr) throws IOException {
        return null;
    }

    @Override
    public void socketAcceptor(Consumer<Socket> acceptor) {
    }

    @Override
    public void sync() throws IOException {
    }

    @Override
    public boolean failover() throws IOException {
        return mInput != null;
    }

    @Override
    public synchronized void controlMessageReceived(long position, byte[] message) {
        mControlMessage = message;
        notifyAll();
    }

    @Override
    public void controlMessageAcceptor(Consumer<byte[]> acceptor) {
        mControlMessageAcceptor = acceptor;
    }

    @Override
    public synchronized void close() throws IOException {
        mClosed = true;

        if (mInput != null) {
            mInput.close();
        }

        if (mOutput != null) {
            mOutput.close();
        }

        mSuspended.clear();

        notifyAll();
    }

    public void setInitialPosition(long position) {
        mInitPos = position;
    }

    public int getPort() {
        return mLocalAddress.getPort();
    }

    public void writeControl(byte[] message) throws IOException {
        mControlMessageAcceptor.accept(message);
    }

    public synchronized void waitForControl(byte[] message) throws InterruptedException {
        while (!Arrays.equals(mControlMessage, message)) {
            wait();
        }
    }

    public synchronized void suspendCommit(boolean b) {
        mSuspendCommit = b;
        notifyAll();

        if (!b && mListeners != null && mPos > mLastListenPos) {
            for (LongConsumer listener : mListeners) {
                listener.accept(mPos);
            }
            mLastListenPos = mPos;
        }
    }

    /**
     * Note: Only suspends waitForCommit, but not uponCommit.
     *
     * @param t suspend for this thread
     */
    public synchronized void suspendCommit(Thread t) {
        mSuspendPos = 0;
        if (t == null) {
            mSuspended.clear();
            notifyAll();
        } else {
            mSuspended.add(t);
        }
    }

    /**
     * Note: Only suspends waitForCommit, but not uponCommit.
     *
     * @param t suspend for this thread
     * @param position waitForCommit position to rollback to
     */
    public synchronized void suspendCommit(Thread t, long position) {
        if (t == null) {
            throw new IllegalArgumentException();
        }
        mSuspendPos = position;
        mSuspended.add(t);
        mPos = position;
    }

    public <T extends Thread> T startAndWaitUntilSuspended(T t) {
        t = TestUtils.startAndWaitUntilBlocked(t);
        while (true) {
            if (isWaiting(t)) {
                return t;
            }
            Thread.yield();
        }
    }

    public synchronized void disableWrites() throws IOException {
        // Create a dummy replica stream that simply blocks until closed.
        mInput = new InputStream() {
            private boolean mClosed;

            @Override
            public synchronized int read() throws IOException {
                while (!mClosed) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        throw new InterruptedIOException();
                    }
                }
                throw new IOException("Closed");
            }

            @Override
            public synchronized void close() {
                mClosed = true;
                notify();
            }
        };

        mOutput.close();
        mOutput = null;

        if (mTasks != null) {
            for (LongConsumer task : mTasks.values()) {
                try {
                    task.accept(-1);
                } catch (Throwable ex) {
                    Utils.uncaught(ex);
                }
            }
            mTasks = null;
        }

        if (mListeners != null) {
            for (LongConsumer listener : mListeners) {
                listener.accept(-1);
            }
        }
    }

    public synchronized long position() {
        return mPos;
    }

    public synchronized long commitPosition() {
        return mSuspendPos == 0 ? mPos : mSuspendPos;
    }

    private synchronized void addCommitListener(LongConsumer listener) {
        if (mListeners == null) {
            mListeners = new ArrayList<>();
        }
        mListeners.add(listener);
    }

    private void uponCommit(long position, LongConsumer task) {
        if (mOutput == null) {
            task.accept(-1);
            return;
        }

        long actual;
        synchronized (this) {
            actual = mPos;
            if (actual < position) {
                if (mTasks == null) {
                    mTasks = new TreeMap<>();
                }
                mTasks.put(position, task);
                return;
            }
        }

        reached(task, actual);
    }

    private void reached(LongConsumer task, long position) {
        if (!mSuspendCommit) {
            task.accept(position);
            return;
        }

        Runner.start(() -> {
            synchronized (SocketReplicator.this) {
                while (mSuspendCommit) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
            }

            task.accept(position);
        });
    }

    private synchronized boolean isWaiting(Thread t) {
        return mWaiting.contains(t);
    }

    private synchronized long waitForCommit(Accessor accessor, long position, long nanosTimeout)
        throws InterruptedIOException
    {
        try {
            try {
                while (mSuspendCommit || mSuspended.contains(Thread.currentThread())) {
                    if (accessor.isClosed()) {
                        return -1;
                    }
                    mWaiting.add(Thread.currentThread());
                    wait();
                }
            } finally {
                mWaiting.remove(Thread.currentThread());
            }

            long commitPos;
            while (position > (commitPos = commitPosition())) {
                if (accessor.isClosed()) {
                    return -1;
                }

                if (nanosTimeout <= 0) {
                    if (nanosTimeout == 0) {
                        return -2;
                    }
                    wait();
                } else {
                    long millis = nanosTimeout / 1_000_000;
                    long start = System.nanoTime();
                    if (millis == 0) {
                        Thread.yield();
                    } else {
                        wait(millis);
                    }
                    nanosTimeout = Math.max(0, nanosTimeout - (System.nanoTime() - start));
                }
            }

            return commitPos;
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    private synchronized void addToPosition(long delta) {
        setPosition(mPos + delta);
    }

    public synchronized void setPosition(long position) {
        mPos = position;
        notifyAll();

        if (mListeners != null && position > mLastListenPos && !mSuspendCommit) {
            for (LongConsumer listener : mListeners) {
                listener.accept(position);
            }
            mLastListenPos = position;
        }

        if (mTasks != null) {
            Iterator<Map.Entry<Long, LongConsumer>> it = mTasks.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<Long, LongConsumer> e = it.next();
                if (e.getKey() > position) {
                    break;
                }
                it.remove();
                try {
                    reached(e.getValue(), position);
                } catch (Throwable ex) {
                    Utils.uncaught(ex);
                }
            }
            if (mTasks.isEmpty()) {
                mTasks = null;
            }
        }
    }

    private abstract class Accessor implements StreamReplicator.Accessor {
        private volatile boolean mClosed;

        @Override
        public long term() {
            return 0;
        }

        @Override
        public long termStartPosition() {
            return 0;
        }

        @Override
        public long termEndPosition() {
            return isClosed() ? position() : Long.MAX_VALUE;
        }

        @Override
        public long position() {
            return SocketReplicator.this.position();
        }

        @Override
        public long commitPosition() {
            return SocketReplicator.this.commitPosition();
        }

        @Override
        public void addCommitListener(LongConsumer listener) {
            SocketReplicator.this.addCommitListener(listener);
        }

        @Override
        public void uponCommit(long position, LongConsumer task) {
            SocketReplicator.this.uponCommit(position, task);
        }

        @Override
        public long waitForCommit(long position, long nanosTimeout) throws InterruptedIOException {
            return SocketReplicator.this.waitForCommit(this, position, nanosTimeout);
        }

        @Override
        public void close() {
            mClosed = true;
            synchronized (SocketReplicator.this) {
                SocketReplicator.this.notifyAll();
            }
        }

        public boolean isClosed() {
            if (SocketReplicator.this.mClosed) {
                close();
            }
            return mClosed;
        }
    }

    private class Reader extends Accessor implements StreamReplicator.Reader {
        @Override
        public int read(byte[] buf, int offset, int length) throws IOException {
            if (isClosed()) {
                throw new IOException("Closed");
            }

            InputStream in = mInput;

            if (in == null) {
                // Not explicitly closed.
                return -1;
            }

            int amt = in.read(buf, offset, length);

            if (amt > 0) {
                addToPosition(amt);
            }

            return amt;
        }

        @Override
        public int tryRead(byte[] buf, int offset, int length) throws IOException {
            // Blocking, but this method isn't expected to be called.
            return read(buf, offset, length);
        }
    }

    private class Writer extends Accessor implements StreamReplicator.Writer {
        @Override
        public int write(byte[] prefix, byte[] messages, int offset, int length,
                         long highestPosition)
            throws IOException
        {
            if (isClosed()) {
                return -1;
            }

            OutputStream out = mOutput;

            if (out == null) {
                close();
                return -1;
            }

            if (prefix != null) {
                doWrite(out, prefix, 0, prefix.length);
            }

            doWrite(out, messages, offset, length);

            return 1;
        }

        private void doWrite(OutputStream out, byte[] b, int off, int len) throws IOException {
            out.write(b, off, len);
            addToPosition(len);
        }

        @Override
        public boolean isClosed() {
            if (mOutput == null) {
                close();
            }
            return super.isClosed();
        }
    }
}
