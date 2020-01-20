/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.OutputStream;

import java.lang.reflect.Constructor;

import java.net.Socket;
import java.net.SocketAddress;

import java.util.HashMap;
import java.util.Map;

import java.util.function.Consumer;

import java.util.zip.CheckedOutputStream;
import java.util.zip.Checksum;
import java.util.zip.CRC32C;

import org.cojen.tupl.ConfirmationFailureException;
import org.cojen.tupl.ConfirmationInterruptedException;
import org.cojen.tupl.ConfirmationTimeoutException;
import org.cojen.tupl.Database;
import org.cojen.tupl.EventListener;
import org.cojen.tupl.EventType;
import org.cojen.tupl.Snapshot;
import org.cojen.tupl.UnmodifiableReplicaException;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.ext.ReplicationManager;

/**
 * ReplicationManager implementation backed by a StreamReplicator.
 *
 * @author Brian S O'Neill
 * @hidden
 */
public final class DatabaseReplicator implements ReplicationManager {
    public static DatabaseReplicator open(ReplicatorConfig config) throws IOException {
        return new DatabaseReplicator(StreamReplicator.open(config));
    }

    private static final long ENCODING = 7944834171105125288L;
    private static final long RESTORE_EVENT_RATE_MILLIS = 5000;

    private final StreamReplicator mRepl;

    private volatile StreamReplicator.Reader mStreamReader;
    private DbWriter mDbWriter;

    private DatabaseReplicator(StreamReplicator repl) {
        mRepl = repl;
    }

    public Role localRole() {
        return mRepl.localRole();
    }

    @Override
    public long encoding() {
        return ENCODING;
    }

    @Override
    public InputStream restoreRequest(EventListener listener) throws IOException {
        Map<String, String> options = new HashMap<>();
        options.put("checksum", "CRC32C");

        Constructor<?> lz4Input;
        try {
            Class<?> clazz = Class.forName("net.jpountz.lz4.LZ4FrameInputStream");
            lz4Input = clazz.getConstructor(InputStream.class);
            options.put("compress", "LZ4Frame");
        } catch (Throwable e) {
            lz4Input = null;
        }

        SnapshotReceiver receiver = mRepl.restore(options);

        if (receiver == null) {
            return null;
        }

        InputStream in;

        try {
            in = receiver.inputStream();
            options = receiver.options();
            long length = receiver.length();

            String compressOption = options.get("compress");

            if (compressOption != null) {
                if (compressOption.equals("LZ4Frame")) {
                    try {
                        in = (InputStream) lz4Input.newInstance(in);
                    } catch (Throwable e) {
                        throw new IOException("Unable to decompress", e);
                    }
                } else {
                    throw new IOException("Unknown compress option: " + compressOption);
                }
            }

            String checksumOption = options.get("checksum");

            if (checksumOption != null) {
                if (checksumOption.equals("CRC32C")) {
                    in = new CheckedInputStream(in, new CRC32C(), length);
                } else {
                    throw new IOException("Unknown checksum option: " + checksumOption);
                }
            }

            if (listener != null && length >= 0 && (mRepl instanceof Controller)) {
                Scheduler scheduler = ((Controller) mRepl).scheduler();
                var rin = new RestoreInputStream(in);
                in = rin;

                listener.notify(EventType.REPLICATION_RESTORE,
                                "Receiving snapshot: %1$,d bytes from %2$s",
                                length, receiver.senderAddress());

                scheduler.schedule(new ProgressTask(listener, scheduler, rin, length));
            }
        } catch (Throwable e) {
            Utils.closeQuietly(receiver);
            throw e;
        }

        return in;
    }

    private static final class ProgressTask extends Delayed {
        private final EventListener mListener;
        private final RestoreInputStream mRestore;
        private final long mLength;
        private final Scheduler mScheduler;

        private long mLastTimeMillis = Long.MIN_VALUE;
        private long mLastReceived;

        ProgressTask(EventListener listener, Scheduler scheduler,
                     RestoreInputStream in, long length)
        {
            super(0);
            mListener = listener;
            mScheduler = scheduler;
            mRestore = in;
            mLength = length;
        }

        @Override
        protected void doRun(long counter) {
            if (mRestore.isFinished()) {
                return;
            }

            long now = System.currentTimeMillis();

            long received = mRestore.received();
            double percent = 100.0 * (received / (double) mLength);
            long progess = received - mLastReceived;

            if (mLastTimeMillis != Long.MIN_VALUE) {
                double rate = (1000.0 * (progess / (double) (now - mLastTimeMillis)));
                String format = "Receiving snapshot: %1$1.3f%%";
                if (rate == 0) {
                    mListener.notify(EventType.REPLICATION_RESTORE, format, percent);
                } else {
                    format += "  rate: %2$,d bytes/s  remaining: ~%3$s";
                    long remainingSeconds = (long) ((mLength - received) / rate);
                    mListener.notify
                        (EventType.REPLICATION_RESTORE, format,
                         percent, (long) rate, remainingDuration(remainingSeconds));
                }
            }

            mCounter = now + RESTORE_EVENT_RATE_MILLIS;

            mLastTimeMillis = now;
            mLastReceived = received;

            mScheduler.schedule(this);
        }

        private static String remainingDuration(long seconds) {
            if (seconds < (60 * 2)) {
                return seconds + "s";
            } else if (seconds < (60 * 60 * 2)) {
                return (seconds / 60) + "m";
            } else if (seconds < (60 * 60 * 24 * 2)) {
                return (seconds / (60 * 60)) + "h";
            } else {
                return (seconds / (60 * 60 * 24)) + "d";
            }
        }
    }

    @Override
    public boolean isReadable(long position) {
        return mRepl.isReadable(position);
    }

    @Override
    public void start(long position) throws IOException {
        if (mStreamReader != null) {
            throw new IllegalStateException();
        }

        mRepl.start();

        while (true) {
            StreamReplicator.Reader reader = mRepl.newReader(position, false);
            if (reader != null) {
                mStreamReader = reader;
                break;
            }
            StreamReplicator.Writer writer = mRepl.newWriter(position);
            if (writer != null) {
                mDbWriter = new DbWriter(writer);
                break;
            }
        }
    }

    @Override
    public boolean ready(ReplicationManager.Accessor accessor) throws IOException {
        // Can now send control messages.
        mRepl.controlMessageAcceptor(message -> {
            try {
                accessor.control(message);
            } catch (UnmodifiableReplicaException e) {
                // Drop it.
            } catch (IOException e) {
                Utils.uncaught(e);
            }
        });

        Database db = accessor.database();

        // Can now accept snapshot requests.
        mRepl.snapshotRequestAcceptor(sender -> {
            try {
                sendSnapshot(db, sender);
            } catch (IOException e) {
                Utils.closeQuietly(sender);
            }
        });

        // Update the local member role.
        mRepl.start();

        // Wait until caught up.
        return catchup();
    }

    /**
     * Wait until local member becomes the leader or until the current term has reached a
     * known commit position.
     *
     * @return true if switched to leader
     */
    private boolean catchup() {
        StreamReplicator.Reader reader = mStreamReader;

        while (true) {
            // If reader is null, then local member is the leader and has implicitly caught up.
            if (reader == null) {
                return true;
            }

            long commitPosition = reader.commitPosition();
            long delayMillis = 1;

            while (true) {
                // Check if the term changed.
                StreamReplicator.Reader currentReader = mStreamReader;
                if (currentReader != reader) {
                    reader = currentReader;
                    break;
                }

                if (reader.position() >= commitPosition) {
                    return false;
                }

                // Delay and double each time, up to 100 millis. Crude, but it means that no
                // special checks and notification logic needs to be added to the reader.
                try {
                    Thread.sleep(delayMillis);
                } catch (InterruptedException e) {
                    return false;
                }

                delayMillis = Math.min(delayMillis << 1, 100);
            }
        }
    }

    private void sendSnapshot(Database db, SnapshotSender sender) throws IOException {
        Map<String, String> requestedOptions = sender.options();

        var options = new HashMap<String, String>();

        Checksum checksum = null;
        if ("CRC32C".equals(requestedOptions.get("checksum"))) {
            options.put("checksum", "CRC32C");
            checksum = new CRC32C();
        }

        Snapshot snapshot = db.beginSnapshot();

        Constructor lz4Output = null;
        if (snapshot.isCompressible() && "LZ4Frame".equals(requestedOptions.get("compress"))) {
            try {
                Class<?> clazz = Class.forName("net.jpountz.lz4.LZ4FrameOutputStream");
                lz4Output = clazz.getConstructor(OutputStream.class);
                options.put("compress", "LZ4Frame");
            } catch (Throwable e) {
                // Not supported.
            }
        }

        OutputStream out = sender.begin(snapshot.length(), snapshot.position(), options);
        try {
            if (lz4Output != null) {
                try {
                    out = (OutputStream) lz4Output.newInstance(out);
                } catch (Throwable e) {
                    throw new IOException("Unable to compress", e);
                }
            }

            CheckedOutputStream cout = null;
            if (checksum != null) {
                out = cout = new CheckedOutputStream(out, checksum);
            }

            snapshot.writeTo(out);

            if (cout != null) {
                var buf = new byte[4];
                Utils.encodeIntLE(buf, 0, (int) checksum.getValue());
                out.write(buf);
            }
        } finally {
            out.close();
        }
    }

    @Override
    public long readPosition() {
        StreamReplicator.Reader reader = mStreamReader;
        if (reader != null) {
            return reader.position();
        } else {
            // Might start off as the leader, so return it's start position. Nothing is
            // actually readable, however.
            return mDbWriter.mWriter.termStartPosition();
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        StreamReplicator.Reader reader = mStreamReader;

        if (reader == null) {
            return -1;
        }

        while (true) {
            int amt = reader.read(b, off, len);
            if (amt >= 0) {
                return amt;
            }

            // Term ended.

            StreamReplicator.Reader nextReader;
            while ((nextReader = mRepl.newReader(reader.position(), false)) == null) {
                StreamReplicator.Writer nextWriter = mRepl.newWriter(reader.position());
                if (nextWriter != null) {
                    // Switch to leader mode.
                    mDbWriter = new DbWriter(nextWriter);
                    mStreamReader = null;
                    reader.close();
                    return -1;
                }
            }

            reader.close();
            mStreamReader = reader = nextReader;
        }
    }

    @Override
    public ReplicationManager.Writer writer() throws IOException {
        return mDbWriter;
    }

    @Override
    public void sync() throws IOException {
        mRepl.sync();
    }

    @Override
    public void syncConfirm(long position, long timeoutNanos) throws IOException {
        if (!mRepl.syncCommit(position, timeoutNanos)) {
            throw new ConfirmationTimeoutException(timeoutNanos);
        }
    }

    @Override
    public boolean failover() throws IOException {
        return mRepl.failover();
    }

    @Override
    public void checkpointed(long position) throws IOException {
        mRepl.compact(position);
    }

    @Override
    public void control(long position, byte[] message) throws IOException {
        mRepl.controlMessageReceived(position, message);
    }

    @Override
    public void close() throws IOException {
        mRepl.close();
    }

    /**
     * Don't close the ServerSocket when closing this Replicator.
     */
    void keepServerSocket() {
        ((Controller) mRepl).keepServerSocket();
    }

    /**
     * Enable or disable partitioned mode, which simulates a network partition. New connections
     * are rejected and existing connections are closed.
     */
    void partitioned(boolean enable) {
        ((Controller) mRepl).partitioned(enable);
    }

    void toReplica(DbWriter expect, long position) {
        if (mDbWriter != expect) {
            throw new IllegalStateException("Mismatched writer: " + mDbWriter + " != " + expect);
        }

        mDbWriter.mWriter.close();
        mDbWriter = null;

        while ((mStreamReader = mRepl.newReader(position, false)) == null) {
            StreamReplicator.Writer nextWriter = mRepl.newWriter(position);
            if (nextWriter != null) {
                // Actually the leader now.
                mDbWriter = new DbWriter(nextWriter);
                return;
            }
        }
    }

    private final class DbWriter implements ReplicationManager.Writer {
        final StreamReplicator.Writer mWriter;

        private boolean mEndConfirmed;

        DbWriter(StreamReplicator.Writer writer) {
            mWriter = writer;
        }

        @Override
        public long position() {
            return mWriter.position();
        }

        @Override
        public long confirmedPosition() {
            return mWriter.commitPosition();
        }

        @Override
        public boolean leaderNotify(Runnable callback) {
            mWriter.uponCommit(Long.MAX_VALUE, position -> new Thread(callback).start());
            return true;
        }

        @Override
        public boolean write(byte[] b, int off, int len, long commitPos) throws IOException {
            return mWriter.write(b, off, len, commitPos);
        }

        @Override
        public boolean confirm(long commitPos, long nanosTimeout) throws IOException {
            long pos;
            try {
                pos = mWriter.waitForCommit(commitPos, nanosTimeout);
            } catch (InterruptedIOException e) {
                throw new ConfirmationInterruptedException();
            }
            if (pos >= commitPos) {
                return true;
            }
            if (pos == -1) {
                return false;
            }
            if (pos == -2) {
                throw new ConfirmationTimeoutException(nanosTimeout);
            }
            throw new ConfirmationFailureException("Unexpected result: " + pos);
        }

        @Override
        public long confirmEnd(long nanosTimeout) throws ConfirmationFailureException {
            long pos;
            try {
                pos = mWriter.waitForEndCommit(nanosTimeout);
            } catch (InterruptedIOException e) {
                throw new ConfirmationInterruptedException();
            }
            if (pos >= 0) {
                synchronized (this) {
                    if (mEndConfirmed) {
                        // Don't call toReplica again.
                        return pos;
                    }
                    mEndConfirmed = true;
                }
                toReplica(this, pos);
                return pos;
            }
            if (pos == -1) {
                throw new ConfirmationFailureException("Closed");
            }
            if (pos == -2) {
                throw new ConfirmationTimeoutException(nanosTimeout);
            }
            throw new ConfirmationFailureException("Unexpected result: " + pos);
        }
    }
}
