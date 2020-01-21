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

package org.cojen.tupl.core;

import java.io.Closeable;
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

import org.cojen.tupl.repl.ReplicatorConfig;
import org.cojen.tupl.repl.Role;
import org.cojen.tupl.repl.SnapshotReceiver;
import org.cojen.tupl.repl.SnapshotSender;
import org.cojen.tupl.repl.StreamReplicator;

import org.cojen.tupl.io.Utils;

/**
 * Adapts a StreamReplicator to be used by the database.
 *
 * @author Brian S O'Neill
 */
final class ReplManager implements Closeable {
    static ReplManager open(ReplicatorConfig config) throws IOException {
        return new ReplManager(StreamReplicator.open(config));
    }

    private static final long RESTORE_EVENT_RATE_MILLIS = 5000;

    final StreamReplicator mRepl;

    private volatile StreamReplicator.Reader mStreamReader;
    private Writer mWriter;

    ReplManager(StreamReplicator repl) {
        mRepl = repl;
    }

    /**
     * Called when the database is created, in an attempt to retrieve an existing database
     * snapshot from a replication peer. If null is returned, the database will try to start
     * reading replication data at the lowest position.
     *
     * @param listener optional restore event listener
     * @return null if no snapshot could be found
     * @throws IOException if a snapshot was found, but requesting it failed
     */
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

            if (listener != null && length >= 0) {
                var rin = new RestoreInputStream(in);
                in = rin;

                listener.notify(EventType.REPLICATION_RESTORE,
                                "Receiving snapshot: %1$,d bytes from %2$s",
                                length, receiver.senderAddress());

                new Progress(listener, rin, length).start();
            }
        } catch (Throwable e) {
            Utils.closeQuietly(receiver);
            throw e;
        }

        return in;
    }

    private static final class Progress extends Thread {
        private final EventListener mListener;
        private final RestoreInputStream mRestore;
        private final long mLength;

        private long mLastTimeMillis = Long.MIN_VALUE;
        private long mLastReceived;

        Progress(EventListener listener, RestoreInputStream in, long length) {
            mListener = listener;
            mRestore = in;
            mLength = length;
            setDaemon(true);
        }

        @Override
        public void run() {
            while (!mRestore.isFinished()) {
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

                mLastTimeMillis = now;
                mLastReceived = received;

                try {
                    Thread.sleep(RESTORE_EVENT_RATE_MILLIS);
                } catch (InterruptedException e) {
                    break;
                }
            }
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

    /**
     * Start the replication manager in replica mode. Invocation of this method implies that
     * all data lower than the given position is confirmed. All data at or higher than the
     * given position might be discarded.
     *
     * <p>After started, the reported {@linkplain #readPosition position} can differ from the
     * one provided to this method.
     *
     * @param position position to start reading from; 0 is the lowest position
     * @throws IllegalArgumentException if position is negative
     * @throws IllegalStateException if already started
     */
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
                mWriter = new Writer(writer);
                break;
            }
        }
    }

    /**
     * Called after replication threads have started, providing an opportunity to wait until
     * replication has sufficiently "caught up". The thread which is opening the database
     * invokes this method, and so it blocks until recovery completes. Default implementation
     * does nothing and returns false.
     *
     * @return true if local member is expected to become the leader (implies that a thread
     * calling read has or will have -1 returned to it)
     */
    public boolean ready(CoreDatabase db) throws IOException {
        // Can now send control messages.
        mRepl.controlMessageAcceptor(message -> {
            try {
                db.writeControlMessage(message);
            } catch (UnmodifiableReplicaException e) {
                // Drop it.
            } catch (IOException e) {
                Utils.uncaught(e);
            }
        });

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

    /**
     * Returns the next position a replica will read from, which must be confirmed. Position is
     * never negative and never retreats.
     */
    public long readPosition() {
        StreamReplicator.Reader reader = mStreamReader;
        if (reader != null) {
            return reader.position();
        } else {
            // Might start off as the leader, so return it's start position. Nothing is
            // actually readable, however.
            return mWriter.mWriter.termStartPosition();
        }
    }

    /**
     * Blocks at most once, reading as much replication input as possible. Returns -1 if local
     * instance has become the leader, and a writer instance now exists.
     *
     * @return amount read, or -1 if leader
     * @throws IllegalStateException if not started
     */
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
                    mWriter = new Writer(nextWriter);
                    mStreamReader = null;
                    reader.close();
                    return -1;
                }
            }

            reader.close();
            mStreamReader = reader = nextReader;
        }
    }

    /**
     * Returns an object which allows the leader to write changes. A new instance is required
     * after a leader transition. Returned object can be null if local instance is a replica.
     */
    public Writer writer() throws IOException {
        return mWriter;
    }

    /**
     * Durably flushes all local data to non-volatile storage, up to the given confirmed
     * position, and then blocks until fully confirmed.
     *
     * @throws ConfirmationFailureException
     */
    public void syncConfirm(long position) throws IOException {
        if (!mRepl.syncCommit(position, -1)) {
            // Unexpected.
            throw new ConfirmationTimeoutException(-1);
        }
    }

    public void close() throws IOException {
        mRepl.close();
    }

    private void toReplica(Writer expect, long position) {
        if (mWriter != expect) {
            throw new IllegalStateException("Mismatched writer: " + mWriter + " != " + expect);
        }

        mWriter.mWriter.close();
        mWriter = null;

        while ((mStreamReader = mRepl.newReader(position, false)) == null) {
            StreamReplicator.Writer nextWriter = mRepl.newWriter(position);
            if (nextWriter != null) {
                // Actually the leader now.
                mWriter = new Writer(nextWriter);
                return;
            }
        }
    }

    public final class Writer {
        final StreamReplicator.Writer mWriter;

        private boolean mEndConfirmed;

        Writer(StreamReplicator.Writer writer) {
            mWriter = writer;
        }

        /**
         * Returns the next position a leader will write to. Valid only if local instance is
         * the leader.
         */
        public long position() {
            return mWriter.position();
        }

        /**
         * Returns the current confirmed log position.
         */
        public long confirmedPosition() {
            return mWriter.commitPosition();
        }

        /**
         * Fully writes the given data, unless leadership is revoked. When the local instance
         * loses leadership, all data rolls back to the highest confirmed position.
         *
         * <p>An optional commit parameter defines the highest log position which immediately
         * follows a transaction commit operation. If leadership is lost, the message stream is
         * guaranteed to be truncated at a position no higher than the highest commit position
         * ever provided. The given commit position is ignored if it's higher than what has
         * actually been written.
         *
         * @param b message buffer
         * @param off message buffer offset
         * @param len message length
         * @param commitPos highest transaction commit position; pass 0 if nothing changed
         * @return false only if the writer is deactivated
         * @throws IllegalArgumentException if commitPos is negative
         */
        public boolean write(byte[] b, int off, int len, long commitPos) throws IOException {
            return mWriter.write(b, off, len, commitPos);
        }

        /**
         * Blocks until all data up to the given log position is confirmed.
         *
         * @param commitPos commit position which was passed to the write method
         * @return false if not leader at the given position
         * @throws ConfirmationFailureException
         */
        public boolean confirm(long commitPos) throws IOException {
            long pos;
            try {
                pos = mWriter.waitForCommit(commitPos, -1);
            } catch (InterruptedIOException e) {
                throw new ConfirmationInterruptedException();
            }
            if (pos >= commitPos) {
                return true;
            }
            if (pos == -1) {
                return false;
            }
            throw unexpected(pos);
        }

        /**
         * Blocks until the leadership end is confirmed. This method must be called before
         * switching to replica mode.
         *
         * @return the end commit position; same as next read position
         * @throws ConfirmationFailureException if end position cannot be determined
         */
        public long confirmEnd() throws ConfirmationFailureException {
            long pos;
            try {
                pos = mWriter.waitForEndCommit(-1);
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
            throw unexpected(pos);
        }

        private ConfirmationFailureException unexpected(long pos) {
            if (pos == -2) {
                return new ConfirmationTimeoutException(-1);
            }
            return new ConfirmationFailureException("Unexpected result: " + pos);
        }
    }
}
