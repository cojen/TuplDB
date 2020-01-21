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
import java.io.InterruptedIOException;
import java.io.IOException;

import org.cojen.tupl.ConfirmationFailureException;
import org.cojen.tupl.ConfirmationInterruptedException;
import org.cojen.tupl.ConfirmationTimeoutException;
import org.cojen.tupl.UnmodifiableReplicaException;

import org.cojen.tupl.repl.ReplicatorConfig;
import org.cojen.tupl.repl.StreamReplicator;

/**
 * Adapts a StreamReplicator to be used by the database.
 *
 * @author Brian S O'Neill
 */
final class ReplManager implements Closeable {
    static ReplManager open(ReplicatorConfig config) throws IOException {
        return new ReplManager(StreamReplicator.open(config));
    }

    final StreamReplicator mRepl;

    private volatile StreamReplicator.Reader mStreamReader;
    private Writer mWriter;

    ReplManager(StreamReplicator repl) {
        mRepl = repl;
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
     * invokes this method, and so it blocks until recovery completes.
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
                ReplUtils.sendSnapshot(db, sender);
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
