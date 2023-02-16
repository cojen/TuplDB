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

import java.io.IOException;

import org.cojen.tupl.repl.StreamReplicator;

import org.cojen.tupl.util.Latch;

/**
 * RedoDecoder used by {@link ReplEngine}, the replication system.
 *
 * @author Brian S O'Neill
 */
final class ReplDecoder extends RedoDecoder {
    volatile boolean mDeactivated;

    ReplDecoder(StreamReplicator repl, long initialPosition, long initialTxnId, Latch decodeLatch) {
        super(false, initialTxnId, new In(initialPosition, repl), decodeLatch);
    }

    @Override
    boolean verifyTerminator(DataIn in) {
        // No terminators to verify.
        return true;
    }

    /**
     * Wait until local member becomes the leader or until the current term has reached a
     * known commit position.
     *
     * @return true if switched to leader
     */
    boolean catchup() {
        return ((In) mIn).catchup(mDecodeLatch);
    }

    /**
     * Return the new leader Writer instance created after decoding stopped.
     */
    StreamReplicator.Writer extractWriter() {
        In in = (In) mIn;
        StreamReplicator.Writer writer = in.mWriter;
        in.mWriter = null;
        return writer;
    }

    static final class In extends DataIn {
        private final StreamReplicator mRepl;
        private volatile StreamReplicator.Reader mReader;
        private volatile StreamReplicator.Writer mWriter;

        In(long position, StreamReplicator repl) {
            this(position, repl, 64 << 10);
        }

        In(long position, StreamReplicator repl, int bufferSize) {
            super(position, bufferSize);
            mRepl = repl;
            mReader = mRepl.newReader(position, false);
        }

        @Override
        int doRead(byte[] buf, int off, int len) throws IOException {
            StreamReplicator.Reader reader = mReader;

            while (true) {
                if (reader != null) {
                    int amt = reader.read(buf, off, len);
                    if (amt >= 0) {
                        return amt;
                    }
                    reader.close();
                }

                while ((reader = mRepl.newReader(mPos, false)) == null) {
                    if ((mWriter = mRepl.newWriter()) != null) {
                        mReader = null;
                        return -1;
                    }
                    Thread.yield();
                }

                mReader = reader;
            }
        }

        boolean catchup(Latch decodeLatch) {
            StreamReplicator.Reader reader = mReader;

            boolean bumped = false;

            while (true) {
                // If null, then local member is the leader and has implicitly caught up.
                if (reader == null) {
                    return true;
                }

                long commitPosition = reader.commitPosition();
                long delayMillis = 1;

                while (true) {
                    // Check if the term changed.
                    StreamReplicator.Reader currentReader = mReader;
                    if (currentReader != reader) {
                        reader = currentReader;
                        break;
                    }

                    long readerPos = reader.position();
                    if (readerPos >= commitPosition && readerPos < reader.termEndPosition()) {
                        // Check if decode thread is caught up.
                        decodeLatch.acquireShared();
                        long decodePos = mPos;
                        decodeLatch.releaseShared();

                        while (decodePos >= commitPosition) {
                            if (!bumped) {
                                // The commit position may have advanced in the meantime, so
                                // check against the new position. Only do this once to prevent
                                // never returning.
                                commitPosition = reader.commitPosition();
                                bumped = true;
                                continue;
                            }

                            // Caught up enough.
                            return false;
                        }

                        // Keep waiting.
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

        @Override
        public void close() {
            if (mReader != null) {
                mReader.close();
                mReader = null;
            }
        }
    }
}
