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

import java.io.InterruptedIOException;
import java.io.IOException;

import java.net.Socket;
import java.net.SocketAddress;

import java.util.function.Consumer;

import org.cojen.tupl.ConfirmationFailureException;
import org.cojen.tupl.ConfirmationInterruptedException;
import org.cojen.tupl.ConfirmationTimeoutException;
import org.cojen.tupl.Database;
import org.cojen.tupl.EventListener;

import org.cojen.tupl.io.Utils;

/**
 * DatabaseReplicator implementation backed by a StreamReplicator.
 *
 * @author Brian S O'Neill
 */
final class DatabaseStreamReplicator implements DatabaseReplicator {
    private static final long ENCODING = 7944834171105125288L;

    private final StreamReplicator mRepl;

    private StreamReplicator.Reader mStreamReader;
    private DbWriter mDbWriter;

    private StreamReplicator.Writer mNextStreamWriter;

    DatabaseStreamReplicator(StreamReplicator repl) {
        mRepl = repl;
    }

    @Override
    public Socket connect(SocketAddress addr) throws IOException {
        return mRepl.connect(addr);
    }

    @Override
    public Consumer<Socket> socketAcceptor(Consumer<Socket> acceptor) {
        return mRepl.socketAcceptor(acceptor);
    }

    @Override
    public long encoding() {
        return ENCODING;
    }

    @Override
    public void start(long position) throws IOException {
        if (position < 0) {
            throw new IllegalArgumentException();
        }
        if (mStreamReader != null) {
            throw new IllegalStateException();
        }

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
    public void recover(Database db, EventListener listener) throws IOException {
        // FIXME
    }

    @Override
    public long readPosition() {
        StreamReplicator.Reader reader = mStreamReader;
        if (reader != null) {
            return reader.index();
        } else {
            // Might start off as the leader, so return it's start position. Nothing is
            // actually readable, however.
            return mDbWriter.mWriter.termStartIndex();
        }
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        StreamReplicator.Reader reader = mStreamReader;

        if (reader == null) {
            StreamReplicator.Writer streamWriter = mDbWriter.mWriter;

            if (!streamWriter.isDeactivated()) {
                return -1;
            }

            while (true) {
                long end = streamWriter.termEndIndex();
                long pos = streamWriter.waitForCommit(end, -1);

                if (pos == end) {
                    streamWriter.close();
                    mDbWriter = null;

                    while ((reader = mRepl.newReader(pos, false)) == null) {
                        StreamReplicator.Writer nextWriter = mRepl.newWriter(pos);
                        if (nextWriter != null) {
                            mNextStreamWriter = nextWriter;
                            return -1;
                        }
                    }

                    mStreamReader = reader;
                    break;
                }

                if (pos != -1) {
                    throw new IllegalStateException("Unexpected result: " + pos);
                }

                // Term ended even lower, so try again.
            }
        }

        while (true) {
            int amt = reader.read(b, off, len);
            if (amt >= 0) {
                return amt;
            }

            StreamReplicator.Reader nextReader;
            while ((nextReader = mRepl.newReader(reader.index(), false)) == null) {
                StreamReplicator.Writer nextWriter = mRepl.newWriter(reader.index());
                if (nextWriter != null) {
                    mNextStreamWriter = nextWriter;
                    return -1;
                }
            }

            reader.close();
            mStreamReader = reader = nextReader;
        }
    }

    @Override
    public void flip() {
        StreamReplicator.Writer nextWriter = mNextStreamWriter;

        if (nextWriter != null) {
            // Finish flip to leader.
            mDbWriter = new DbWriter(nextWriter);
            if (mStreamReader != null) {
                mStreamReader.close();
                mStreamReader = null;
            }
            mNextStreamWriter = null;
        }
    }

    @Override
    public Writer writer() throws IOException {
        return mDbWriter;
    }

    @Override
    public void sync() throws IOException {
        mRepl.sync();
    }

    @Override
    public void syncConfirm(long position, long timeoutNanos) throws IOException {
        mRepl.sync();
        // FIXME: Require that a majority have also persisted up to the given position.
    }

    @Override
    public void checkpointed(long position) throws IOException {
        // FIXME: Can perform log compaction.
    }

    @Override
    public void close() throws IOException {
        mRepl.close();
    }

    private static final class DbWriter implements DatabaseReplicator.Writer {
        final StreamReplicator.Writer mWriter;

        DbWriter(StreamReplicator.Writer writer) {
            mWriter = writer;
        }

        @Override
        public long position() {
            return mWriter.index();
        }

        @Override
        public boolean leaderNotify(Runnable callback) {
            mWriter.uponCommit(Long.MAX_VALUE, index -> new Thread(callback).start());
            return true;
        }

        @Override
        public boolean write(byte[] b, int off, int len, long commitPos) throws IOException {
            return mWriter.write(b, off, len, commitPos) >= len;
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
    }
}
