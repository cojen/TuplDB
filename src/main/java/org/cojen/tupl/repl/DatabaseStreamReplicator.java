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

import org.cojen.tupl.ConfirmationFailureException;
import org.cojen.tupl.ConfirmationInterruptedException;
import org.cojen.tupl.ConfirmationTimeoutException;
import org.cojen.tupl.EventListener;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class DatabaseStreamReplicator implements DatabaseReplicator {
    private static final long ENCODING = 7944834171105125288L;

    private final StreamReplicator mRepl;

    private StreamReplicator.Reader mStreamReader;
    private DbWriter mDbWriter;

    DatabaseStreamReplicator(StreamReplicator repl) {
        mRepl = repl;
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
        doFlip(position);
    }

    @Override
    public void recover(EventListener listener) throws IOException {
        // FIXME
    }

    @Override
    public long readPosition() {
        StreamReplicator.Reader reader = mStreamReader;
        if (reader != null) {
            return reader.index();
        } else {
            return mDbWriter.mWriter.index();
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
            StreamReplicator.Reader nextReader = mRepl.newReader(reader.index(), false);
            if (nextReader == null) {
                return -1;
            }
            reader.close();
            mStreamReader = reader = nextReader;
        }
    }

    @Override
    public void flip() {
        if (mStreamReader != null) {
            doFlip(mStreamReader.index());
            return;
        }

        StreamReplicator.Writer writer = mDbWriter.mWriter;

        while (true) {
            long end = writer.termEndIndex();

            if (!writer.isDeactivated()) {
                // Don't flip from active term.
                return;
            }

            long pos;
            while (true) {
                try {
                    pos = writer.waitForCommit(end, -1);
                    break;
                } catch (InterruptedIOException e) {
                    // Ignore and keep trying.
                    Thread.yield();
                }
            }

            if (pos == end) {
                doFlip(pos);
                return;
            }

            if (pos != -1) {
                String msg;
                if (pos == Long.MIN_VALUE) {
                    msg = "Closed";
                } else {
                    msg = "Unexpected result: " + pos;
                }
                throw new IllegalStateException(msg);
            }

            // Term ended even lower, so try again.
        }
    }

    private void doFlip(long pos) {
        if (mStreamReader != null) {
            mStreamReader.close();
            mStreamReader = null;
        }

        if (mDbWriter != null) {
            mDbWriter.mWriter.close();
            mDbWriter = null;
        }

        while (true) {
            StreamReplicator.Reader reader = mRepl.newReader(pos, false);
            if (reader != null) {
                mStreamReader = reader;
                return;
            }
            StreamReplicator.Writer writer = mRepl.newWriter(pos);
            if (writer != null) {
                mDbWriter = new DbWriter(writer);
                return;
            }
        }
    }

    @Override
    public Writer writer() throws IOException {
        return mDbWriter;
    }

    @Override
    public void sync() throws IOException {
        // FIXME
    }

    @Override
    public void syncConfirm(long position, long timeoutNanos) throws IOException {
        // FIXME
    }

    @Override
    public void checkpointed(long position) throws IOException {
        // FIXME
    }

    @Override
    public void close() throws IOException {
        mRepl.close();
    }

    /**
     * Returns normally only if term ended before reaching the desired position.
     */
    static void evaluateConfirmFailure(long pos, long nanosTimeout)
        throws ConfirmationFailureException
    {
        if (pos != -1) {
            if (pos == Long.MIN_VALUE) {
                throw new ConfirmationFailureException("Closed");
            }
            if (pos == -2) {
                throw new ConfirmationTimeoutException(nanosTimeout);
            }
            throw new ConfirmationFailureException("Unexpected result: " + pos);
        }
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
            mWriter.uponCommit(Long.MAX_VALUE, index -> {
                if (index != Long.MIN_VALUE) {
                    new Thread(callback).start();
                }
            });

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
            if (pos == Long.MIN_VALUE) {
                throw new ConfirmationFailureException("Closed");
            }
            if (pos == -2) {
                throw new ConfirmationTimeoutException(nanosTimeout);
            }
            throw new ConfirmationFailureException("Unexpected result: " + pos);
        }
    }
}
