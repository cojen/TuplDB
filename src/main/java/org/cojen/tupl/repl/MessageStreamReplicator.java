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

import java.io.EOFException;
import java.io.InterruptedIOException;
import java.io.IOException;

import java.net.Socket;
import java.net.SocketAddress;

import java.util.Map;

import java.util.function.Consumer;
import java.util.function.LongConsumer;

import org.cojen.tupl.io.Utils;

/**
 * MessageReplicator implementation backed by a StreamReplicator.
 *
 * @author Brian S O'Neill
 */
final class MessageStreamReplicator implements MessageReplicator {
    /*
      Message header:

      0b0xxx_xxxx +0  normal message, 0..127 bytes in length
      0b10xx_xxxx +1  normal message, 128..16511 bytes in length
      0b1110_0000 +4  normal message, 32-bit length
      0b1111_1111 +4  control message, 32-bit length
    */

    private final StreamReplicator mRepl;

    private boolean mWriterExists;
    private MsgWriter mWriter;

    MessageStreamReplicator(StreamReplicator repl) {
        mRepl = repl;
        repl.controlMessageAcceptor(this::writeControl);
    }

    @Override
    public long getLocalMemberId() {
        return mRepl.getLocalMemberId();
    }

    @Override
    public SocketAddress getLocalAddress() {
        return mRepl.getLocalAddress();
    }

    @Override
    public Role getLocalRole() {
        return mRepl.getLocalRole();
    }

    @Override
    public Socket connect(SocketAddress addr) throws IOException {
        return mRepl.connect(addr);
    }

    @Override
    public void socketAcceptor(Consumer<Socket> acceptor) {
        mRepl.socketAcceptor(acceptor);
    }

    @Override
    public void sync() throws IOException {
        mRepl.sync();
    }

    @Override
    public boolean syncCommit(long index, long nanosTimeout) throws IOException {
        return mRepl.syncCommit(index, nanosTimeout);
    }

    @Override
    public void compact(long index) throws IOException {
        mRepl.compact(index);
    }

    @Override
    public void start() throws IOException {
        mRepl.start();
    }

    @Override
    public SnapshotReceiver restore(Map<String, String> options) throws IOException {
        return mRepl.restore(options);
    }

    @Override
    public SnapshotReceiver requestSnapshot(Map<String, String> options) throws IOException {
        return mRepl.requestSnapshot(options);
    }

    @Override
    public void snapshotRequestAcceptor(Consumer<SnapshotSender> acceptor) {
        mRepl.snapshotRequestAcceptor(acceptor);
    }

    @Override
    public void close() throws IOException {
        mRepl.close();
    }

    /**
     * Enable or disable partitioned mode, which simulates a network partition. New connections
     * are rejected and existing connections are closed.
     */
    void partitioned(boolean enable) {
        ((Controller) mRepl).partitioned(enable);
    }

    @Override
    public Reader newReader(long index, boolean follow) {
        StreamReplicator.Reader source = mRepl.newReader(index, follow);
        if (source == null) {
            return null;
        }
        try {
            return new MsgReader(source);
        } catch (Throwable e) {
            Utils.closeQuietly(source);
            throw e;
        }
    }

    @Override
    public Writer newWriter() {
        return createWriter(-1);
    }

    @Override
    public Writer newWriter(long index) {
        if (index < 0) {
            throw new IllegalArgumentException();
        }
        return createWriter(index);
    }

    private synchronized Writer createWriter(long index) {
        if (mWriter != null) {
            if (mWriterExists) {
                throw new IllegalStateException("Writer already exists");
            }
            if (index >= 0 && index != mWriter.index()) {
                return null;
            }
        } else {
            StreamReplicator.Writer source;
            if (index < 0) {
                source = mRepl.newWriter();
            } else {
                source = mRepl.newWriter(index);
            }
            if (source == null) {
                return null;
            }
            try {
                mWriter = new MsgWriter(source);
            } catch (Throwable e) {
                Utils.closeQuietly(source);
                throw e;
            }
        }

        mWriterExists = true;
        return mWriter;
    }

    private void writeControl(byte[] message) {
        MsgWriter writer;

        synchronized (this) {
            writer = mWriter;
            if (writer == null) {
                StreamReplicator.Writer source = mRepl.newWriter();
                if (source == null) {
                    return;
                }
                try {
                    mWriter = writer = new MsgWriter(source);
                } catch (Throwable e) {
                    Utils.closeQuietly(source);
                    throw e;
                }
                // Don't set mWriterExists, because the writer wasn't explicitly requested.
            }
        }

        long index;
        try {
            index = writer.writeControl(message);
        } catch (IOException e) {
            throw Utils.rethrow(e);
        }

        if (index < 0) {
            return;
        }

        long commitIndex;
        try {
            commitIndex = writer.waitForCommit(index, 1_000_000_000L); // 1 second timeout
        } catch (InterruptedIOException e) {
            return;
        }

        if (commitIndex >= 0) {
            // If the application is also reading, then the message might be observed
            // twice. This is harmless because control messages are versioned (by the
            // GroupFile class), and are therefore idempotent.
            try {
                mRepl.controlMessageReceived(index, message);
            } catch (Throwable e) {
                Utils.closeQuietly(this);
                Utils.uncaught(e);
            }
        }
    }

    synchronized void closed(MsgWriter writer) {
        if (mWriter == writer) {
            mWriterExists = false;
            mWriter = null;
        }
    }

    private final class MsgReader implements Reader {
        private final StreamReplicator.Reader mSource;
        private final byte[] mBuffer;
        private int mPos;
        private int mEnd;

        private int mRemaining;

        MsgReader(StreamReplicator.Reader source) {
            mSource = source;
            mBuffer = new byte[8192];
        }

        @Override
        public long term() {
            return mSource.term();
        }

        @Override
        public long termStartIndex() {
            return mSource.termStartIndex();
        }

        @Override
        public long termEndIndex() {
            return mSource.termEndIndex();
        }

        @Override
        public long index() {
            return mSource.index();
        }

        @Override
        public void close() {
            mSource.close();
        }

        @Override
        public byte[] readMessage() throws IOException {
            if (mRemaining != 0) {
                throw new IllegalStateException("Partial message remains: " + mRemaining);
            }

            try {
                while (true) {
                    int avail = mEnd - mPos;
                    if (avail <= 0) {
                        avail = mSource.read(mBuffer);
                        if (avail <= 0) {
                            return null;
                        }
                        mPos = 0;
                        mEnd = avail;
                    }

                    int length = readLength(avail);
                    byte[] message = new byte[length & ~(1 << 31)];
                    decodeMessage(message);

                    if (length >= 0) {
                        return message;
                    }

                    mRepl.controlMessageReceived(index() - (mEnd - mPos), message);
                }
            } catch (Throwable e) {
                Utils.closeQuietly(this);
                throw e;
            }
        }

        private void decodeMessage(byte[] message) throws IOException {
            decodeMessage(message, 0, message.length);
        }

        private void decodeMessage(byte[] message, int offset, int length) throws IOException {
            int avail = mEnd - mPos;
            int rem = length - avail;

            if (rem <= 0) {
                System.arraycopy(mBuffer, mPos, message, offset, length);
                mPos += length;
            } else {
                System.arraycopy(mBuffer, mPos, message, offset, avail);
                offset += avail;
                mPos = 0;
                mEnd = 0;
                if (rem >= mBuffer.length) {
                    mSource.readFully(message, offset, rem);
                } else {
                    fillBuffer(rem, 0);
                    System.arraycopy(mBuffer, mPos, message, offset, rem);
                    mPos += rem;
                }
            }
        }

        @Override
        public int readMessage(byte[] buf, int offset, int length) throws IOException {
            try {
                int rem = mRemaining;

                if (rem == 0) {
                    while (true) {
                        int avail = mEnd - mPos;
                        if (avail <= 0) {
                            avail = mSource.read(mBuffer);
                            if (avail <= 0) {
                                return -1;
                            }
                            mPos = 0;
                            mEnd = avail;
                        }
                        rem = readLength(avail);
                        if (rem >= 0) {
                            break;
                        }
                        byte[] message = new byte[rem & ~(1 << 31)];
                        decodeMessage(message);
                        mRepl.controlMessageReceived(index() - (mEnd - mPos), message);
                    }
                }

                if (rem <= length) {
                    decodeMessage(buf, offset, rem);
                    mRemaining = 0;
                    return rem;
                } else {
                    decodeMessage(buf, offset, length);
                    rem -= length;
                    mRemaining = rem;
                    return ~rem;
                }
            } catch (Throwable e) {
                Utils.closeQuietly(this);
                throw e;
            }
        }

        /**
         * @param avail must be >= 1
         * @return length; high bit is set for control messages
         */
        private int readLength(int avail) throws IOException {
            byte b = mBuffer[mPos++];

            if (b >= 0) {
                // Short-length normal message.
                return b;
            }

            avail--;

            if ((b & 0xc0) == 0x80) {
                // Medium-length normal message.
                if (avail <= 0) {
                    fillBuffer(1, 0);
                }
                return 128 + (((b & 0x3f) << 8) | (mBuffer[mPos++] & 0xff));
            }

            // 32-bit length expected.
            if (avail < 4) {
                fillBuffer(4, avail);
            }

            int length = Utils.decodeIntLE(mBuffer, mPos);
            mPos += 4;

            if (length >= 0) {
                if (b == (byte) 0xe0) {
                    // Normal message.
                    return length;
                } else if (b == (byte) 0xff) {
                    // Control message.
                    return length | (1 << 31);
                }
            }

            throw new IllegalStateException("Illegal message: " + length + ", " + b);
        }

        private void fillBuffer(int required) throws IOException {
            int avail = mEnd - mPos;
            if ((required -= avail) > 0) {
                fillBuffer(required, avail);
            }
        }

        private void fillBuffer(int required, int avail) throws IOException {
            byte[] buf = mBuffer;
            int end = mEnd;
            int tail = buf.length - end;
            if (tail < required) {
                // Shift buffer contents to make room.
                System.arraycopy(buf, mPos, buf, 0, avail);
                mPos = 0;
                mEnd = end = avail;
                tail = buf.length - end;
            }

            while (true) {
                avail = mSource.read(buf, end, tail);
                if (avail <= 0) {
                    throw new EOFException();
                }
                end += avail;
                mEnd = end;
                required -= avail;
                if (required <= 0) {
                    break;
                }
                tail -= avail;
            }
        }
    }

    private final class MsgWriter implements Writer {
        private final StreamReplicator.Writer mSource;
        private final byte[] mBuffer;

        private boolean mFinished;

        MsgWriter(StreamReplicator.Writer source) {
            mSource = source;
            mBuffer = new byte[8192];
            mFinished = true;
        }

        @Override
        public long term() {
            return mSource.term();
        }

        @Override
        public long termStartIndex() {
            return mSource.termStartIndex();
        }

        @Override
        public long termEndIndex() {
            return mSource.termEndIndex();
        }

        @Override
        public long index() {
            return mSource.index();
        }

        @Override
        public long commitIndex() {
            return mSource.commitIndex();
        }

        @Override
        public void close() {
            closed(this);
            mSource.close();
        }

        @Override
        public long waitForCommit(long index, long nanosTimeout) throws InterruptedIOException {
            return mSource.waitForCommit(index, nanosTimeout);
        }

        @Override
        public void uponCommit(long index, LongConsumer task) {
            mSource.uponCommit(index, task);
        }

        @Override
        public synchronized boolean writeMessage(byte[] message, int offset, int length,
                                                 boolean finished)
            throws IOException
        {
            byte[] buffer = mBuffer;
            int pos = 0;

            if (length < 128) {
                buffer[pos++] = (byte) length;
            } else if (length < 16512) {
                int v = length - 128;
                buffer[pos++] = (byte) (0x80 | (v >> 8));
                buffer[pos++] = (byte) v;
            } else {
                buffer[pos++] = (byte) 0xe0;
                Utils.encodeIntLE(buffer, pos, length);
                pos += 4;
            }

            // Stash this to prevent control messages from finishing batches too soon.
            mFinished = finished;

            return doWriteMessage(pos, message, offset, length);
        }

        /**
         * @return index after message or -1 if not the leader
         */
        synchronized long writeControl(byte[] message) throws IOException {
            byte[] buffer = mBuffer;
            int pos = 0;
            buffer[pos++] = (byte) 0xff;
            Utils.encodeIntLE(buffer, pos, message.length);
            pos += 4;
            if (!doWriteMessage(pos, message, 0, message.length)) {
                return -1;
            } else {
                return mSource.index();
            }
        }

        // Caller must be synchronized.
        private boolean doWriteMessage(int pos, byte[] message, int offset, int length)
            throws IOException
        {
            try {
                byte[] buffer = mBuffer;
                int avail = buffer.length - pos;

                if (length <= avail) {
                    System.arraycopy(message, offset, buffer, pos, length);
                    pos += length;
                    return mSource.write(buffer, 0, pos, mFinished ? (index() + pos) : 0) >= pos;
                } else {
                    System.arraycopy(message, offset, buffer, pos, avail);
                    offset += avail;
                    length -= avail;
                    mSource.write(buffer, 0, buffer.length, 0);
                    return mSource.write(message, offset, length,
                                         mFinished ? (index() + length) : 0) >= length;
                }
            } catch (Throwable e) {
                Utils.closeQuietly(this);
                throw e;
            }
        }
    }
}
