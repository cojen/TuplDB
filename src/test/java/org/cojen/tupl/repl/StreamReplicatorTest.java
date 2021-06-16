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

import java.net.ServerSocket;

import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.function.LongConsumer;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.TestUtils;

import static org.cojen.tupl.repl.FileStateLogTest.*;
import static org.cojen.tupl.repl.StreamReplicator.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class StreamReplicatorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(StreamReplicatorTest.class.getName());
    }

    private static final long COMMIT_TIMEOUT_NANOS = 5_000_000_000L;

    @Before
    public void setup() throws Exception {
    }

    @After
    public void teardown() throws Exception {
        if (mReplicators != null) {
            for (StreamReplicator repl : mReplicators) {
                if (repl != null) {
                    repl.close();
                }
            }
        }

        TestUtils.deleteTempFiles(getClass());
    }

    private ReplicatorConfig[] mConfigs;
    private StreamReplicator[] mReplicators;

    /**
     * @return first is the leader
     */
    private StreamReplicator[] startGroup(int members) throws Exception {
        if (members < 1) {
            throw new IllegalArgumentException();
        }

        var sockets = new ServerSocket[members];

        for (int i=0; i<members; i++) {
            sockets[i] = TestUtils.newServerSocket();
        }

        mConfigs = new ReplicatorConfig[members];
        mReplicators = new StreamReplicator[members];

        for (int i=0; i<members; i++) {
            mConfigs[i] = new ReplicatorConfig()
                .baseFile(TestUtils.newTempBaseFile(getClass()))
                .groupToken(1)
                .localSocket(sockets[i]);

            if (i > 0) {
                mConfigs[i].addSeed(sockets[0].getLocalSocketAddress());
                // Use RESTORING state instead of OBSERVER to avoid race conditions caused by
                // control messages which update the role.
                mConfigs[i].localRole(Role.RESTORING);
            }

            StreamReplicator repl = StreamReplicator.open(mConfigs[i]);
            mReplicators[i] = repl;

            repl.controlMessageAcceptor(message -> {
                byte[] wrapped = wrapControlMessage(message);
                Writer writer = repl.newWriter();
                try {
                    writer.write(wrapped);
                    long position = writer.position();
                    writer.waitForCommit(position, -1);
                    repl.controlMessageReceived(position, message);
                } catch (IOException e) {
                    Utils.rethrow(e);
                } finally {
                    writer.close();
                }
            });

            repl.start();

            readyCheck: {
                for (int trial=0; trial<100; trial++) {
                    Thread.sleep(100);

                    if (i == 0) {
                        // Wait to become leader.
                        Reader reader = repl.newReader(0, false);
                        if (reader == null) {
                            break readyCheck;
                        }
                        reader.close();
                    } else {
                        // Wait to join the group.
                        Reader reader = repl.newReader(0, true);
                        reader.close();
                        if (reader.term() > 0) {
                            break readyCheck;
                        }
                    }
                }

                throw new AssertionError(i == 0 ? "No leader" : "Not joined");
            }
        }

        return mReplicators;
    }

    @Test
    public void oneMember() throws Exception {
        StreamReplicator[] repls = startGroup(1);
        assertTrue(repls.length == 1);

        StreamReplicator repl = repls[0];

        Reader reader = newReader(repl, 0, false);
        assertNull(reader);

        reader = newReader(repl, 0, true);
        assertNotNull(reader);

        Writer writer = repl.newWriter(0);
        assertNotNull(writer);

        long term = writer.term();
        assertEquals(1, term);
        assertEquals(term, reader.term());

        assertEquals(0, writer.termStartPosition());
        assertEquals(0, reader.termStartPosition());

        assertEquals(Long.MAX_VALUE, writer.termEndPosition());
        assertEquals(Long.MAX_VALUE, reader.termEndPosition());

        byte[] message = "hello".getBytes();
        byte[] wrapped = wrapMessage(message);
        assertTrue(writer.write(wrapped) > 0);
        assertEquals(wrapped.length, writer.waitForCommit(wrapped.length, 0));

        var buf = new byte[message.length];
        readFully(reader, buf);
        TestUtils.fastAssertArrayEquals(message, buf);
    }

    @Test
    public void twoMembers() throws Exception {
        StreamReplicator[] repls = startGroup(2);
        assertTrue(repls.length == 2);

        StreamReplicator repl = repls[0];

        Reader reader = newReader(repl, 0, false);
        assertNull(reader);

        reader = newReader(repl, 0, true);
        assertNotNull(reader);

        Writer writer = repl.newWriter();
        assertNotNull(writer);

        Reader replica = newReader(repls[1], 0, false);
        assertNotNull(replica);

        long term = writer.term();
        assertTrue(term > 0);
        assertEquals(term, reader.term());
        assertEquals(term, replica.term());

        assertEquals(0, writer.termStartPosition());
        assertEquals(0, reader.termStartPosition());
        assertEquals(0, replica.termStartPosition());

        assertEquals(Long.MAX_VALUE, writer.termEndPosition());
        assertEquals(Long.MAX_VALUE, reader.termEndPosition());
        assertEquals(Long.MAX_VALUE, replica.termEndPosition());

        byte[] message = "hello".getBytes();
        byte[] wrapped = wrapMessage(message);
        assertTrue(writer.write(wrapped) > 0);
        long highPosition = writer.position();
        assertEquals(highPosition, writer.waitForCommit(highPosition, COMMIT_TIMEOUT_NANOS));

        var buf = new byte[message.length];
        readFully(reader, buf);
        TestUtils.fastAssertArrayEquals(message, buf);

        readFully(replica, buf);
        TestUtils.fastAssertArrayEquals(message, buf);

        message = "world!".getBytes();
        wrapped = wrapMessage(message);
        assertTrue(writer.write(wrapped) > 0);
        highPosition = writer.position();
        assertEquals(highPosition, writer.waitForCommit(highPosition, COMMIT_TIMEOUT_NANOS));

        buf = new byte[message.length];
        readFully(reader, buf);
        TestUtils.fastAssertArrayEquals(message, buf);

        readFully(replica, buf);
        TestUtils.fastAssertArrayEquals(message, buf);
    }

    /**
     * Returns a reader which filters and redirect control messages.
     */
    private static Reader newReader(StreamReplicator repl, long start, boolean follow) {
        Reader source = repl.newReader(start, follow);
        return source == null ? null : new ControlReader(repl, source);
    }

    private static byte[] wrapMessage(byte[] message) {
        return wrapMessage(message, 0);
    }

    private static byte[] wrapControlMessage(byte[] message) {
        return wrapMessage(message, 0x80000000);
    }

    private static byte[] wrapMessage(byte[] message, int control) {
        var wrapped = new byte[4 + message.length];
        Utils.encodeLongLE(wrapped, 0, message.length | control);
        System.arraycopy(message, 0, wrapped, 4, message.length);
        return wrapped;
    }

    private static abstract class ControlAccessor<A extends Accessor> implements Accessor {
        final StreamReplicator mRepl;
        final A mSource;

        ControlAccessor(StreamReplicator repl, A source) {
            mRepl = repl;
            mSource = source;
        }

        @Override
        public long term() {
            return mSource.term();
        }

        @Override
        public long termStartPosition() {
            return mSource.termStartPosition();
        }

        @Override
        public long termEndPosition() {
            return mSource.termEndPosition();
        }

        @Override
        public long position() {
            return mSource.position();
        }

        @Override
        public long commitPosition() {
            return mSource.commitPosition();
        }

        @Override
        public void addCommitListener(LongConsumer listener) {
            mSource.addCommitListener(listener);
        }

        @Override
        public long waitForCommit(long position, long nanosTimeout) throws InterruptedIOException {
            return mSource.waitForCommit(position, nanosTimeout);
        }

        @Override
        public void uponCommit(long position, LongConsumer task) {
            mSource.uponCommit(position, task);
        }

        @Override
        public void close() {
            mSource.close();
        }
    }

    public static class ControlReader extends ControlAccessor<Reader> implements Reader {
        private byte[] mMessage;
        private int mMessagePos;

        ControlReader(StreamReplicator repl, Reader source) {
            super(repl, source);
        }

        @Override
        public int read(byte[] buf, int offset, int length) throws IOException {
            if (mMessage == null) {
                while (true) {
                    var b = new byte[4];
                    FileStateLogTest.readFully(mSource, b);
                    int len = Utils.decodeIntLE(b, 0);

                    b = new byte[len & 0x7fffffff];
                    FileStateLogTest.readFully(mSource, b);

                    if (len >= 0) {
                        mMessage = b;
                        mMessagePos = 0;
                        break;
                    }

                    // Control message.
                    mRepl.controlMessageReceived(mSource.position(), b);
                }
            }

            int avail = mMessage.length - mMessagePos;

            if (length >= avail) {
                System.arraycopy(mMessage, mMessagePos, buf, offset, avail);
                mMessage = null;
                return avail;
            } else {
                System.arraycopy(mMessage, mMessagePos, buf, offset, length);
                mMessagePos += length;
                return length;
            }
        }

        @Override
        public int tryRead(byte[] buf, int offset, int length) throws IOException {
            throw new UnsupportedOperationException();
        }
    }
}
