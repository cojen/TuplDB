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

import java.io.File;

import java.net.ServerSocket;

import java.util.Arrays;
import java.util.Random;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.core.TestUtils;

import static org.cojen.tupl.repl.MessageReplicator.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class MessageReplicatorTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(MessageReplicatorTest.class.getName());
    }

    private static final long COMMIT_TIMEOUT_NANOS = 10_000_000_000L;

    @Before
    public void setup() throws Exception {
    }

    @After
    public void teardown() throws Exception {
        if (mReplicators != null) {
            for (Replicator repl : mReplicators) {
                if (repl != null) {
                    repl.close();
                }
            }
        }

        TestUtils.deleteTempFiles(getClass());
    }

    private File[] mReplBaseFiles;
    private int[] mReplPorts;
    private ReplicatorConfig[] mConfigs;
    private MessageReplicator[] mReplicators;

    /**
     * @return first is the leader
     */
    private MessageReplicator[] startGroup(int members) throws Exception {
        return startGroup(members, Role.OBSERVER, true, false);
    }

    /**
     * @return first is the leader
     */
    private MessageReplicator[] startGroup(int members, Role replicaRole, boolean waitToJoin,
                                           boolean proxy)
        throws Exception
    {
        if (members < 1) {
            throw new IllegalArgumentException();
        }

        ServerSocket[] sockets = new ServerSocket[members];

        for (int i=0; i<members; i++) {
            sockets[i] = new ServerSocket(0);
        }

        mReplBaseFiles = new File[members];
        mReplPorts = new int[members];
        mConfigs = new ReplicatorConfig[members];
        mReplicators = new MessageReplicator[members];

        for (int i=0; i<members; i++) {
            mReplBaseFiles[i] = TestUtils.newTempBaseFile(getClass()); 
            mReplPorts[i] = sockets[i].getLocalPort();

            mConfigs[i] = new ReplicatorConfig()
                .baseFile(mReplBaseFiles[i])
                .groupToken(1)
                .localSocket(sockets[i])
                .proxyWrites(proxy);

            if (i > 0) {
                mConfigs[i].addSeed(sockets[0].getLocalSocketAddress());
                mConfigs[i].localRole(replicaRole);
            }

            MessageReplicator repl = MessageReplicator.open(mConfigs[i]);
            mReplicators[i] = repl;
            repl.start();

            readyCheck: {
                for (int trial=0; trial<100; trial++) {
                    TestUtils.sleep(100);

                    if (i == 0) {
                        // Wait to become leader.
                        Reader reader = repl.newReader(0, false);
                        if (reader == null) {
                            break readyCheck;
                        }
                        reader.close();
                    } else if (waitToJoin) {
                        // Wait to join the group.
                        Reader reader = repl.newReader(0, true);
                        reader.close();
                        if (reader.term() > 0) {
                            break readyCheck;
                        }
                    } else {
                        break readyCheck;
                    }
                }

                throw new AssertionError(i == 0 ? "No leader" : "Not joined");
            }
        }

        return mReplicators;
    }

    @Test
    public void oneMember() throws Exception {
        MessageReplicator[] repls = startGroup(1);
        assertTrue(repls.length == 1);

        MessageReplicator repl = repls[0];

        Reader reader = repl.newReader(0, false);
        assertNull(reader);

        reader = repl.newReader(0, true);
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
        assertTrue(writer.writeMessage(message));
        assertEquals(writer.position(), writer.waitForCommit(writer.position(), 0));

        TestUtils.fastAssertArrayEquals(message, reader.readMessage());
    }

    @Test
    public void twoMembers() throws Exception {
        MessageReplicator[] repls = startGroup(2);
        assertTrue(repls.length == 2);

        MessageReplicator repl = repls[0];

        Reader reader = repl.newReader(0, false);
        assertNull(reader);

        reader = repl.newReader(0, true);
        assertNotNull(reader);

        Writer writer = repl.newWriter();
        assertNotNull(writer);

        Reader replica = repls[1].newReader(0, false);
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
        assertTrue(writer.writeMessage(message));
        long highPosition = writer.position();
        assertEquals(highPosition, writer.waitForCommit(highPosition, COMMIT_TIMEOUT_NANOS));

        TestUtils.fastAssertArrayEquals(message, reader.readMessage());
        TestUtils.fastAssertArrayEquals(message, replica.readMessage());

        message = "world!".getBytes();
        assertTrue(writer.writeMessage(message));
        highPosition = writer.position();
        assertEquals(highPosition, writer.waitForCommit(highPosition, COMMIT_TIMEOUT_NANOS));

        TestUtils.fastAssertArrayEquals(message, reader.readMessage());
        TestUtils.fastAssertArrayEquals(message, replica.readMessage());
    }

    @Test
    public void threeMembers() throws Exception {
        MessageReplicator[] repls = startGroup(3);
        assertTrue(repls.length == 3);

        Writer writer = repls[0].newWriter();
        Reader r0 = repls[0].newReader(0, true);
        Reader r1 = repls[1].newReader(0, true);
        Reader r2 = repls[2].newReader(0, true);

        byte[][] messages = {"hello".getBytes(), "world!".getBytes()};

        for (byte[] message : messages) {
            assertTrue(writer.writeMessage(message));
            long highPosition = writer.position();
            assertTrue(highPosition <= writer.waitForCommit(highPosition, COMMIT_TIMEOUT_NANOS));

            TestUtils.fastAssertArrayEquals(message, r0.readMessage());
            TestUtils.fastAssertArrayEquals(message, r1.readMessage());
            TestUtils.fastAssertArrayEquals(message, r2.readMessage());
        }
    }

    @Test
    public void variableMessageSizes() throws Exception {
        variableMessageSizes(false);
    }

    @Test
    public void variableMessageSizesPartial() throws Exception {
        variableMessageSizes(true);
    }

    private void variableMessageSizes(boolean partial) throws Exception {
        MessageReplicator[] repls = startGroup(3);

        int[] sizes = {
            0, 1, 10, 100, 127,
            128, 129, 1000, 8190, 8191, 8192, 8193, 10000, 16383, 16384, 16385, 16510, 16511,
            16512, 16513, 20000, 100_000
        };

        final long seed = 9823745;
        Random rnd = new Random(seed);

        Writer writer = repls[0].newWriter();

        for (int size : sizes) {
            byte[] message = TestUtils.randomStr(rnd, size);
            assertTrue(writer.writeMessage(message));
        }

        long highPosition = writer.position();
        assertEquals(highPosition, writer.waitForCommit(highPosition, COMMIT_TIMEOUT_NANOS));

        byte[] buf = new byte[1001];

        for (MessageReplicator repl : repls) {
            rnd = new Random(seed);

            Reader reader = repl.newReader(0, true);

            for (int size : sizes) {
                byte[] expected = TestUtils.randomStr(rnd, size);

                byte[] actual;
                if (!partial) {
                    actual = reader.readMessage();
                } else {
                    int result = reader.readMessage(buf, 1, 1000);

                    if (result >= 0) {
                        actual = Arrays.copyOfRange(buf, 1, 1 + result);
                    } else {
                        try {
                            reader.readMessage();
                            fail();
                        } catch (IllegalStateException e) {
                            // Cannot read next message when partial exists.
                        }
                        assertNotEquals(-1, result);
                        actual = new byte[1000 + ~result];
                        System.arraycopy(buf, 1, actual, 0, 1000);
                        reader.readMessage(actual, 1000, ~result);
                    }
                }

                TestUtils.fastAssertArrayEquals(expected, actual);
            }

            reader.close();
        }
    }

    @Test
    public void largeGroupNoWaitToJoin() throws Exception {
        largeGroupNoWaitToJoin(false);
    }

    @Test
    public void largeGroupNoWaitToJoinProxyWrites() throws Exception {
        largeGroupNoWaitToJoin(true);
    }

    private void largeGroupNoWaitToJoin(boolean proxy) throws Exception {
        final int count = 10;

        MessageReplicator[] repls = startGroup(count, Role.PROXY, false, proxy);

        Writer writer = repls[0].newWriter();

        Reader[] readers = new Reader[count];
        for (int i=0; i<count; i++) {
            readers[i] = repls[i].newReader(0, true);
        }

        for (int q=0; q<20; q++) {
            byte[] message = ("hello-" + q).getBytes();

            assertTrue(writer.writeMessage(message));
            long highPosition = writer.position();

            // Note: Actual commit position might be higher, because of control messages which
            // were sent by the replication system for membership changes.
            long commitPosition = writer.waitForCommit(highPosition, COMMIT_TIMEOUT_NANOS);
            assertTrue("highPosition: " + highPosition + ", commitPosition: " + commitPosition,
                       commitPosition >= highPosition);

            for (int j=0; j<readers.length; j++) {
                Reader reader = readers[j];

                byte[] actual;
                while ((actual = reader.readMessage()) == null) {
                    // Reader was created at false term of 0. Move to the correct one.
                    reader.close();
                    reader = repls[j].newReader(reader.position(), true);
                    readers[j] = reader;
                }

                TestUtils.fastAssertArrayEquals(message, actual);
            }
        }
    }

    @Test
    public void explicitFailover() throws Exception {
        for (int i=10; --i>=0; ) {
            try {
                doExplicitFailover();
                return;
            } catch (AssertionError e) {
                // Failover is racy and can elect the original leader.
                if (i == 0) {
                    throw e;
                }
                teardown();
                setup();
            }
        }
    }

    private void doExplicitFailover() throws Exception {
        MessageReplicator[] repls = startGroup(3, Role.NORMAL, true, false);
        assertTrue(repls.length == 3);

        Writer writer = repls[0].newWriter();

        Reader[] readers = new Reader[repls.length];
        for (int i=0; i<readers.length; i++) {
            readers[i] = repls[i].newReader(0, true);
        }

        byte[][] messages = {"hello".getBytes(), "world!".getBytes()};

        for (byte[] message : messages) {
            assertTrue(writer.writeMessage(message));
            long highPosition = writer.position();
            assertTrue(highPosition <= writer.waitForCommit(highPosition, COMMIT_TIMEOUT_NANOS));

            for (Reader r : readers) {
                TestUtils.fastAssertArrayEquals(message, r.readMessage());
            }
        }

        for (int i=1; i<repls.length; i++) {
            // Replicas aren't leaders, so failover should do nothing.
            assertTrue(repls[i].failover());
        }

        // Now do the actual failover.
        assertTrue(repls[0].failover());

        // Writes to the old leader immediately fail.
        byte[] message = "failover".getBytes();
        assertFalse(writer.writeMessage(message));
        writer.close();

        // Try to find the new leader.
        writer = null;
        int writerSlot = -1;

        find: {
            Writer[] writers = new Writer[repls.length];

            for (int i=0; i<10; i++) {
                for (int j=0; j<repls.length; j++) {
                    MessageReplicator repl = repls[j];
                    Writer w = writers[j];
                    if (w == null) {
                        writers[j] = w = repl.newWriter();
                    }
                    if (w != null && w.writeMessage(message)) {
                        writer = w;
                        writerSlot = j;
                        break find;
                    }
                }

                TestUtils.sleep(1000);
            }
        }

        assertNotNull(writer);
        assertNotEquals(0, writerSlot);

        long highPosition = writer.position();
        assertTrue(highPosition <= writer.waitForCommit(highPosition, COMMIT_TIMEOUT_NANOS));

        for (int i=0; i<readers.length; i++) {
            while (true) {
                byte[] msg = readers[i].readMessage();
                if (msg == null) {
                    readers[i].close();
                    readers[i] = repls[i].newReader(readers[i].position(), true);
                } else {
                    TestUtils.fastAssertArrayEquals(message, msg);
                    break;
                }
            }
        }
    }
}
