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

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.TestUtils;

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

    private static final long COMMIT_TIMEOUT_NANOS = 5_000_000_000L;

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

    private ReplicatorConfig[] mConfigs;
    private MessageReplicator[] mReplicators;

    /**
     * @return first is the leader
     */
    private MessageReplicator[] startGroup(int members) throws Exception {
        if (members < 1) {
            throw new IllegalArgumentException();
        }

        ServerSocket[] sockets = new ServerSocket[members];

        for (int i=0; i<members; i++) {
            sockets[i] = new ServerSocket(0);
        }

        mConfigs = new ReplicatorConfig[members];
        mReplicators = new MessageReplicator[members];

        for (int i=0; i<members; i++) {
            mConfigs[i] = new ReplicatorConfig()
                .baseFile(TestUtils.newTempBaseFile(getClass()))
                .groupToken(1)
                .localSocket(sockets[i]);

            if (i > 0) {
                mConfigs[i].addSeed(sockets[0].getLocalSocketAddress());
                mConfigs[i].localRole(Role.OBSERVER);
            }

            MessageReplicator repl = MessageReplicator.open(mConfigs[i]);
            mReplicators[i] = repl;
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

        assertEquals(0, writer.termStartIndex());
        assertEquals(0, reader.termStartIndex());

        assertEquals(Long.MAX_VALUE, writer.termEndIndex());
        assertEquals(Long.MAX_VALUE, reader.termEndIndex());

        byte[] message = "hello".getBytes();
        assertTrue(writer.writeMessage(message));
        assertEquals(writer.index(), writer.waitForCommit(writer.index(), 0));

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

        assertEquals(0, writer.termStartIndex());
        assertEquals(0, reader.termStartIndex());
        assertEquals(0, replica.termStartIndex());

        assertEquals(Long.MAX_VALUE, writer.termEndIndex());
        assertEquals(Long.MAX_VALUE, reader.termEndIndex());
        assertEquals(Long.MAX_VALUE, replica.termEndIndex());

        byte[] message = "hello".getBytes();
        assertTrue(writer.writeMessage(message));
        long highIndex = writer.index();
        assertEquals(highIndex, writer.waitForCommit(highIndex, COMMIT_TIMEOUT_NANOS));

        TestUtils.fastAssertArrayEquals(message, reader.readMessage());
        TestUtils.fastAssertArrayEquals(message, replica.readMessage());

        message = "world!".getBytes();
        assertTrue(writer.writeMessage(message));
        highIndex = writer.index();
        assertEquals(highIndex, writer.waitForCommit(highIndex, COMMIT_TIMEOUT_NANOS));

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
            long highIndex = writer.index();
            assertEquals(highIndex, writer.waitForCommit(highIndex, COMMIT_TIMEOUT_NANOS));

            TestUtils.fastAssertArrayEquals(message, r0.readMessage());
            TestUtils.fastAssertArrayEquals(message, r1.readMessage());
            TestUtils.fastAssertArrayEquals(message, r2.readMessage());
        }
    }
}
