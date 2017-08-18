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
        ServerSocket[] sockets = new ServerSocket[members];

        for (int i=0; i<members; i++) {
            sockets[i] = new ServerSocket(0);
        }

        mConfigs = new ReplicatorConfig[members];
        mReplicators = new StreamReplicator[members];

        for (int i=0; i<members; i++) {
            mConfigs[i] = new ReplicatorConfig()
                .baseFile(TestUtils.newTempBaseFile(getClass()))
                .groupId(1)
                .localSocket(sockets[i]);

            for (int j=0; j<members; j++) {
                mConfigs[i].addMember(j + 1, sockets[j].getLocalSocketAddress());
            }

            mReplicators[i] = StreamReplicator.open(mConfigs[i]);
        }

        final long start = 0;

        for (StreamReplicator repl : mReplicators) {
            repl.start();
        }

        // Wait for a leader.

        for (int trial=0; trial<100; trial++) {
            Thread.sleep(100);

            for (int i=0; i<mReplicators.length; i++) {
                StreamReplicator repl = mReplicators[i];
                Reader reader = repl.newReader(start, false);
                if (reader != null) {
                    reader.close();
                    continue;
                }
                mReplicators[i] = mReplicators[0];
                mReplicators[0] = repl;
                return mReplicators;
            }
        }

        throw new AssertionError("No leader");
    }

    @Test
    public void oneMember() throws Exception {
        StreamReplicator[] repls = startGroup(1);
        assertTrue(repls.length == 1);

        StreamReplicator repl = repls[0];

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
        assertEquals(message.length, writer.write(message));
        assertEquals(message.length, writer.waitForCommit(message.length, 0));

        byte[] buf = new byte[message.length];
        readFully(reader, buf);
        TestUtils.fastAssertArrayEquals(message, buf);
    }

    @Test
    public void twoMembers() throws Exception {
        StreamReplicator[] repls = startGroup(2);
        assertTrue(repls.length == 2);

        StreamReplicator repl = repls[0];

        Reader reader = repl.newReader(0, false);
        assertNull(reader);

        reader = repl.newReader(0, true);
        assertNotNull(reader);

        Writer writer = repl.newWriter(0);
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
        assertEquals(message.length, writer.write(message));
        long highIndex = message.length;
        assertEquals(highIndex, writer.waitForCommit(highIndex, COMMIT_TIMEOUT_NANOS));

        byte[] buf = new byte[message.length];
        readFully(reader, buf);
        TestUtils.fastAssertArrayEquals(message, buf);

        readFully(replica, buf);
        TestUtils.fastAssertArrayEquals(message, buf);

        message = "world!".getBytes();
        assertEquals(message.length, writer.write(message));
        highIndex += message.length;
        assertEquals(highIndex, writer.waitForCommit(highIndex, COMMIT_TIMEOUT_NANOS));

        buf = new byte[message.length];
        readFully(reader, buf);
        TestUtils.fastAssertArrayEquals(message, buf);

        readFully(replica, buf);
        TestUtils.fastAssertArrayEquals(message, buf);
    }
}
