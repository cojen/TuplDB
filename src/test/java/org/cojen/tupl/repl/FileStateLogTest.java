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
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.TestUtils;

import org.cojen.tupl.io.Utils;

import static org.cojen.tupl.repl.FileTermLogTest.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class FileStateLogTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(FileStateLogTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mBase = TestUtils.newTempBaseFile(getClass());
        mLog = new FileStateLog(mBase);
    }

    @After
    public void teardown() throws Exception {
        if (mLog != null) {
            mLog.close();
        }

        for (StateLog log : mMoreLogs) {
            log.close();
        }

        TestUtils.deleteTempFiles(getClass());
    }

    private File mBase;
    private FileStateLog mLog;
    private List<StateLog> mMoreLogs = new ArrayList<>();

    private StateLog newTempLog() throws Exception {
        StateLog log = new FileStateLog(TestUtils.newTempBaseFile(getClass()));
        mMoreLogs.add(log);
        return log;
    }

    @Test
    public void defineTerms() throws Exception {
        // Tests various ways in which terms can be defined (or not).

        // No such previous term exists.
        assertFalse(mLog.defineTerm(10, 11, 1000));

        // Allow leader to define a term with no previous term check.
        assertTrue(mLog.defineTerm(0, 10, 1000));

        // Allow leader to define a higher term with no previous term check.
        assertTrue(mLog.defineTerm(0, 11, 2000));

        // Allow follower to define a higher term.
        TermLog term = mLog.defineTermLog(11, 15, 3000);
        assertTrue(term != null);
        assertTrue(term == mLog.defineTermLog(11, 15, 3000));

        // Previous term conflict.
        assertFalse(mLog.defineTerm(10, 16, 4000));

        // Index in the middle of an existing term.
        assertTrue(mLog.defineTerm(15, 15, 4000));

        // Index in the middle of an existing term, but index is out of bounds.
        assertFalse(mLog.defineTerm(11, 11, 1000));
        assertFalse(mLog.defineTerm(11, 11, 2000));
        assertFalse(mLog.defineTerm(11, 11, 3000));
        assertFalse(mLog.defineTerm(11, 11, 5000));

        // Mustn't define a term if index as the highest, although it's not usable.
        term = mLog.defineTermLog(15, 15, 10000);
        assertTrue(term != null);
        assertTrue(term == mLog.defineTermLog(15, 15, Long.MAX_VALUE));

        int[] countRef = {0};

        mLog.queryTerms(0, 20000, (prevTerm, trm, startIndex) -> {
            countRef[0]++;
            switch ((int) trm) {
            default:
                fail("unknown term: " + trm);
            case 10:
                assertEquals(0, prevTerm);
                assertEquals(1000, startIndex);
                break;
            case 11:
                assertEquals(10, prevTerm);
                assertEquals(2000, startIndex);
                break;
            case 15:
                assertEquals(11, prevTerm);
                assertEquals(3000, startIndex);
                break;
            }
        });

        assertEquals(3, countRef[0]);
    }

    @Test
    public void missingRanges() throws Exception {
        // Verify that missing ranges can be queried.

        RangeResult result = new RangeResult();
        assertEquals(0, mLog.checkForMissingData(Long.MAX_VALUE, result));
        assertEquals(0, result.mRanges.size());

        LogWriter writer = mLog.openWriter(0, 1, 0);
        write(writer, new byte[100]);
        writer.release();

        result = new RangeResult();
        assertEquals(100, mLog.checkForMissingData(0, result));
        assertEquals(0, result.mRanges.size());
        assertEquals(100, mLog.checkForMissingData(100, result));

        // Define a new term before the previous one is filled in.
        mLog.defineTerm(1, 2, 500);
        result = new RangeResult();
        assertEquals(100, mLog.checkForMissingData(100, result));
        assertEquals(1, result.mRanges.size());
        assertEquals(new Range(100, 500), result.mRanges.get(0));

        // Write some data into the new term.
        writer = mLog.openWriter(1, 2, 500);
        write(writer, new byte[10]);
        writer.release();
        result = new RangeResult();
        assertEquals(100, mLog.checkForMissingData(100, result));
        assertEquals(1, result.mRanges.size());
        assertEquals(new Range(100, 500), result.mRanges.get(0));

        // Create a missing range in the new term.
        writer = mLog.openWriter(2, 2, 600);
        write(writer, new byte[10]);
        writer.release();
        result = new RangeResult();
        assertEquals(100, mLog.checkForMissingData(100, result));
        assertEquals(2, result.mRanges.size());
        assertEquals(new Range(100, 500), result.mRanges.get(0));
        assertEquals(new Range(510, 600), result.mRanges.get(1));

        // Go back to the previous term and fill in some of the missing range, creating another
        // missing range.
        writer = mLog.openWriter(1, 1, 200);
        write(writer, new byte[50]);
        writer.release();
        result = new RangeResult();
        assertEquals(100, mLog.checkForMissingData(100, result));
        assertEquals(3, result.mRanges.size());
        assertEquals(new Range(100, 200), result.mRanges.get(0));
        assertEquals(new Range(250, 500), result.mRanges.get(1));
        assertEquals(new Range(510, 600), result.mRanges.get(2));

        // Fill in the lowest missing range.
        writer = mLog.openWriter(1, 1, 100);
        write(writer, new byte[100]);
        writer.release();
        result = new RangeResult();
        assertEquals(250, mLog.checkForMissingData(100, result));
        assertEquals(0, result.mRanges.size());
        result = new RangeResult();
        assertEquals(250, mLog.checkForMissingData(250, result));
        assertEquals(2, result.mRanges.size());
        assertEquals(new Range(250, 500), result.mRanges.get(0));
        assertEquals(new Range(510, 600), result.mRanges.get(1));

        // Partially fill in the highest missing range.
        writer = mLog.openWriter(2, 2, 510);
        write(writer, new byte[20]);
        writer.release();
        result = new RangeResult();
        assertEquals(250, mLog.checkForMissingData(250, result));
        assertEquals(2, result.mRanges.size());
        assertEquals(new Range(250, 500), result.mRanges.get(0));
        assertEquals(new Range(530, 600), result.mRanges.get(1));

        // Fill in the next lowest missing range.
        writer = mLog.openWriter(1, 1, 250);
        write(writer, new byte[250]);
        writer.release();
        result = new RangeResult();
        assertEquals(530, mLog.checkForMissingData(250, result));
        assertEquals(0, result.mRanges.size());
        result = new RangeResult();
        assertEquals(530, mLog.checkForMissingData(530, result));
        assertEquals(1, result.mRanges.size());
        assertEquals(new Range(530, 600), result.mRanges.get(0));

        // Fill in the last missing range.
        writer = mLog.openWriter(2, 2, 530);
        write(writer, new byte[70]);
        writer.release();
        result = new RangeResult();
        assertEquals(610, mLog.checkForMissingData(530, result));
        assertEquals(0, result.mRanges.size());
        result = new RangeResult();
        assertEquals(610, mLog.checkForMissingData(610, result));
        assertEquals(0, result.mRanges.size());
    }

    @Test
    public void waitForTerm() throws Exception {
        // Opening a reader might require a wait.

        LogReader reader = mLog.openReader(0, 0);
        assertNull(reader);
        reader = mLog.openReader(0, 10);
        assertNull(reader);

        new Thread(() -> {
            try {
                TestUtils.sleep(500);
                mLog.defineTerm(0, 2, 50);
            } catch (Exception e) {
                Utils.uncaught(e);
            }
        }).start();

        reader = mLog.openReader(100, -1);
        assertEquals(2, reader.term());
        assertEquals(100, reader.index());

        // Missing term at index 0 makes reading impossible.
        try {
            mLog.openReader(0, -1);
            fail();
        } catch (IllegalStateException e) {
            // Expected.
        }

        // No wait because a term exists.
        reader = mLog.openReader(60, -1);
        assertTrue(reader != null);
        assertEquals(2, reader.term());
        assertEquals(60, reader.index());

        mLog.defineTerm(1, 2, 1000);

        reader = mLog.openReader(1100, -1);
        assertTrue(reader != null);
        assertEquals(2, reader.term());
        assertEquals(1100, reader.index());
    }

    //@Test
    public void raftFig7() throws Exception {
        // Tests the scenarios shown in figure 7 of the Raft paper.

        // Leader.
        writeTerm(mLog,  0,  '1', 1 - 1, 3);
        writeTerm(mLog, '1', '4', 4 - 1, 2);
        writeTerm(mLog, '4', '5', 6 - 1, 2);
        writeTerm(mLog, '5', '6', 8 - 1, 3);
        verifyLog(mLog, 0, "1114455666".getBytes());
        writeTerm(mLog, '6', '8', 11 - 1, 1);
        verifyLog(mLog, 0, "11144556668".getBytes());

        StateLog logA = newTempLog();
        writeTerm(logA,  0,  '1', 1 - 1, 3);
        writeTerm(logA, '1', '4', 4 - 1, 2);
        writeTerm(logA, '4', '5', 6 - 1, 2);
        writeTerm(logA, '5', '6', 8 - 1, 2);
        verifyLog(logA, 0, "111445566".getBytes());

        replicate(mLog, 11 - 1, logA);
        verifyLog(logA, 0, "11144556668".getBytes());

        StateLog logB = newTempLog();
        writeTerm(logB,  0,  '1', 1 - 1, 3);
        writeTerm(logB, '1', '4', 4 - 1, 1);
        verifyLog(logB, 0, "1114".getBytes());

        replicate(mLog, 11 - 1, logB);
        verifyLog(logB, 0, "11144556668".getBytes());

        StateLog logC = newTempLog();
        writeTerm(logC,  0,  '1', 1 - 1, 3);
        writeTerm(logC, '1', '4', 4 - 1, 2);
        writeTerm(logC, '4', '5', 6 - 1, 2);
        writeTerm(logC, '5', '6', 8 - 1, 4);
        verifyLog(logC, 0, "11144556666".getBytes());

        replicate(mLog, 11 - 1, logC);
        verifyLog(logC, 0, "11144556668".getBytes());

        StateLog logD = newTempLog();
        writeTerm(logD,  0,  '1', 1 - 1, 3);
        writeTerm(logD, '1', '4', 4 - 1, 2);
        writeTerm(logD, '4', '5', 6 - 1, 2);
        writeTerm(logD, '5', '6', 8 - 1, 3);
        writeTerm(logD, '6', '7', 11 - 1, 2);
        verifyLog(logD, 0, "111445566677".getBytes());

        replicate(mLog, 11 - 1, logD);
        verifyLog(logD, 0, "11144556668".getBytes());

        StateLog logE = newTempLog();
        writeTerm(logE,  0,  '1', 1 - 1, 3);
        writeTerm(logE, '1', '4', 4 - 1, 4);
        verifyLog(logE, 0, "1114444".getBytes());

        replicate(mLog, 11 - 1, logE);
        verifyLog(logE, 0, "11144556668".getBytes());

        StateLog logF = newTempLog();
        writeTerm(logF,  0,  '1', 1 - 1, 3);
        writeTerm(logF, '1', '2', 4 - 1, 3);
        writeTerm(logF, '2', '3', 7 - 1, 5);
        verifyLog(logF, 0, "11122233333".getBytes());

        replicate(mLog, 11 - 1, logF);
        verifyLog(logF, 0, "11144556668".getBytes());
    }

    private static void replicate(StateLog from, long index, StateLog to) throws IOException {
        LogInfo info = new LogInfo();
        to.captureHighest(info);

        if (info.mHighestIndex < index) {
            index = info.mHighestIndex;
        }

        LogReader reader;
        LogWriter writer;

        while (true) {
            reader = from.openReader(index, -1);
            writer = to.openWriter(reader.prevTerm(), reader.term(), index);
            if (writer != null) {
                break;
            }
            reader.release();
            index--;
        }

        byte[] buf = new byte[100];

        while (true) {
            int amt = reader.readAny(buf, 0, buf.length);
            if (amt <= 0) {
                reader.release();
                writer.release();
                if (amt == 0) {
                    break;
                }
                reader = from.openReader(index, -1);
                writer = to.openWriter(reader.prevTerm(), reader.term(), index);
            } else {
                write(writer, buf, 0, amt);
                index += amt;
            }
        }
    }

    private static void verifyLog(StateLog log, int index, byte[] expect) throws IOException {
        LogReader reader = log.openReader(index, -1);

        byte[] buf = new byte[expect.length];
        int offset = 0;

        while (offset < buf.length) {
            int amt = reader.readAny(buf, offset, buf.length - offset);
            if (amt <= 0) {
                reader.release();
                if (amt == 0) {
                    fail("nothing read");
                }
                reader = log.openReader(index, -1);
            } else {
                index += amt;
                offset += amt;
            }
        }

        TestUtils.fastAssertArrayEquals(expect, buf);

        assertEquals(0, reader.readAny(buf, 0, 1));
        reader.release();
    }

    private static void writeTerm(StateLog log, int prevTerm, int term, int index, int len)
        throws IOException
    {
        byte[] data = new byte[len];
        Arrays.fill(data, (byte) term);
        LogWriter writer = log.openWriter(prevTerm, term, index);
        write(writer, data);
        writer.release();
    }

    private static void write(LogWriter writer, byte[] data) throws IOException {
        write(writer, data, 0, data.length);
    }

    private static void write(LogWriter writer, byte[] data, int off, int len) throws IOException {
        int amt = writer.write(data, off, len, writer.index() + len);
        assertEquals(len, amt);
    }
}
