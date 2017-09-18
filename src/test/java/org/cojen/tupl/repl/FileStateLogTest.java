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
import java.io.File;
import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

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

        // Allow term definition with no previous term at the start.
        mLog.truncateStart(1000);
        assertTrue(mLog.defineTerm(0, 10, 1000));
        assertTrue(mLog.defineTerm(0, 10, 1000));

        assertFalse(mLog.defineTerm(0, 11, 2000));
        assertTrue(mLog.defineTerm(10, 11, 2000));

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
        assertFalse(mLog.defineTerm(11, 11, 1500));
        assertFalse(mLog.defineTerm(11, 11, 2000));
        assertFalse(mLog.defineTerm(11, 11, 3000));
        assertFalse(mLog.defineTerm(11, 11, 5000));
        assertTrue(mLog.defineTerm(11, 11, 2500)); // actually in bounds

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
                break;
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

        // Find terms when the index range doesn't touch a boundary.

        countRef[0] = 0;
        mLog.queryTerms(1100, 1200, (prevTerm, trm, startIndex) -> {
            countRef[0]++;
            assertEquals(0, prevTerm);
            assertEquals(10, trm);
            assertEquals(1000, startIndex);
        });

        assertEquals(1, countRef[0]);

        countRef[0] = 0;
        mLog.queryTerms(1100, 2000, (prevTerm, trm, startIndex) -> {
            countRef[0]++;
            assertEquals(0, prevTerm);
            assertEquals(10, trm);
            assertEquals(1000, startIndex);
        });

        assertEquals(1, countRef[0]);

        countRef[0] = 0;
        mLog.queryTerms(2000, 2100, (prevTerm, trm, startIndex) -> {
            countRef[0]++;
            assertEquals(10, prevTerm);
            assertEquals(11, trm);
            assertEquals(2000, startIndex);
        });

        assertEquals(1, countRef[0]);

        countRef[0] = 0;
        mLog.queryTerms(2100, 2100, (prevTerm, trm, startIndex) -> {
            countRef[0]++;
        });

        assertEquals(0, countRef[0]);
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
    public void missingRangesHighStart() throws Exception {
        // Verify missing ranges when log starts higher than index zero.

        // Start at index 1000.
        mLog.truncateStart(1000);
        mLog.defineTerm(0, 10, 1000);

        RangeResult result = new RangeResult();
        assertEquals(1000, mLog.checkForMissingData(Long.MAX_VALUE, result));
        assertEquals(0, result.mRanges.size());

        LogWriter writer = mLog.openWriter(0, 10, 1000);
        write(writer, new byte[100]);
        writer.release();

        result = new RangeResult();
        assertEquals(1100, mLog.checkForMissingData(Long.MAX_VALUE, result));
        assertEquals(0, result.mRanges.size());
        result = new RangeResult();
        assertEquals(1100, mLog.checkForMissingData(1000, result));
        assertEquals(0, result.mRanges.size());

        // Define a new term before the previous one is filled in.
        mLog.defineTerm(10, 11, 2000);
        result = new RangeResult();
        assertEquals(1100, mLog.checkForMissingData(Long.MAX_VALUE, result));
        assertEquals(0, result.mRanges.size());
        result = new RangeResult();
        assertEquals(1100, mLog.checkForMissingData(1100, result));
        assertEquals(1, result.mRanges.size());
        assertEquals(new Range(1100, 2000), result.mRanges.get(0));

        // Write some data into the new term.
        writer = mLog.openWriter(10, 11, 2000);
        write(writer, new byte[100]);
        writer.release();
        result = new RangeResult();
        assertEquals(1100, mLog.checkForMissingData(Long.MAX_VALUE, result));
        assertEquals(0, result.mRanges.size());
        result = new RangeResult();
        assertEquals(1100, mLog.checkForMissingData(1100, result));
        assertEquals(1, result.mRanges.size());
        assertEquals(new Range(1100, 2000), result.mRanges.get(0));

        // Fill in the missing range.
        writer = mLog.openWriter(10, 10, 1100);
        write(writer, new byte[900]);
        writer.release();
        result = new RangeResult();
        assertEquals(2100, mLog.checkForMissingData(Long.MAX_VALUE, result));
        assertEquals(0, result.mRanges.size());
        result = new RangeResult();
        assertEquals(2100, mLog.checkForMissingData(2100, result));
        assertEquals(0, result.mRanges.size());
    }

    @Test
    public void primordialTerm() throws Exception {
        LogReader reader = mLog.openReader(0);

        byte[] buf = new byte[10];
        assertEquals(0, reader.readAny(buf, 0, buf.length));

        new Thread(() -> {
            try {
                TestUtils.sleep(500);
                mLog.defineTerm(0, 2, 0);
            } catch (Exception e) {
                Utils.uncaught(e);
            }
        }).start();

        assertEquals(-1, reader.read(buf, 0, buf.length));
    }

    @Test
    public void highestIndex() throws Exception {
        // Verify highest index and commit index behavior.

        LogInfo info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(0, info.mTerm);
        assertEquals(0, info.mHighestIndex);
        assertEquals(0, info.mCommitIndex);

        mLog.commit(0);

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(0, info.mTerm);
        assertEquals(0, info.mHighestIndex);
        assertEquals(0, info.mCommitIndex);

        LogWriter writer = mLog.openWriter(0, 1, 0);
        write(writer, new byte[100]);
        writer.release();

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(1, info.mTerm);
        assertEquals(100, info.mHighestIndex);
        assertEquals(0, info.mCommitIndex);

        mLog.commit(50);
        
        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(1, info.mTerm);
        assertEquals(100, info.mHighestIndex);
        assertEquals(50, info.mCommitIndex);

        writer = mLog.openWriter(1, 2, 500);
        write(writer, new byte[100]);
        writer.release();

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(1, info.mTerm);
        assertEquals(100, info.mHighestIndex);
        assertEquals(50, info.mCommitIndex);

        mLog.commit(800);

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(1, info.mTerm);
        assertEquals(100, info.mHighestIndex);
        assertEquals(100, info.mCommitIndex);

        writer = mLog.openWriter(1, 1, 100);
        write(writer, new byte[300]);
        writer.release();

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(1, info.mTerm);
        assertEquals(400, info.mHighestIndex);
        assertEquals(400, info.mCommitIndex);

        writer = mLog.openWriter(1, 1, 400);
        write(writer, new byte[100]);
        writer.release();

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(2, info.mTerm);
        assertEquals(600, info.mHighestIndex);
        assertEquals(600, info.mCommitIndex);

        writer = mLog.openWriter(2, 2, 600);
        write(writer, new byte[100]);
        writer.release();

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(2, info.mTerm);
        assertEquals(700, info.mHighestIndex);
        assertEquals(700, info.mCommitIndex);

        writer = mLog.openWriter(2, 2, 700);
        write(writer, new byte[200]);
        writer.release();

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(2, info.mTerm);
        assertEquals(900, info.mHighestIndex);
        assertEquals(800, info.mCommitIndex);

        writer = mLog.openWriter(2, 2, 900);
        write(writer, new byte[50]);
        writer.release();
        mLog.commit(950);

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(2, info.mTerm);
        assertEquals(950, info.mHighestIndex);
        assertEquals(950, info.mCommitIndex);

        writer = mLog.openWriter(2, 2, 950);
        write(writer, new byte[50]);
        writer.release();
        mLog.commit(1000);

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(2, info.mTerm);
        assertEquals(1000, info.mHighestIndex);
        assertEquals(1000, info.mCommitIndex);
    }

    @Test
    public void currentTerm() throws Exception {
        assertEquals(0, mLog.checkCurrentTerm(-1));
        assertEquals(0, mLog.checkCurrentTerm(0));
        assertEquals(1, mLog.checkCurrentTerm(1));

        mLog.close();
        mLog = new FileStateLog(mBase);

        assertEquals(1, mLog.checkCurrentTerm(0));
        assertEquals(5, mLog.incrementCurrentTerm(4));

        mLog.close();
        mLog = new FileStateLog(mBase);

        assertEquals(5, mLog.checkCurrentTerm(0));

        try {
            mLog.incrementCurrentTerm(0);
            fail();
        } catch (IllegalArgumentException e) {
        }
    }

    @Test
    public void recoverState() throws Exception {
        Random rnd = new Random(7435847);

        long prevTerm = mLog.checkCurrentTerm(0);
        long term = mLog.incrementCurrentTerm(1);
        LogWriter writer = mLog.openWriter(prevTerm, term, 0);
        byte[] msg1 = new byte[1000];
        rnd.nextBytes(msg1);
        write(writer, msg1);
        writer.release();

        prevTerm = mLog.checkCurrentTerm(0);
        term = mLog.incrementCurrentTerm(1);
        writer = mLog.openWriter(prevTerm, term, 1000);
        byte[] msg2 = new byte[2000];
        rnd.nextBytes(msg2);
        write(writer, msg2);
        writer.release();

        // Reopen without sync. All data for second term is gone, but the first term remains.
        mLog.close();
        mLog = new FileStateLog(mBase);

        LogInfo info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(prevTerm, info.mTerm);
        assertEquals(1000, info.mHighestIndex);
        assertEquals(0, info.mCommitIndex);

        // Last term must always be opened without an end index.
        verifyLog(mLog, 0, msg1, 0);

        term = mLog.incrementCurrentTerm(1);
        writer = mLog.openWriter(prevTerm, term, 1000);
        byte[] msg3 = new byte[3500];
        rnd.nextBytes(msg3);
        write(writer, msg3);
        writer.release();

        // Reopen with sync. All data is preserved, but not committed.
        mLog.sync();
        mLog.close();
        mLog = new FileStateLog(mBase);

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(term, info.mTerm);
        assertEquals(4500, info.mHighestIndex);
        assertEquals(0, info.mCommitIndex);

        // Partially commit and reopen.
        mLog.commit(4000);
        assertEquals(4000, mLog.syncCommit(term, term, 4000));
        mLog.commitDurable(4000);
        mLog.close();
        mLog = new FileStateLog(mBase);

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(term, info.mTerm);
        assertEquals(4500, info.mHighestIndex);
        assertEquals(4000, info.mCommitIndex);

        // Verify data.
        verifyLog(mLog, 0, msg1, -1); // end of term
        verifyLog(mLog, 1000, msg3, 0);

        // Cannot read past commit.

        Reader reader = TestUtils.startAndWaitUntilBlocked(new Reader(0));
        assertEquals(4000, reader.mTotal);

        reader.interrupt();
        reader.join();
    }

    class Reader extends Thread {
        final long mStartIndex;
        volatile long mTotal;

        Reader(long start) {
            mStartIndex = start;
        }

        @Override
        public void run() {
            try {
                LogReader reader = mLog.openReader(mStartIndex);
                byte[] buf = new byte[1000];
                while (true) {
                    int amt = reader.read(buf, 0, buf.length);
                    if (amt < 0) {
                        reader = mLog.openReader(reader.index());
                    } else {
                        mTotal += amt;
                    }
                }
            } catch (InterruptedIOException e) {
                // Stop.
            } catch (Throwable e) {
                Utils.uncaught(e);
            }
        }
    }

    @Test
    public void recoverState2() throws Exception {
        // Write data over multiple segments.

        long seed = 1334535;
        Random rnd = new Random(seed);
        byte[] buf = new byte[1000];

        long prevTerm = mLog.checkCurrentTerm(0);
        long term = mLog.incrementCurrentTerm(1);
        LogWriter writer = mLog.openWriter(prevTerm, term, 0);

        for (int i=0; i<10_000; i++) {
            rnd.nextBytes(buf);
            write(writer, buf);
        }
        writer.release();

        prevTerm = mLog.checkCurrentTerm(0);
        term = mLog.incrementCurrentTerm(1);
        writer = mLog.openWriter(prevTerm, term, writer.index());

        for (int i=0; i<5_000; i++) {
            rnd.nextBytes(buf);
            write(writer, buf);
        }
        writer.release();

        final long highestIndex = writer.index();

        mLog.commit(highestIndex);
        mLog.sync();

        // Generate data over more terms, none of which will be committed.

        for (int x=0; x<2; x++) {
            prevTerm = mLog.checkCurrentTerm(0);
            term = mLog.incrementCurrentTerm(1);
            writer = mLog.openWriter(prevTerm, term, writer.index());
            for (int i=0; i<5_000; i++) {
                rnd.nextBytes(buf);
                writer.write(buf, 0, buf.length, 0); // don't advance highest index
            }
            writer.release();
        }

        mLog.close();
        mLog = new FileStateLog(mBase);

        LogInfo info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(prevTerm, info.mTerm);
        assertEquals(highestIndex, info.mHighestIndex);
        // Didn't call syncCommit.
        assertEquals(0, info.mCommitIndex);

        mLog.commit(highestIndex);

        // Verify data.

        rnd = new Random(seed);
        byte[] buf2 = new byte[buf.length];

        LogReader r = mLog.openReader(0);
        for (int i=0; i<10_000; i++) {
            rnd.nextBytes(buf);
            readFully(r, buf2);
            TestUtils.fastAssertArrayEquals(buf, buf2);
        }

        assertEquals(-1, r.read(buf2, 0, 1)); // end of term

        r = mLog.openReader(r.index());
        for (int i=0; i<5_000; i++) {
            rnd.nextBytes(buf);
            readFully(r, buf2);
            TestUtils.fastAssertArrayEquals(buf, buf2);
        }

        assertEquals(-1, r.read(buf2, 0, 1)); // end of term

        Reader reader = TestUtils.startAndWaitUntilBlocked(new Reader(r.index()));
        assertEquals(0, reader.mTotal);

        reader.interrupt();
        reader.join();
    }

    @Test
    public void raftFig7() throws Exception {
        // Tests the scenarios shown in figure 7 of the Raft paper.

        // Leader.
        writeTerm(mLog,  0,  '1', 1 - 1, 3);
        writeTerm(mLog, '1', '4', 4 - 1, 2);
        writeTerm(mLog, '4', '5', 6 - 1, 2);
        writeTerm(mLog, '5', '6', 8 - 1, 3);
        verifyLog(mLog, 0, "1114455666".getBytes(), 0);
        writeTerm(mLog, '6', '8', 11 - 1, 1);
        verifyLog(mLog, 0, "11144556668".getBytes(), 0);

        StateLog logA = newTempLog();
        writeTerm(logA,  0,  '1', 1 - 1, 3);
        writeTerm(logA, '1', '4', 4 - 1, 2);
        writeTerm(logA, '4', '5', 6 - 1, 2);
        writeTerm(logA, '5', '6', 8 - 1, 2);
        verifyLog(logA, 0, "111445566".getBytes(), 0);

        replicate(mLog, 11 - 1, logA);
        verifyLog(logA, 0, "11144556668".getBytes(), 0);

        StateLog logB = newTempLog();
        writeTerm(logB,  0,  '1', 1 - 1, 3);
        writeTerm(logB, '1', '4', 4 - 1, 1);
        verifyLog(logB, 0, "1114".getBytes(), 0);

        replicate(mLog, 11 - 1, logB);
        verifyLog(logB, 0, "11144556668".getBytes(), 0);

        StateLog logC = newTempLog();
        writeTerm(logC,  0,  '1', 1 - 1, 3);
        writeTerm(logC, '1', '4', 4 - 1, 2);
        writeTerm(logC, '4', '5', 6 - 1, 2);
        writeTerm(logC, '5', '6', 8 - 1, 4);
        verifyLog(logC, 0, "11144556666".getBytes(), 0);

        replicate(mLog, 11 - 1, logC);
        verifyLog(logC, 0, "11144556668".getBytes(), 0);

        StateLog logD = newTempLog();
        writeTerm(logD,  0,  '1', 1 - 1, 3);
        writeTerm(logD, '1', '4', 4 - 1, 2);
        writeTerm(logD, '4', '5', 6 - 1, 2);
        writeTerm(logD, '5', '6', 8 - 1, 3);
        writeTerm(logD, '6', '7', 11 - 1, 2);
        verifyLog(logD, 0, "111445566677".getBytes(), 0);

        replicate(mLog, 11 - 1, logD);
        verifyLog(logD, 0, "11144556668".getBytes(), 0);

        StateLog logE = newTempLog();
        writeTerm(logE,  0,  '1', 1 - 1, 3);
        writeTerm(logE, '1', '4', 4 - 1, 4);
        verifyLog(logE, 0, "1114444".getBytes(), 0);

        replicate(mLog, 11 - 1, logE);
        verifyLog(logE, 0, "11144556668".getBytes(), 0);

        StateLog logF = newTempLog();
        writeTerm(logF,  0,  '1', 1 - 1, 3);
        writeTerm(logF, '1', '2', 4 - 1, 3);
        writeTerm(logF, '2', '3', 7 - 1, 5);
        verifyLog(logF, 0, "11122233333".getBytes(), 0);

        replicate(mLog, 11 - 1, logF);
        verifyLog(logF, 0, "11144556668".getBytes(), 0);
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
            reader = from.openReader(index);
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
                reader = from.openReader(index);
                writer = to.openWriter(reader.prevTerm(), reader.term(), index);
            } else {
                write(writer, buf, 0, amt);
                index += amt;
            }
        }
    }

    private static void verifyLog(StateLog log, int index, byte[] expect, int finalAmt)
        throws IOException
    {
        LogReader reader = log.openReader(index);

        byte[] buf = new byte[expect.length];
        int offset = 0;

        while (offset < buf.length) {
            int amt = reader.readAny(buf, offset, buf.length - offset);
            if (amt <= 0) {
                reader.release();
                if (amt == 0) {
                    fail("nothing read");
                }
                reader = log.openReader(index);
            } else {
                index += amt;
                offset += amt;
            }
        }

        TestUtils.fastAssertArrayEquals(expect, buf);

        assertEquals(finalAmt, reader.readAny(buf, 0, 1));
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

    static void readFully(StreamReplicator.Reader reader, byte[] buf) throws IOException {
        int off = 0;
        int len = buf.length;
        while (len > 0) {
            int amt = reader.read(buf, off, len);
            if (amt < 0) {
                throw new EOFException();
            }
            off += amt;
            len -= amt;
        }
    }
}
