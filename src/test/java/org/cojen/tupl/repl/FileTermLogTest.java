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
import java.io.FileOutputStream;
import java.io.IOException;

import java.nio.channels.ClosedByInterruptException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicReference;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LatchCondition;
import org.cojen.tupl.util.Worker;

import org.cojen.tupl.TestUtils;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class FileTermLogTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(FileTermLogTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
        mBase = TestUtils.newTempBaseFile(getClass());
        mWorker = Worker.make(1, 15, TimeUnit.SECONDS, null);
        mLog = FileTermLog.openTerm(mWorker, mBase, 0, 1, 0, 0, 0, null);
    }

    @After
    public void teardown() throws Exception {
        if (mLog != null) {
            mLog.close();
        }

        TestUtils.deleteTempFiles(getClass());

        if (mWorker != null) {
            mWorker.join(true);
        }
    }

    private File mBase;
    private Worker mWorker;
    private TermLog mLog;

    @Test
    public void basic() throws Exception {
        basic(283742, false);
    }

    @Test
    public void basicReopen() throws Exception {
        basic(3209441, true);
    }

    private void basic(final long seed, boolean reopen) throws Exception {
        assertEquals(Long.MAX_VALUE, mLog.endIndex());

        // Write a bunch of data and read it back.

        final byte[] buf = new byte[10000];
        Random rnd = new Random(seed);
        Random rnd2 = new Random(seed + 1);
        LogWriter writer = mLog.openWriter(0);
        assertEquals(0, writer.prevTerm());
        assertEquals(1, writer.term());
        long index = 0;

        for (int i=0; i<100000; i++) {
            assertEquals(index, writer.index());
            int len = rnd.nextInt(buf.length);
            for (int j=0; j<len; j++) {
                buf[j] = (byte) rnd2.nextInt();
            }
            assertEquals(len, writer.write(buf, 0, len, index + len));
            index += len;
            LogInfo info = new LogInfo();
            mLog.captureHighest(info);
            assertEquals(index, info.mHighestIndex);
        }

        writer.release();

        if (reopen) {
            LogInfo info = new LogInfo();
            mLog.captureHighest(info);
            mLog.sync();
            mLog.close();
            mLog = FileTermLog.openTerm(mWorker, mBase, 0, 1, 0, 0, info.mHighestIndex, null);
        }

        rnd = new Random(seed);
        rnd2 = new Random(seed + 1);
        LogReader reader = mLog.openReader(0);
        assertEquals(0, reader.prevTerm());
        assertEquals(1, reader.term());
        long total = index;
        index = 0;

        while (true) {
            assertEquals(index, reader.index());
            int amt = reader.tryReadAny(buf, 0, buf.length);
            if (amt <= 0) {
                assertTrue(amt == 0);
                break;
            }
            for (int j=0; j<amt; j++) {
                assertEquals((byte) rnd2.nextInt(), buf[j]);
            }
            index += amt;
        }

        reader.release();
        assertEquals(total, index);

        mLog.finishTerm(index);
        assertEquals(index, mLog.endIndex());

        mLog.finishTerm(index);
        assertEquals(index, mLog.endIndex());

        /* Extending the term is allowed, because conflicting empty terms can be removed.
        try {
            mLog.finishTerm(index + 1);
            fail();
        } catch (IllegalStateException e) {
            // Expected.
        }
        */

        // Cannot write past end.
        if (reopen) {
            try {
                writer.write(buf, 0, 1, 9_999_999_999L);
                fail();
            } catch (IOException e) {
                // Closed.
            }
            writer = mLog.openWriter(writer.index());
        }
        assertEquals(0, writer.write(buf, 0, 1, 9_999_999_999L));
        writer.release();

        assertEquals(index, mLog.endIndex());

        LogInfo info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(index, info.mHighestIndex);

        // Permit partial write up to the end.
        writer = mLog.openWriter(index - 1);
        assertEquals(1, writer.write(buf, 0, 2, 9_999_999_999L));
        writer.release();
        
        assertEquals(index, mLog.endIndex());

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(index, info.mHighestIndex);
    }

    @Test
    public void reopenCleanup() throws Exception {
        reopenCleanup(false);
    }

    @Test
    public void reopenCleanupDiscover() throws Exception {
        reopenCleanup(true);
    }

    private void reopenCleanup(boolean discoverStart) throws Exception {
        mLog.close();
        final long prevTerm = 0;
        final long term = 1;
        final long startIndex = 1000;
        mLog = FileTermLog.openTerm
            (mWorker, mBase, prevTerm, term, startIndex, startIndex, startIndex, null);

        final byte[] buf = new byte[1000];
        Random rnd = new Random(62723);
        Random rnd2 = new Random(8675309);
        LogWriter writer = mLog.openWriter(startIndex);
        long index = startIndex;

        for (int i=0; i<10_000; i++) {
            int len = rnd.nextInt(buf.length);
            for (int j=0; j<len; j++) {
                buf[j] = (byte) rnd2.nextInt();
            }
            assertEquals(len, writer.write(buf, 0, len, index + len));
            index += len;
        }

        writer.release();

        final String basePath = mBase.getPath() + '.';

        // Create some files that should be ignored.
        new File(basePath).createNewFile();
        new File(basePath + term).createNewFile();
        TestUtils.newTempBaseFile(getClass()).createNewFile();
        new File(basePath + "foo").createNewFile();
        new File(basePath + ".foo").createNewFile();
        new File(basePath + ".123foo").createNewFile();
        new File(basePath + "..123.foo").createNewFile();
        new File(basePath + term + "foo").createNewFile();
        new File(basePath + term + ".foo").createNewFile();
        new File(basePath + term + ".123foo").createNewFile();
        new File(basePath + '.' + term + ".foo").createNewFile();

        // Create some out-of-bounds segments that should be deleted.
        File low = new File(basePath + term + ".123." + prevTerm);
        low.createNewFile();
        assertTrue(low.exists());
        File high = new File(basePath + term + ".999999999999");
        high.createNewFile();
        assertTrue(high.exists());

        // Expand a segment that should be truncated.
        File first = new File(basePath + term + ".1000." + prevTerm);
        assertTrue(first.exists());
        long firstLen = first.length();
        assertTrue(firstLen > 1_000_000);
        try (FileOutputStream out = new FileOutputStream(first, true)) {
            out.write("hello".getBytes());
        }
        assertEquals(firstLen + 5, first.length());

        // Close and reopen.
        LogInfo info = new LogInfo();
        mLog.captureHighest(info);
        mLog.close();

        final long startWith;
        if (!discoverStart) {
            startWith = startIndex;
        } else {
            try {
                FileTermLog.openTerm
                    (mWorker, mBase, -1, term, -1, startIndex, info.mHighestIndex, null);
                fail();
            } catch (Exception e) {
                assertTrue(e.getMessage().indexOf(low.toString()) >= 0);
            }

            low.delete();
            low = null;

            startWith = -1;
        }

        try {
            mLog = FileTermLog.openTerm
                (mWorker, mBase, 12, term, startWith, startIndex, info.mHighestIndex, null);
            fail();
        } catch (IllegalStateException e) {
            // Mismatched previous term.
        }

        mLog = FileTermLog.openTerm
            (mWorker, mBase, -1, term, startWith, startIndex, info.mHighestIndex, null);

        if (low != null) {
            assertTrue(!low.exists());
        }
        assertTrue(!high.exists());
        assertEquals(firstLen, first.length());

        // Verify the data.

        rnd2 = new Random(8675309);
        LogReader reader = mLog.openReader(startIndex);
        assertEquals(0, reader.prevTerm());
        assertEquals(1, reader.term());
        long total = index;
        index = startIndex;

        while (true) {
            assertEquals(index, reader.index());
            int amt = reader.tryReadAny(buf, 0, buf.length);
            if (amt <= 0) {
                assertTrue(amt == 0);
                break;
            }
            for (int j=0; j<amt; j++) {
                assertEquals((byte) rnd2.nextInt(), buf[j]);
            }
            index += amt;
        }

        reader.release();
        assertEquals(total, index);

        // Reopen with missing segments.
        mLog.close();

        if (!discoverStart) {
            first.delete();
        } else {
            teardown();
        }

        try {
            FileTermLog.openTerm
                (mWorker, mBase, prevTerm, term, startWith, startIndex, info.mHighestIndex, null);
            fail();
        } catch (Exception e) {
            String msg = e.getMessage();
            String expect;
            if (!discoverStart) {
                expect = "Missing start";
            } else {
                expect = "No segment files";
            }
            assertTrue(expect, msg.indexOf(expect) >= 0);
        }
    }

    @Test
    public void tail() throws Throwable {
        tail(false);
    }

    @Test
    public void tailAndCompact() throws Throwable {
        tail(true);
    }

    private void tail(boolean compact) throws Throwable {
        // Write and commit a bunch of data, while concurrently reading it.

        final int seed = 762390;

        class Reader extends Thread {
            volatile Throwable mEx;
            volatile long mTotal;

            @Override
            public void run() {
                try {
                    byte[] buf = new byte[1024];
                    Random rnd2 = new Random(seed + 1);
                    LogReader reader = mLog.openReader(0);

                    while (true) {
                        int amt = reader.read(buf, 0, buf.length);
                        if (amt <= 0) {
                            assertTrue(amt < 0);
                            break;
                        }
                        for (int j=0; j<amt; j++) {
                            assertEquals((byte) rnd2.nextInt(), buf[j]);
                        }
                        if (compact) {
                            mLog.compact(reader.index());
                        }
                    }

                    mTotal = reader.index();
                } catch (Throwable e) {
                    mEx = e;
                }
            }
        }

        final Reader r = new Reader();
        TestUtils.startAndWaitUntilBlocked(r);

        final byte[] buf = new byte[10000];
        Random rnd = new Random(seed);
        Random rnd2 = new Random(seed + 1);
        LogWriter writer = mLog.openWriter(0);
        assertEquals(0, writer.prevTerm());
        assertEquals(1, writer.term());
        long index = 0;

        for (int i=0; i<100000; i++) {
            assertEquals(index, writer.index());
            int len = rnd.nextInt(buf.length);
            for (int j=0; j<len; j++) {
                buf[j] = (byte) rnd2.nextInt();
            }
            assertEquals(len, writer.write(buf, 0, len, index + len));
            index += len;
            LogInfo info = new LogInfo();
            mLog.captureHighest(info);
            assertEquals(index, info.mHighestIndex);

            long commitIndex = index + (rnd.nextInt(1000) - 500);
            if (commitIndex >= 0 && i < (100000 - 1)) {
                mLog.commit(commitIndex);
            }
        }

        writer.release();
        mLog.commit(index);

        LogInfo info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(index, info.mHighestIndex);
        assertEquals(index, info.mCommitIndex);

        mLog.finishTerm(index);
        assertEquals(index, mLog.endIndex());
        r.join();

        Throwable ex = r.mEx;
        if (ex != null) {
            throw ex;
        }

        assertEquals(index, r.mTotal);
    }

    @Test
    public void waitForCommit() throws Exception {
        commitWait(false);
    }

    @Test
    public void uponCommit() throws Exception {
        commitWait(true);
    }

    private void commitWait(boolean upon) throws Exception {
        // Test waiting for a commit.

        class Waiter extends Thread {
            final long mWaitFor;
            final boolean mUpon;
            final Latch mLatch;
            final LatchCondition mLatchCondition;
            Exception mEx;
            long mCommit;

            Waiter(long waitFor, boolean upon) {
                mLatch = new Latch();
                mUpon = upon;
                mLatchCondition = new LatchCondition();
                mWaitFor = waitFor;
            }

            void begin() {
                if (mUpon) {
                    mLog.uponCommit(new Delayed(mWaitFor) {
                        @Override
                        protected void doRun(long commit) {
                            mLatch.acquireExclusive();
                            mCommit = commit;
                            mLatchCondition.signal();
                            mLatch.releaseExclusive();
                        }
                    });
                } else {
                    TestUtils.startAndWaitUntilBlocked(this);
                }
            }

            @Override
            public void run() {
                try {
                    long commit = mLog.waitForCommit(mWaitFor, -1);
                    mLatch.acquireExclusive();
                    mCommit = commit;
                    mLatchCondition.signal();
                    mLatch.releaseExclusive();
                } catch (Exception e) {
                    mLatch.acquireExclusive();
                    mEx = e;
                    mLatchCondition.signal();
                    mLatch.releaseExclusive();
                }
            }

            long waitForResult(long timeoutMillis) throws Exception { 
                mLatch.acquireExclusive();
                try {
                    int r = Integer.MAX_VALUE;
                    while (true) {
                        if (mCommit != 0) {
                            return mCommit;
                        }
                        if (mEx != null) {
                            throw mEx;
                        }
                        if (r != Integer.MAX_VALUE) {
                            return -1;
                        }
                        r = mLatchCondition.await(mLatch, timeoutMillis, TimeUnit.MILLISECONDS);
                    }
                } finally {
                    mLatch.releaseExclusive();
                }
            }
        }

        // ------
        long index = 0;
        byte[] msg1 = "hello".getBytes();

        // Wait for the full message.
        Waiter waiter = new Waiter(index + msg1.length, upon);
        waiter.begin();

        LogWriter writer = mLog.openWriter(0);
        write(writer, msg1);

        // Timed out waiting, because noting has been committed yet.
        assertEquals(-1, waiter.waitForResult(500));

        // Commit too little.
        mLog.commit(index += 2);
        assertEquals(-1, waiter.waitForResult(500));

        // Commit the rest.
        mLog.commit(index += 3);
        assertEquals(index, waiter.waitForResult(-1));

        // ------
        byte[] msg2 = "world!!!".getBytes();

        // Wait for a partial message.
        waiter = new Waiter(index + 5, upon);
        waiter.begin();

        write(writer, msg2);

        // Timed out waiting.
        assertEquals(-1, waiter.waitForResult(500));

        // Commit too little.
        mLog.commit(index += 2);
        assertEquals(-1, waiter.waitForResult(500));

        // Commit the rest, observing more than what was requested.
        mLog.commit(index += 6);
        assertEquals(index, waiter.waitForResult(-1));

        // ------
        byte[] msg3 = "stuff".getBytes();

        // Wait for the full message.
        waiter = new Waiter(index + msg3.length, upon);
        waiter.begin();

        // Commit ahead.
        mLog.commit(index + 100);

        // Timed out waiting, because nothing has been written yet.
        assertEquals(-1, waiter.waitForResult(500));

        write(writer, msg3);
        index += msg3.length;
        assertEquals(index, waiter.waitForResult(-1));

        writer.release();

        // ------
        // No wait.
        waiter = new Waiter(index, upon);
        waiter.begin();
        assertEquals(index, waiter.waitForResult(-1));

        // ------
        // Unblock after term is finished.
        long findex = index;
        TestUtils.startAndWaitUntilBlocked(new Thread(() -> {
            TestUtils.sleep(1000);
            try {
                LogWriter w = mLog.openWriter(findex);
                int rem = 100 - msg3.length;
                byte[] b = new byte[(int) (findex + rem)]; 
                for (int i=0; i<b.length; i++) {
                    b[i] = (byte) (i + 1);
                }
                write(w, b);
                mLog.finishTerm(findex + rem);
            } catch (IOException e) {
                Utils.uncaught(e);
            }
        }));

        waiter = new Waiter(index + 101, upon);
        waiter.begin();
        assertEquals(-1, waiter.waitForResult(-1));

        // Verify that everything is read back properly, with no blocking.
        byte[] complete = concat(msg1, msg2, msg3);
        byte[] buf = new byte[complete.length + 100 - msg3.length];
        LogReader reader = mLog.openReader(0);
        int amt = reader.read(buf, 0, buf.length);
        assertEquals(buf.length, amt);
        reader.release();
        TestUtils.fastAssertArrayEquals(complete, Arrays.copyOf(buf, complete.length));
        for (int i=0; i<(100 - msg3.length); i++) {
            assertEquals((byte) (i + 1), buf[i + complete.length]);
        }
    }

    @Test
    public void rangesNoSync() throws Exception {
        ranges(false);
    }

    @Test
    public void rangesWithSync() throws Exception {
        ranges(true);
    }

    private void ranges(boolean sync) throws Exception {
        // Define a bunch of random ranges, with random data, and write to them in random order
        // via several threads. The reader shouldn't read beyond the contiguous range, and the
        // data it observes should match what was written.

        final long seed = 289023475245L;
        Random rnd = new Random(seed);

        final int threadCount = 10;
        final int sliceLength = 100_000;
        Range[] ranges = new Range[sliceLength * threadCount];

        long index = 0;
        for (int i=0; i<ranges.length; i++) {
            Range range = new Range();
            range.mStart = index;
            int len = rnd.nextInt(1000);
            index += len;
            range.mEnd = index;
            ranges[i] = range;
        }

        // Commit and finish in advance.
        mLog.commit(index);
        mLog.finishTerm(index);

        Collections.shuffle(Arrays.asList(ranges), rnd);

        Range[][] slices = new Range[threadCount][];
        for (int i=0; i<slices.length; i++) {
            int from = i * sliceLength;
            slices[i] = Arrays.copyOfRange(ranges, from, from + sliceLength);
        }

        class Writer extends Thread {
            private final Range[] mRanges;
            private final Random mRnd;
            volatile long mSum;

            Writer(Range[] ranges, Random rnd) {
                mRanges = ranges;
                mRnd = rnd;
            }

            @Override
            public void run() {
                try {
                    long sum = 0;

                    for (Range range : mRanges) {
                        byte[] data = new byte[(int) (range.mEnd - range.mStart)];
                        mRnd.nextBytes(data);
                        LogWriter writer = mLog.openWriter(range.mStart);
                        write(writer, data);
                        writer.release();
                        for (int i=0; i<data.length; i++) {
                            sum += 1 + (data[i] & 0xff);
                        }
                    }

                    mSum = sum;
                } catch (IOException e) {
                    Utils.uncaught(e);
                }
            }
        }

        Thread syncThread = null;

        if (sync) {
            // First one should do nothing.
            mLog.sync();

            syncThread = new Thread(() -> {
                try {
                    while (true) {
                        Thread.sleep(500);
                        mLog.sync();
                    }
                } catch (InterruptedException | ClosedByInterruptException e) {
                    // Done.
                } catch (IOException e) {
                    Utils.uncaught(e);
                }
            });

            syncThread.start();
        }

        Writer[] writers = new Writer[threadCount];
        for (int i=0; i<writers.length; i++) {
            writers[i] = new Writer(slices[i], new Random(seed + i));
        }

        for (Writer w : writers) {
            w.start();
        }

        LogReader reader = mLog.openReader(0);
        byte[] buf = new byte[1024];
        long sum = 0;

        while (true) {
            int amt = reader.read(buf, 0, buf.length);
            if (amt < 0) {
                break;
            }
            for (int i=0; i<amt; i++) {
                sum += 1 + (buf[i] & 0xff);
            }
        }

        assertEquals(index, reader.index());

        reader.release();

        long expectedSum = 0;

        for (Writer w : writers) {
            w.join();
            expectedSum += w.mSum;
        }

        assertEquals(expectedSum, sum);

        if (syncThread != null) {
            syncThread.interrupt();
            syncThread.join();
        }
    }

    @Test
    public void missingRanges() throws Exception {
        // Verify that missing ranges can be queried.

        RangeResult result = new RangeResult();
        assertEquals(0, mLog.checkForMissingData(Long.MAX_VALUE, result));
        assertEquals(0, result.mRanges.size());

        LogWriter writer = mLog.openWriter(50);
        write(writer, new byte[100]);
        writer.release();

        result = new RangeResult();
        assertEquals(0, mLog.checkForMissingData(0, result));
        assertEquals(1, result.mRanges.size());
        assertEquals(new Range(0, 50), result.mRanges.get(0));

        // Fill in the missing range. Overlap is fine.
        writer = mLog.openWriter(0);
        write(writer, new byte[55]);
        writer.release();

        result = new RangeResult();
        assertEquals(150, mLog.checkForMissingData(0, result));
        assertEquals(0, result.mRanges.size());
        assertEquals(150, mLog.checkForMissingData(10, result));
        assertEquals(0, result.mRanges.size());
        assertEquals(150, mLog.checkForMissingData(1000, result));
        assertEquals(0, result.mRanges.size());

        // Create a few missing ranges.
        writer = mLog.openWriter(200);
        write(writer, new byte[50]);
        writer.release();
        writer = mLog.openWriter(350);
        write(writer, new byte[50]);
        writer.release();
        writer = mLog.openWriter(300);
        write(writer, new byte[50]);
        writer.release();

        result = new RangeResult();
        assertEquals(150, mLog.checkForMissingData(100, result));
        assertEquals(0, result.mRanges.size());
        assertEquals(150, mLog.checkForMissingData(150, result));
        assertEquals(2, result.mRanges.size());
        assertEquals(new Range(150, 200), result.mRanges.get(0));
        assertEquals(new Range(250, 300), result.mRanges.get(1));

        // Finish the term, creating a new missing range.
        mLog.finishTerm(1000);
        result = new RangeResult();
        assertEquals(150, mLog.checkForMissingData(150, result));
        assertEquals(3, result.mRanges.size());
        assertEquals(new Range(150, 200), result.mRanges.get(0));
        assertEquals(new Range(250, 300), result.mRanges.get(1));
        assertEquals(new Range(400, 1000), result.mRanges.get(2));

        // Fill in the missing ranges...
        writer = mLog.openWriter(150);
        write(writer, new byte[50]);
        writer.release();

        result = new RangeResult();
        assertEquals(250, mLog.checkForMissingData(150, result));
        assertEquals(0, result.mRanges.size());
        assertEquals(250, mLog.checkForMissingData(250, result));
        assertEquals(2, result.mRanges.size());
        assertEquals(new Range(250, 300), result.mRanges.get(0));
        assertEquals(new Range(400, 1000), result.mRanges.get(1));

        writer = mLog.openWriter(400);
        write(writer, new byte[600]);
        writer.release();

        result = new RangeResult();
        assertEquals(250, mLog.checkForMissingData(250, result));
        assertEquals(1, result.mRanges.size());
        assertEquals(new Range(250, 300), result.mRanges.get(0));

        LogInfo info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(250, info.mHighestIndex);

        writer = mLog.openWriter(250);
        write(writer, new byte[50]);
        writer.release();

        result = new RangeResult();
        assertEquals(1000, mLog.checkForMissingData(250, result));
        assertEquals(0, result.mRanges.size());
        assertEquals(1000, mLog.checkForMissingData(1000, result));
        assertEquals(0, result.mRanges.size());

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(1000, info.mHighestIndex);
    }

    @Test
    public void discardNonContigRanges() throws Exception {
        // Any non-contiguous ranges past the end of a finished term must be discarded.

        LogWriter writer = mLog.openWriter(100);
        write(writer, new byte[50]);
        writer.release();

        writer = mLog.openWriter(250);
        write(writer, new byte[50]);
        writer.release();

        writer = mLog.openWriter(400);
        write(writer, new byte[100]);
        writer.release();

        RangeResult result = new RangeResult();
        assertEquals(0, mLog.checkForMissingData(Long.MAX_VALUE, result));
        assertEquals(0, result.mRanges.size());

        result = new RangeResult();
        assertEquals(0, mLog.checkForMissingData(-1, result));
        assertEquals(3, result.mRanges.size());
        assertEquals(new Range(0, 100), result.mRanges.get(0));
        assertEquals(new Range(150, 250), result.mRanges.get(1));
        assertEquals(new Range(300, 400), result.mRanges.get(2));

        mLog.finishTerm(200);

        result = new RangeResult();
        assertEquals(0, mLog.checkForMissingData(0, result));
        assertEquals(2, result.mRanges.size());
        assertEquals(new Range(0, 100), result.mRanges.get(0));
        assertEquals(new Range(150, 200), result.mRanges.get(1));

        LogInfo info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(0, info.mHighestIndex);

        writer = mLog.openWriter(0);
        write(writer, new byte[100]);
        writer.release();

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(150, info.mHighestIndex);

        result = new RangeResult();
        assertEquals(150, mLog.checkForMissingData(0, result));
        assertEquals(0, result.mRanges.size());
        assertEquals(150, mLog.checkForMissingData(150, result));
        assertEquals(1, result.mRanges.size());
        assertEquals(new Range(150, 200), result.mRanges.get(0));
    }

    @Test
    public void discardAllRanges() throws Exception {
        // All ranges past the end of a finished term must be discarded.

        LogWriter writer = mLog.openWriter(0);
        write(writer, new byte[200]);
        writer.release();

        writer = mLog.openWriter(300);
        write(writer, new byte[100]);
        writer.release();

        RangeResult result = new RangeResult();
        assertEquals(200, mLog.checkForMissingData(Long.MAX_VALUE, result));
        assertEquals(0, result.mRanges.size());
        assertEquals(200, mLog.checkForMissingData(200, result));
        assertEquals(1, result.mRanges.size());
        assertEquals(new Range(200, 300), result.mRanges.get(0));

        LogInfo info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(200, info.mHighestIndex);

        mLog.finishTerm(170);

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(170, info.mHighestIndex);
        assertEquals(0, info.mCommitIndex);

        result = new RangeResult();
        assertEquals(170, mLog.checkForMissingData(200, result));
        assertEquals(0, result.mRanges.size());

        mLog.commit(170);

        info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(170, info.mHighestIndex);
        assertEquals(170, info.mCommitIndex);

        try {
            mLog.finishTerm(100);
            fail();
        } catch (IllegalStateException e) {
            // Expected.
        }
    }

    @Test
    public void clampHighest() throws Exception {
        // Verify that the highest index cannot be set higher than the contiguous index.

        LogWriter writer = mLog.openWriter(0);
        write(writer, new byte[100]);

        LogInfo info = new LogInfo();
        mLog.captureHighest(info);
        assertEquals(100, info.mHighestIndex);

        write(writer, new byte[100], 250);
        mLog.captureHighest(info);
        assertEquals(100, info.mHighestIndex);

        write(writer, new byte[100], 250);
        mLog.captureHighest(info);
        assertEquals(250, info.mHighestIndex);
    }

    @Test
    public void compactionZone() throws Exception {
        // Reads from the compaction zone should fail, and writes into the compaction zone
        // should be ignored.

        LogWriter writer = mLog.openWriter(0);
        byte[] b = new byte[10000];
        for (int i=0; i<1000; i++) {
            write(writer, b);
        }
        writer.release();

        long commitIndex = 1_500_000;
        mLog.commit(commitIndex);
        mLog.compact(commitIndex);

        LogReader reader = mLog.openReader(0);
        try {
            int amt = reader.read(b);
            System.out.println(amt);
            fail();
        } catch (IllegalStateException e) {
            // Too low.
        }

        reader.release();

        writer = mLog.openWriter(0);
        assertEquals(0, writer.write(b));
        writer.release();

        // Some segment overlap. Although the higher portion of the write could be written, the
        // returned amount from the write wouldn't make sense. Ditch it all.

        writer = mLog.openWriter(1024 * 1024 - 100);
        assertEquals(0, writer.write(b));
        writer.release();

        // Try again with writers that reference deleted segements.

        writer = mLog.openWriter(commitIndex);
        for (int i=0; i<1000; i++) {
            write(writer, b);
        }
        writer.release();

        // Re-open at the start index.
        writer = mLog.openWriter(commitIndex);
        // Force segement to be referenced.
        writer.write(b);

        long commitIndex2 = 4_000_000;
        mLog.commit(commitIndex2);
        mLog.compact(commitIndex2);

        assertEquals(0, writer.write(b));
        writer.release();
    }

    @Test
    public void commitBoostHighest() throws Exception {
        // Commit should advance the highest index in some cases, because not all writes are
        // expected to provide a highest index. Replies for missing data don't provide a
        // highest index, and then the term might end before the leader writes again.

        LogWriter writer = mLog.openWriter(0);
        write(writer, new byte[100], 0);
        writer.release();

        AtomicReference<Object> result = new AtomicReference<>();

        Thread stuckReader = TestUtils.startAndWaitUntilBlocked(new Thread(() -> {
            try {
                byte[] buf = new byte[1000];
                LogReader reader = mLog.openReader(0);
                result.set(reader.read(buf));
            } catch (Throwable e) {
                result.set(e);
            }
        }));

        assertNull(result.get());

        mLog.commit(200);
        mLog.finishTerm(200);

        byte[] buf = new byte[1000];

        LogReader reader = mLog.openReader(0);
        int amt = reader.read(buf);
        assertEquals(100, amt);
        reader.release();

        stuckReader.join();
        assertEquals(100, result.get());
    }

    private static void write(LogWriter writer, byte[] data) throws IOException {
        int amt = writer.write(data, 0, data.length, writer.index() + data.length);
        assertEquals(data.length, amt);
    }

    private static void write(LogWriter writer, byte[] data, long highestIndex)
        throws IOException
    {
        int amt = writer.write(data, 0, data.length, highestIndex);
        assertEquals(data.length, amt);
    }

    private static byte[] concat(byte[]... chunks) {
        int length = 0;
        for (byte[] chunk : chunks) {
            length += chunk.length;
        }

        byte[] complete = new byte[length];

        int pos = 0;
        for (byte[] chunk : chunks) {
            System.arraycopy(chunk, 0, complete, pos, chunk.length);
            pos += chunk.length;
        }        

        return complete;
    }

    static class Range {
        long mStart, mEnd;

        Range() {
        }

        Range(long start, long end) {
            mStart = start;
            mEnd = end;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof Range) {
                return ((Range) obj).mStart == mStart && ((Range) obj).mEnd == mEnd;
            }
            return false;
        }

        @Override
        public String toString() {
            return "[" + mStart + "," + mEnd + "]";
        }
    }

    static class RangeResult implements IndexRange {
        List<Range> mRanges = new ArrayList<>();

        @Override
        public void range(long start, long end) {
            mRanges.add(new Range(start, end));
        }

        @Override
        public String toString() {
            return mRanges.toString();
        }
    }
}
