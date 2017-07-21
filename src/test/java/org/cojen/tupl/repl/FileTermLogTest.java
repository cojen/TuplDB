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

import java.util.Random;

import java.util.concurrent.TimeUnit;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.util.Worker;

import org.cojen.tupl.TestUtils;

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
        mLog = new FileTermLog(mWorker, mBase, 0, 1, 0, 0, 0);
        assertEquals(Long.MAX_VALUE, mLog.endIndex());

        // Write a bunch of data and read it back.

        final int seed = 283742;

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

        rnd = new Random(seed);
        rnd2 = new Random(seed + 1);
        LogReader reader = mLog.openReader(0);
        assertEquals(0, reader.prevTerm());
        assertEquals(1, reader.term());
        long total = index;
        index = 0;

        while (true) {
            assertEquals(index, reader.index());
            int amt = reader.readAny(buf, 0, buf.length);
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
    }

    @Test
    public void tail() throws Throwable {
        mLog = new FileTermLog(mWorker, mBase, 0, 1, 0, 0, 0);

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
                    }

                    mTotal = reader.index();
                } catch (Throwable e) {
                    mEx = e;
                }
            }
        }

        Reader r = new Reader();
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
            if (commitIndex >= 0) {
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

        assertEquals(index, r.mTotal);
        Throwable ex = r.mEx;
        if (ex != null) {
            throw ex;
        }
    }
}
