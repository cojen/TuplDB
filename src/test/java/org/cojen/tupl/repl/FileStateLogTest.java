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
    private StateLog mLog;
    private List<StateLog> mMoreLogs = new ArrayList<>();

    private StateLog newTempLog() throws Exception {
        StateLog log = new FileStateLog(TestUtils.newTempBaseFile(getClass()));
        mMoreLogs.add(log);
        return log;
    }

    @Test
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
