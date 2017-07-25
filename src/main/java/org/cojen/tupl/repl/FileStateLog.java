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
import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.Iterator;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import org.cojen.tupl.io.CRC32C;
import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.LatchCondition;
import org.cojen.tupl.util.Worker;

/**
 * Standard StateLog implementation which stores data in file segments.
 *
 * @author Brian S O'Neill
 */
final class FileStateLog extends Latch implements StateLog {
    /*
      File naming:

      <base>  (metadata file)
      <base>.<term>.<start index>  (log files)

      Metadata file is 4608 bytes, with fields stored at offset 0 and 4096, alternating. Fields
      are little endian encoded.

      0:   Magic number (long)
      8:   Highest term (long)
      16:  Highest index (exclusive) (long)
      24:  Commit index (exclusive) (long)
      32.. Reserved
      508: CRC32C (int)

      Example files with base of "mydata.repl":

      mydata.repl
      mydata.repl.0.0
      mydata.repl.1.1000
      mydata.repl.1.2000
      mydata.repl.2.2500

    */

    private static final long MAGIC_NUMBER = 5267718596810043313L;

    private final Worker mWorker;
    private final File mBase;

    // Terms are keyed only by their start index.
    private final ConcurrentSkipListSet<LKey<TermLog>> mTermLogs;

    private TermLog mHighestTermLog;
    private TermLog mCommitTermLog;
    private TermLog mContigTermLog;

    private final LatchCondition mTermCondition;

    private boolean mClosed;

    FileStateLog(File base) throws IOException {
        base = base.getAbsoluteFile();
        if (base.isDirectory()) {
            throw new IllegalArgumentException("Base file is a directory: " + base);
        }
        if (!base.getParentFile().exists()) {
            throw new IllegalArgumentException("Parent file doesn't exist: " + base);
        }

        mWorker = Worker.make(10, 15, TimeUnit.SECONDS, null);
        mBase = base;

        mTermLogs = new ConcurrentSkipListSet<>();
        mTermCondition = new LatchCondition();

        // FIXME: Open terms. If no metadata file, chuck everything. If metadata file exists,
        // chuck all files which are higher than what metadata file reports.
    }

    @Override
    public void captureHighest(LogInfo info) {
        acquireShared();

        TermLog highestLog = mHighestTermLog;
        if (highestLog != null && highestLog == mTermLogs.last()) {
            highestLog.captureHighest(info);
            releaseShared();
            return;
        }

        int size = mTermLogs.size();

        if (size == 0) {
            info.mTerm = 0;
            info.mHighestIndex = 0;
            info.mCommitIndex = 0;
            releaseShared();
            return;
        }

        if (highestLog != null) {
            // Search again, in case log instance has been orphaned.
            highestLog = (TermLog) mTermLogs.ceiling(highestLog); // findGe
        }

        if (highestLog == null) {
            highestLog = (TermLog) mTermLogs.first();
        }

        while (true) {
            TermLog nextLog = (TermLog) mTermLogs.higher(highestLog); // findGt
            highestLog.captureHighest(info);
            if (nextLog == null || info.mHighestIndex < nextLog.startIndex()) {
                if (tryUpgrade()) {
                    mHighestTermLog = highestLog;
                    releaseExclusive();
                } else {
                    releaseShared();
                }
                return;
            }
            highestLog = nextLog;
        }
    }

    @Override
    public void commit(long commitIndex) {
        acquireShared();

        TermLog commitLog = mCommitTermLog;
        if (commitLog != null && commitLog == mTermLogs.last()) {
            commitLog.commit(commitIndex);
            releaseShared();
            return;
        }

        int size = mTermLogs.size();

        if (size == 0) {
            releaseShared();
            return;
        }

        if (commitLog != null) {
            // Search again, in case log instance has been orphaned.
            commitLog = (TermLog) mTermLogs.ceiling(commitLog); // findGe
        }

        if (commitLog == null) {
            commitLog = (TermLog) mTermLogs.first();
        }

        LogInfo info = new LogInfo();
        while (true) {
            commitLog.captureHighest(info);
            if (info.mCommitIndex < commitLog.endIndex()) {
                break;
            }
            TermLog nextLog = (TermLog) mTermLogs.higher(commitLog); // findGt
            if (nextLog == null) {
                break;
            }
            commitLog = nextLog;
        }

        if (commitLog != mCommitTermLog && tryUpgrade()) {
            mCommitTermLog = commitLog;
            downgrade();
        }

        do {
            commitLog.commit(commitIndex);
        } while ((commitLog = (TermLog) mTermLogs.higher(commitLog)) != null);

        releaseShared();
    }

    @Override
    public boolean defineTerm(long prevTerm, long term, long index) throws IOException {
        return defineTermLog(prevTerm, term, index) != null;
    }

    @Override
    public void queryTerms(long startIndex, long endIndex, TermQuery results) {
        LKey<TermLog> startKey = new LKey.Finder<>(startIndex);
        LKey<TermLog> endKey = new LKey.Finder<>(endIndex);

        for (Object key : mTermLogs.subSet(startKey, endKey)) {
            TermLog termLog = (TermLog) key;
            results.term(termLog.prevTerm(), termLog.term(), termLog.startIndex());
        }
    }

    @Override
    public long checkForMissingData(long contigIndex, IndexRange results) {
        acquireShared();

        TermLog termLog = mContigTermLog;
        if (termLog != null && termLog == mTermLogs.last()) {
            releaseShared();
            return termLog.checkForMissingData(contigIndex, results);
        }

        int size = mTermLogs.size();

        if (size == 0) {
            releaseShared();
            return 0;
        }

        if (termLog != null) {
            // Search again, in case log instance has been orphaned.
            termLog = (TermLog) mTermLogs.ceiling(termLog); // findGe
        }

        if (termLog == null) {
            termLog = (TermLog) mTermLogs.first();
        }

        final long originalContigIndex = contigIndex;

        TermLog nextLog;
        while (true) {
            nextLog = (TermLog) mTermLogs.higher(termLog); // findGt
            contigIndex = termLog.checkForMissingData(contigIndex, results);
            if (contigIndex < termLog.endIndex() || nextLog == null) {
                break;
            }
            termLog = nextLog;
        }

        if (termLog != mContigTermLog && tryUpgrade()) {
            mContigTermLog = termLog;
            if (nextLog == null) {
                releaseExclusive();
                return contigIndex;
            }
            downgrade();
        }

        if (contigIndex == originalContigIndex) {
            // Scan ahead into all remaining terms.
            while (nextLog != null) {
                nextLog.checkForMissingData(0, results);
                nextLog = (TermLog) mTermLogs.higher(nextLog); // findGt
            }
        }

        releaseShared();
        return contigIndex;
    }

    @Override
    public LogWriter openWriter(long prevTerm, long term, long index) throws IOException {
        TermLog termLog = defineTermLog(prevTerm, term, index);
        return termLog == null ? null : termLog.openWriter(index);
    }

    /**
     * @return null if not defined due to term mismatch
     */
    // package-private for testing
    TermLog defineTermLog(long prevTerm, long term, long index) throws IOException {
        final LKey<TermLog> key = new LKey.Finder<>(index);

        acquireExclusive();
        try {
            final TermLog termLog;
            defineTermLog: {
                TermLog prevTermLog = (TermLog) mTermLogs.lower(key); // findLt

                if (prevTerm == 0) {
                    if (prevTermLog != null) {
                        prevTerm = prevTermLog.term();
                    }
                } else if (prevTermLog == null) {
                    termLog = null;
                    break defineTermLog;
                } else {
                    long actualPrevTerm = prevTermLog.term();
                    if (prevTerm != actualPrevTerm) {
                        termLog = null;
                        break defineTermLog;
                    }
                    if (term == actualPrevTerm && index < prevTermLog.endIndex()) {
                        termLog = prevTermLog;
                        break defineTermLog;
                    }
                }

                TermLog startTermLog = (TermLog) mTermLogs.floor(key); // findLe

                if (startTermLog != null) {
                    long actualTerm = startTermLog.term();
                    if (term == actualTerm) {
                        termLog = startTermLog;
                        break defineTermLog;
                    }

                    if (term < actualTerm) {
                        termLog = null;
                        break defineTermLog;
                    }

                    prevTermLog = startTermLog;
                    prevTermLog.finishTerm(index);
                    
                    // Truncate and remove all higher conflicting term logs. Iterator might
                    // start with the prevTermLog just finished, facilitating its
                    // removal. Finishing again at the same index is harmless.
                    Iterator<LKey<TermLog>> it = mTermLogs.tailSet(key).iterator(); // viewGe
                    while (it.hasNext()) {
                        TermLog logGe = (TermLog) it.next();
                        logGe.finishTerm(index);
                        it.remove();
                    }
                }

                if (mClosed) {
                    throw new IOException("Closed");
                }

                File file = new File(mBase.getPath() + '.' + term);
                // FIXME: provide correct commitIndex
                termLog = new FileTermLog(mWorker, file, prevTerm, term, index, 0, index);

                mTermLogs.add(termLog);
                mTermCondition.signalAll();
            }

            return termLog;
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public LogReader openReader(long index, long nanosTimeout) throws IOException {
        LKey<TermLog> key = new LKey.Finder<>(index);

        boolean exclusive = false;
        acquireShared();
        try {
            LogReader reader;
            openReader: {
                while (true) {
                    reader = tryOpenReader(key);
                    if (reader != null || nanosTimeout == 0) {
                        break openReader;
                    }
                    if (exclusive) {
                        break;
                    }
                    if (tryUpgrade()) {
                        exclusive = true;
                        break;
                    }
                    releaseShared();
                    acquireExclusive();
                    exclusive = true;
                }

                long nanosEnd = nanosTimeout > 0 ? System.nanoTime() : 0;

                while (true) {
                    int result = mTermCondition.await(this, nanosTimeout, nanosEnd);
                    if (result < 0) {
                        throw new InterruptedIOException();
                    }
                    reader = tryOpenReader(key);
                    if (reader != null) {
                        break openReader;
                    }
                    if (result == 0 ||
                        (nanosTimeout >= 0 && (nanosTimeout = nanosEnd - System.nanoTime()) <= 0))
                    {
                        reader = null;
                        break openReader;
                    }
                }
            }

            return reader;
        } finally {
            release(exclusive);
        }
    }

    // Caller must hold any latch.
    private LogReader tryOpenReader(LKey<TermLog> key) throws IOException {
        TermLog termLog = (TermLog) mTermLogs.floor(key); // findLe
        if (termLog != null) {
            return termLog.openReader(key.key());
        }
        if (mTermLogs.isEmpty()) {
            return null;
        }
        throw new IllegalStateException
            ("Index is lower than start index: " + key.key() + " < " +
             ((TermLog) mTermLogs.first()).startIndex());
    }

    @Override
    public void sync() throws IOException {
        // FIXME: sync
        throw null;
    }

    @Override
    public void close() throws IOException {
        acquireExclusive();
        try {
            if (mClosed) {
                return;
            }
            mClosed = true;
            for (Object key : mTermLogs) {
                ((TermLog) key).close();
            }
        } finally {
            releaseExclusive();
        }
    }
}
