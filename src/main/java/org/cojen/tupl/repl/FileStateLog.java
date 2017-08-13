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

import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;

import java.nio.channels.FileChannel;

import java.nio.file.StandardOpenOption;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.zip.Checksum;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import org.cojen.tupl.io.CRC32C;
import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Worker;

/**
 * Standard StateLog implementation which stores data in file segments.
 *
 * @author Brian S O'Neill
 */
final class FileStateLog extends Latch implements StateLog {
    /*
      File naming:

      <base>.md                    (metadata file)
      <base>.<term>.<start index>  (log files)

      Metadata file stores little-endian fields at offset 0 and 4096, alternating.

      0:  Magic number (long)
      8:  Encoding version (int)
      12: Metadata counter (int)
      16: Start index (long)
      24: Current term (long)
      32: Highest log term (long)
      40: Highest contiguous log index (exclusive) (long)
      48: Commit log index (exclusive) (long)
      56: CRC32C (int)

      Example files with base of "mydata.repl":

      mydata.repl
      mydata.repl.0.0
      mydata.repl.1.1000
      mydata.repl.1.2000
      mydata.repl.2.2500

    */

    private static final long MAGIC_NUMBER = 5267718596810043313L;
    private static final int ENCODING_VERSION = 20170812;
    private static final int SECTION_POW = 12;
    private static final int SECTION_SIZE = 1 << SECTION_POW;
    private static final int METADATA_SIZE = 60;
    private static final int METADATA_FILE_SIZE = SECTION_SIZE + METADATA_SIZE;
    private static final int COUNTER_OFFSET = 12;
    private static final int START_INDEX_OFFSET = 16;
    private static final int CRC_OFFSET = METADATA_SIZE - 4;

    private final File mBase;
    private final Worker mWorker;

    // Terms are keyed only by their start index.
    private final ConcurrentSkipListSet<LKey<TermLog>> mTermLogs;

    private final FileChannel mMetadataFile;
    private final MappedByteBuffer mMetadataBuffer;
    private final Checksum mMetadataCrc;
    private final LogInfo mMetadataInfo;
    private int mMetadataCounter;
    private long mStartIndex;
    private long mCurrentTerm;

    private TermLog mHighestTermLog;
    private TermLog mCommitTermLog;
    private TermLog mContigTermLog;

    private boolean mClosed;

    FileStateLog(File base) throws IOException {
        base = FileTermLog.checkBase(base);

        mBase = base;
        mWorker = Worker.make(10, 15, TimeUnit.SECONDS, null);

        mTermLogs = new ConcurrentSkipListSet<>();

        mMetadataFile = FileChannel.open
            (new File(base.getPath() + ".md").toPath(),
             StandardOpenOption.READ,
             StandardOpenOption.WRITE,
             StandardOpenOption.CREATE,
             StandardOpenOption.DSYNC);

        boolean mdFileExists = mMetadataFile.size() != 0;

        mMetadataBuffer = mMetadataFile.map(FileChannel.MapMode.READ_WRITE, 0, METADATA_FILE_SIZE);
        mMetadataBuffer.order(ByteOrder.LITTLE_ENDIAN);

        mMetadataCrc = CRC32C.newInstance();
        mMetadataInfo = new LogInfo();

        if (!mdFileExists) {
            // Prepare a new metadata file.
            cleanMetadata(0, 0);
            cleanMetadata(SECTION_SIZE, 1);
        }

        // Ensure existing contents are durable following a process restart.
        mMetadataBuffer.force();

        long counter0 = verifyMetadata(0);
        long counter1 = verifyMetadata(SECTION_SIZE);

        // Select the higher counter from the valid sections.
        int counter, offset;
        select: {
            if (counter0 < 0) {
                if (counter1 < 0) {
                    if (counter0 == -1 || counter1 == -1) {
                        throw new IOException("Metadata magic number is wrong");
                    }
                    throw new IOException("Metadata CRC mismatch");
                }
            } else if (counter1 < 0 || (((int) counter0) - (int) counter1) > 0) {
                counter = (int) counter0;
                offset = 0;
                break select;
            }
            counter = (int) counter1;
            offset = SECTION_SIZE;
        }

        if (((counter & 1) << SECTION_POW) != offset) {
            throw new IOException("Metadata sections are swapped");
        }

        mMetadataCounter = counter;

        mMetadataBuffer.clear();
        mMetadataBuffer.position(offset + START_INDEX_OFFSET);
        long startIndex = mMetadataBuffer.getLong();
        long currentTerm = mMetadataBuffer.getLong();
        long highestTerm = mMetadataBuffer.getLong();
        long highestIndex = mMetadataBuffer.getLong();
        long commitIndex = mMetadataBuffer.getLong();

        if (currentTerm < highestTerm) {
            throw new IOException("Current term is lower than highest term");
        }
        if (highestIndex < commitIndex) {
            throw new IOException("Highest index is lower than commit index");
        }
        if (startIndex > commitIndex) {
            throw new IOException("Start index is higher than commit index");
        }

        mStartIndex = startIndex;
        mCurrentTerm = currentTerm;

        // Open all the existing terms.
        TreeMap<Long, List<String>> mTermFileNames = new TreeMap<>();
        String[] fileNames = mBase.getParentFile().list();

        if (fileNames != null && fileNames.length != 0) {
            Pattern p = Pattern.compile(base.getName() + "\\.(\\d*)\\.\\d*");
            for (String name : fileNames) {
                Matcher m = p.matcher(name);
                if (m.matches()) {
                    Long term = Long.valueOf(m.group(1));
                    if (term <= 0) {
                        throw new IOException("Illegal term: " + term);
                    }
                    List<String> termNames = mTermFileNames.get(term);
                    if (termNames == null) {
                        termNames = new ArrayList<>();
                        mTermFileNames.put(term, termNames);
                    }
                    termNames.add(name);
                }
            }
        }

        long prevTerm = 0;
        for (Map.Entry<Long, List<String>> e : mTermFileNames.entrySet()) {
            long term = e.getKey();
            TermLog termLog = FileTermLog.openTerm
                (mWorker, mBase, prevTerm, term, -1, 0, highestIndex, e.getValue());
            mTermLogs.add(termLog);
            prevTerm = term;
        }

        TermLog highest = null;

        if (!mTermLogs.isEmpty()) {
            Iterator<LKey<TermLog>> it = mTermLogs.iterator();
            TermLog termLog = (TermLog) it.next();

            while (true) {
                if (termLog.term() > highestTerm) {
                    // Delete all terms higher than the highest.
                    while (true) {
                        termLog.finishTerm(termLog.startIndex());
                        it.remove();
                        if (!it.hasNext()) {
                            break;
                        }
                        termLog = (TermLog) it.next();
                    }
                    break;
                }

                highest = termLog;

                TermLog next;
                if (it.hasNext()) {
                    next = (TermLog) it.next();
                } else {
                    next = null;
                }

                if (next == null) {
                    break;
                }

                if (next.term() <= highestTerm) {
                    termLog.finishTerm(next.startIndex());
                }

                termLog = next;
            }
        }

        if (commitIndex > 0) {
            commit(commitIndex);
        }
    }

    private void cleanMetadata(int offset, int counter) {
        MappedByteBuffer bb = mMetadataBuffer;

        bb.clear();
        bb.position(offset);
        bb.limit(offset + METADATA_SIZE);

        bb.putLong(MAGIC_NUMBER);
        bb.putInt(ENCODING_VERSION);
        bb.putInt(counter);

        // Start index, current term, highest log term, highest log index, commit log index.
        if (bb.position() != (offset + START_INDEX_OFFSET)) {
            throw new AssertionError();
        }
        for (int i=0; i<5; i++) {
            bb.putLong(0);
        }

        if (bb.position() != (offset + CRC_OFFSET)) {
            throw new AssertionError();
        }

        bb.limit(bb.position());
        bb.position(offset);

        mMetadataCrc.reset();
        CRC32C.update(mMetadataCrc, bb);

        bb.limit(offset + METADATA_SIZE);
        bb.putInt((int) mMetadataCrc.getValue());
    }

    /**
     * @return uint32 counter value, or -1 if wrong magic number, or -2 if CRC doesn't match
     * @throws IOException if wrong magic number or encoding version
     */
    private long verifyMetadata(int offset) throws IOException {
        MappedByteBuffer bb = mMetadataBuffer;

        bb.clear();
        bb.position(offset);
        bb.limit(offset + CRC_OFFSET);

        if (bb.getLong() != MAGIC_NUMBER) {
            return -1;
        }

        bb.position(offset);

        mMetadataCrc.reset();
        CRC32C.update(mMetadataCrc, bb);

        bb.limit(offset + METADATA_SIZE);
        int actual = bb.getInt();

        if (actual != (int) mMetadataCrc.getValue()) {
            return -2;
        }

        bb.clear();
        bb.position(offset + 8);
        bb.limit(offset + CRC_OFFSET);

        int encoding = bb.getInt();
        if (encoding != ENCODING_VERSION) {
            throw new IOException("Metadata encoding version is unknown: " + encoding);
        }

        return bb.getInt() & 0xffffffffL;
    }

    @Override
    public void captureHighest(LogInfo info) {
        acquireShared();
        TermLog highestLog = mHighestTermLog;
        if (highestLog != null && highestLog == mTermLogs.last()) {
            highestLog.captureHighest(info);
            releaseShared();
        } else {
            doCaptureHighest(info, highestLog, true);
        }
    }

    // Must be called with shared latch held.
    private TermLog doCaptureHighest(LogInfo info, TermLog highestLog, boolean releaseLatch) {
        int size = mTermLogs.size();

        if (size == 0) {
            info.mTerm = 0;
            info.mHighestIndex = 0;
            info.mCommitIndex = 0;
            if (releaseLatch) {
                releaseShared();
            }
            return null;
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
                    if (releaseLatch) {
                        releaseExclusive();
                    } else {
                        downgrade();
                    }
                } else {
                    if (releaseLatch) {
                        releaseShared();
                    }
                }
                return highestLog;
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
            commitLog = (TermLog) mTermLogs.higher(commitLog); // findGt
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
    public long incrementCurrentTerm(int termIncrement) throws IOException {
        if (termIncrement <= 0) {
            throw new IllegalArgumentException();
        }
        synchronized (mMetadataInfo) {
            mCurrentTerm += termIncrement;
            doSync();
            return mCurrentTerm;
        }
    }

    @Override
    public long checkCurrentTerm(long term) throws IOException {
        synchronized (mMetadataInfo) {
            if (term > mCurrentTerm) {
                mCurrentTerm = term;
                doSync();
            }
            return mCurrentTerm;
        }
    }

    @Override
    public void startIndex(long index) throws IOException {
        acquireExclusive();
        if (mStartIndex != 0) {
            // FIXME: Allow changing the start index to a higher value, but not higher than the
            // commit index. Sync metadata before deleting any segments.
            releaseExclusive();
            throw new IllegalStateException();
        }
        mStartIndex = index;
        releaseExclusive();
    }

    @Override
    public boolean defineTerm(long prevTerm, long term, long index) throws IOException {
        return defineTermLog(prevTerm, term, index) != null;
    }

    @Override
    public void queryTerms(long startIndex, long endIndex, TermQuery results) {
        if (startIndex >= endIndex) {
            return;
        }

        LKey<TermLog> startKey = new LKey.Finder<>(startIndex);
        LKey<TermLog> endKey = new LKey.Finder<>(endIndex);

        LKey<TermLog> prev = mTermLogs.floor(startKey); // findLe
        if (prev != null && ((TermLog) prev).endIndex() > startIndex) {
            startKey = prev;
        }

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

        boolean exclusive = false;
        acquireShared();
        try {
            TermLog termLog;
            defineTermLog: while (true) {
                TermLog prevTermLog = (TermLog) mTermLogs.lower(key); // findLt

                if (prevTermLog == null) {
                    if (index != mStartIndex) {
                        termLog = null;
                        break defineTermLog;
                    }
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

                    if (term < actualTerm || (actualTerm > 0 && prevTermLog == null)) {
                        termLog = null;
                        break defineTermLog;
                    }

                    if (!exclusive) {
                        if (tryUpgrade()) {
                            exclusive = true;
                        } else {
                            releaseShared();
                            acquireExclusive();
                            exclusive = true;
                            continue;
                        }
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

                if (!exclusive) {
                    if (tryUpgrade()) {
                        exclusive = true;
                    } else {
                        releaseShared();
                        acquireExclusive();
                        exclusive = true;
                        continue;
                    }
                }

                long commitIndex;
                if (prevTermLog == null) {
                    commitIndex = index;
                } else {
                    prevTermLog.sync();

                    LogInfo info = new LogInfo();
                    prevTermLog.captureHighest(info);
                    commitIndex = info.mCommitIndex;
                }

                termLog = FileTermLog.newTerm(mWorker, mBase, prevTerm, term, index, commitIndex);

                mTermLogs.add(termLog);
                break defineTermLog;
            }

            return termLog;
        } finally {
            release(exclusive);
        }
    }

    @Override
    public LogReader openReader(long index) {
        LKey<TermLog> key = new LKey.Finder<>(index);

        boolean exclusive = false;
        acquireShared();
        try {
            TermLog termLog;
            findTerm: {
                while (true) {
                    termLog = (TermLog) mTermLogs.floor(key); // findLe
                    if (termLog != null) {
                        break findTerm;
                    }
                    if (!mTermLogs.isEmpty()) {
                        throw new IllegalStateException
                            ("Index is lower than start index: " + key.key() + " < " +
                             ((TermLog) mTermLogs.first()).startIndex());
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

                // Create a primordial term.
                termLog = FileTermLog.newTerm(mWorker, mBase, 0, 0, mStartIndex, mStartIndex);
                mTermLogs.add(termLog);
            }

            return termLog.openReader(index);
        } finally {
            release(exclusive);
        }

    }

    @Override
    public void sync() throws IOException {
        synchronized (mMetadataInfo) {
            doSync();
        }
    }

    // Caller must be synchronized on mMetadataInfo.
    private void doSync() throws IOException {
        if (mClosed) {
            return;
        }

        acquireShared();
        try {
            TermLog highestLog = mHighestTermLog;
            if (highestLog != null && highestLog == mTermLogs.last()) {
                highestLog.captureHighest(mMetadataInfo);
                highestLog.sync();
            } else {
                highestLog = doCaptureHighest(mMetadataInfo, highestLog, false);
                if (highestLog != null) {
                    highestLog.sync();
                }
            }
        } finally {
            releaseShared();
        }

        int counter = mMetadataCounter + 1;
        int offset = (counter & 1) << SECTION_POW; 

        MappedByteBuffer bb = mMetadataBuffer;

        bb.clear();
        bb.position(offset + COUNTER_OFFSET);
        bb.limit(offset + CRC_OFFSET);

        bb.putInt(counter);
        bb.putLong(mStartIndex);
        bb.putLong(mCurrentTerm);
        bb.putLong(mMetadataInfo.mTerm);
        bb.putLong(mMetadataInfo.mHighestIndex);
        bb.putLong(mMetadataInfo.mCommitIndex);

        bb.position(offset);
        mMetadataCrc.reset();
        CRC32C.update(mMetadataCrc, bb);

        bb.limit(offset + METADATA_SIZE);
        bb.putInt((int) mMetadataCrc.getValue());

        bb.force();

        mMetadataCounter = counter;
    }

    @Override
    public void close() throws IOException {
        synchronized (mMetadataInfo) {
            acquireExclusive();
            try {
                if (mClosed) {
                    return;
                }

                mClosed = true;

                mMetadataFile.close();
                Utils.delete(mMetadataBuffer);

                for (Object key : mTermLogs) {
                    ((TermLog) key).close();
                }
            } finally {
                releaseExclusive();
            }
        }

        mWorker.join(true);
    }
}
