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
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

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

      <base>.md                                 (metadata file)
      <base>[.<prevTerm>].<term>.<start index>  (log files)

      Metadata file stores little-endian fields at offset 0 and 4096, alternating.

      0:  Magic number (long)
      8:  Encoding version (int)
      12: Metadata counter (int)
      16: Current term (long)
      24: Voted for (long)
      32: Highest log term (long)
      40: Highest contiguous log index (exclusive) (long)
      48: Durable commit log index (exclusive) (long)
      56: CRC32C (int)

      Example segment files with base of "mydata.repl":

      mydata.repl
      mydata.repl.0.1.0
      mydata.repl.1.4000
      mydata.repl.1.8000
      mydata.repl.1.2.9000

      The prevTerm field is not required if it matches the term.

    */

    private static final long MAGIC_NUMBER = 5267718596810043313L;
    private static final int ENCODING_VERSION = 20170925;
    private static final int SECTION_POW = 12;
    private static final int SECTION_SIZE = 1 << SECTION_POW;
    private static final int METADATA_SIZE = 60;
    private static final int METADATA_FILE_SIZE = SECTION_SIZE + METADATA_SIZE;
    private static final int COUNTER_OFFSET = 12;
    private static final int CURRENT_TERM_OFFSET = 16;
    private static final int CRC_OFFSET = METADATA_SIZE - 4;

    private final File mBase;
    private final Worker mWorker;

    // Terms are keyed only by their start index.
    private final ConcurrentSkipListSet<LKey<TermLog>> mTermLogs;

    private final FileChannel mMetadataFile;
    private final FileLock mMetadataLock;
    private final MappedByteBuffer mMetadataBuffer;
    private final Checksum mMetadataCrc;
    private final LogInfo mMetadataInfo;
    private int mMetadataCounter;
    private long mMetadataHighestIndex;
    private long mMetadataDurableIndex;

    private long mCurrentTerm;
    private long mVotedForId;

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

        try {
            mMetadataLock = mMetadataFile.tryLock();
        } catch (OverlappingFileLockException e) {
            Utils.closeQuietly(mMetadataFile);
            throw new IOException("Replicator is already open by current process");
        }

        if (mMetadataLock == null) {
            Utils.closeQuietly(mMetadataFile);
            throw new IOException("Replicator is open and locked by another process");
        }

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
        mMetadataBuffer.position(offset + CURRENT_TERM_OFFSET);
        long currentTerm = mMetadataBuffer.getLong();
        long votedForId = mMetadataBuffer.getLong();
        long highestTerm = mMetadataBuffer.getLong();
        long highestIndex = mMetadataBuffer.getLong();
        long durableIndex = mMetadataBuffer.getLong();

        if (currentTerm < highestTerm) {
            throw new IOException("Current term is lower than highest term: " +
                                  currentTerm + " < " + highestTerm);
        }
        if (highestIndex < durableIndex) {
            throw new IOException("Highest index is lower than durable index: " +
                                  highestIndex + " < " + durableIndex);
        }

        // Initialize mMetadataInfo with valid state. Note that the mCommitIndex field isn't
        // actually used at all when persisting metadata, but initialize it anyhow.
        mMetadataInfo.mTerm = highestTerm;
        mMetadataInfo.mHighestIndex = highestIndex;
        mMetadataInfo.mCommitIndex = durableIndex;

        // Seemingly redundant, these fields are updated after the metadata file is written.
        mMetadataHighestIndex = highestIndex;
        mMetadataDurableIndex = durableIndex;

        mCurrentTerm = currentTerm;
        mVotedForId = votedForId;

        // Open all the existing terms.
        TreeMap<Long, List<String>> mTermFileNames = new TreeMap<>();
        String[] fileNames = mBase.getParentFile().list();

        if (fileNames != null && fileNames.length != 0) {
            // This pattern captures the term, but it discards the optional prevTerm.
            Pattern p = Pattern.compile(base.getName() + "\\.(\\d+)\\.\\d+(?:\\.\\d+)?");

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

        long prevTerm = -1;
        for (Map.Entry<Long, List<String>> e : mTermFileNames.entrySet()) {
            long term = e.getKey();
            TermLog termLog = FileTermLog.openTerm
                (mWorker, mBase, prevTerm, term, -1, 0, highestIndex, e.getValue());
            mTermLogs.add(termLog);
            prevTerm = term;
        }

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

        if (mTermLogs.isEmpty()) {
            // Create a primordial term.
            mTermLogs.add(FileTermLog.newTerm(mWorker, mBase, 0, 0, 0, 0));
        }

        if (durableIndex > 0) {
            commit(durableIndex);
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

        // Initialize current term, voted for, highest log term, highest contiguous log index,
        // and durable commit log index.
        if (bb.position() != (offset + CURRENT_TERM_OFFSET)) {
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

        // Pass the commit index to the terms in descending order, to prevent a race condition
        // with the doCaptureHighest method. It iterates in ascending order, assuming that the
        // commit index of a higher term cannot be lower. If the terms are committed in
        // ascending order, latch coupling would be required to prevent the doCaptureHighest
        // from going too far ahead. Descending order commit is simpler.

        Iterator<LKey<TermLog>> it = mTermLogs.descendingIterator();
        while (it.hasNext()) {
            TermLog termLog = (TermLog) it.next();
            termLog.commit(commitIndex);
            if (termLog == commitLog) {
                break;
            }
        }

        releaseShared();
    }

    @Override
    public long incrementCurrentTerm(int termIncrement, long candidateId) throws IOException {
        if (termIncrement <= 0) {
            throw new IllegalArgumentException();
        }
        synchronized (mMetadataInfo) {
            mCurrentTerm += termIncrement;
            mVotedForId = candidateId;
            doSync();
            return mCurrentTerm;
        }
    }

    @Override
    public long checkCurrentTerm(long term) throws IOException {
        synchronized (mMetadataInfo) {
            if (term > mCurrentTerm) {
                mCurrentTerm = term;
                mVotedForId = 0;
                doSync();
            }
            return mCurrentTerm;
        }
    }

    @Override
    public boolean checkCandidate(long candidateId) throws IOException {
        synchronized (mMetadataInfo) {
            if (mVotedForId == candidateId) {
                return true;
            }
            if (mVotedForId == 0) {
                mVotedForId = candidateId;
                doSync();
                return true;
            }
            return false;
        }
    }

    @Override
    public void compact(long index) throws IOException {
        Iterator<LKey<TermLog>> it = mTermLogs.iterator();

        if (it.hasNext()) {
            TermLog termLog = (TermLog) it.next();

            while (true) {
                TermLog nextLog;
                if (!it.hasNext() || (nextLog = (TermLog) it.next()).isEmpty()) {
                    termLog.compact(index, false);
                    break;
                }
                termLog.compact(index, true);
                if (!termLog.isEmpty()) {
                    break;
                }
                mTermLogs.remove(termLog);
                termLog = nextLog;
            }
        }
    }

    @Override
    public void truncateAll(long prevTerm, long term, long index) throws IOException {
        synchronized (mMetadataInfo) {
            if (mClosed) {
                throw new IOException("Closed");
            }

            acquireExclusive();
            try {
                Iterator<LKey<TermLog>> it = mTermLogs.iterator();
                while (it.hasNext()) {
                    TermLog termLog = (TermLog) it.next();
                    termLog.compact(Long.MAX_VALUE, true);
                    it.remove();
                }

                // Create a new primordial term.
                mTermLogs.add(FileTermLog.newTerm(mWorker, mBase, prevTerm, term, index, index));
            } finally {
                releaseExclusive();
            }

            mMetadataInfo.mTerm = term;
            mMetadataInfo.mHighestIndex = index;
            mMetadataInfo.mCommitIndex = index;
            mMetadataHighestIndex = index;
            mMetadataDurableIndex = index;

            syncMetadata();
        }
    }

    @Override
    public boolean defineTerm(long prevTerm, long term, long index) throws IOException {
        return defineTermLog(prevTerm, term, index) != null;
    }

    @Override
    public TermLog termLogAt(long index) {
        return (TermLog) mTermLogs.floor(new LKey.Finder<>(index)); // findLe
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

        if (mTermLogs.isEmpty()) {
            releaseShared();
            return 0;
        }

        TermLog termLog = mContigTermLog;
        if (termLog != null && termLog == mTermLogs.last()) {
            releaseShared();
            return termLog.checkForMissingData(contigIndex, results);
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
                termLog = (TermLog) mTermLogs.floor(key); // findLe

                if (termLog == null) {
                    break defineTermLog;
                }

                long actualPrevTerm = termLog.prevTermAt(index);

                if (prevTerm != actualPrevTerm) {
                    int removeCount = countConflictingEmptyTerms(prevTerm, index, termLog);

                    if (removeCount <= 0) {
                        termLog = null;
                        break defineTermLog;
                    }

                    // Remove the empty terms, but exclusive latch must be held.

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

                    while (true) {
                        termLog.finishTerm(termLog.startIndex());
                        mTermLogs.remove(termLog);
                        removeCount--;
                        if (removeCount <= 0) {
                            break;
                        }
                        termLog = (TermLog) mTermLogs.lower(termLog);
                    }

                    continue;
                }

                long actualTerm = termLog.term();

                if (term == actualTerm) {
                    // Term already exists.
                    break defineTermLog;
                }

                if (term < actualTerm) {
                    termLog = null;
                    break defineTermLog;
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

                termLog.sync();

                long commitIndex = termLog.finishTerm(index);

                // Truncate and remove all higher conflicting term logs. Iterator might start
                // with the term log just finished, facilitating its removal. Finishing again
                // at the same index is harmless.
                Iterator<LKey<TermLog>> it = mTermLogs.tailSet(key).iterator(); // viewGe
                while (it.hasNext()) {
                    TermLog logGe = (TermLog) it.next();
                    logGe.finishTerm(index);
                    it.remove();
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

    /**
     * Checks if removing empty terms would resolve a prev term conflict.
     * Caller must hold any latch.
     *
     * @return amount of terms to remove; 0 if the conflict cannot be resolved
     */
    private int countConflictingEmptyTerms(long prevTerm, long index, TermLog termLog) {
        int count = 1;

        while (true) {
            if (!termLog.isEmpty()) {
                return 0;
            }
            termLog = (TermLog) mTermLogs.lower(termLog); // findLt
            if (termLog == null) {
                // Cannot remove the primordial term.
                return 0;
            }
            if (prevTerm == termLog.prevTermAt(index)) {
                return count;
            }
            count++;
        }
    }

    @Override
    public LogReader openReader(long index) {
        LKey<TermLog> key = new LKey.Finder<>(index);

        acquireShared();
        try {
            TermLog termLog = (TermLog) mTermLogs.floor(key); // findLe

            if (termLog != null) {
                return termLog.openReader(index);
            }

            termLog = (TermLog) mTermLogs.first();

            throw new IllegalStateException
                ("Index is lower than start index: " + index + " < " + termLog.startIndex());
        } finally {
            releaseShared();
        }

    }

    @Override
    public void sync() throws IOException {
        synchronized (mMetadataInfo) {
            doSync();
        }
    }

    @Override
    public long syncCommit(long prevTerm, long term, long index) throws IOException {
        synchronized (mMetadataInfo) {
            if (mClosed) {
                throw new IOException("Closed");
            }

            acquireShared();
            try {
                TermLog termLog = termLogAt(index);

                if (termLog == null || term != termLog.term()
                    || prevTerm != termLog.prevTermAt(index))
                {
                    return -1;
                }

                TermLog highestLog = mHighestTermLog;
                if (highestLog != null && highestLog == mTermLogs.last()) {
                    highestLog.captureHighest(mMetadataInfo);
                } else {
                    highestLog = doCaptureHighest(mMetadataInfo, highestLog, false);
                }

                if (index > mMetadataInfo.mHighestIndex) {
                    return -1;
                }

                if (index <= mMetadataHighestIndex) {
                    // Nothing to do.
                    return mMetadataInfo.mCommitIndex;
                }

                highestLog.sync();
            } finally {
                releaseShared();
            }

            syncMetadata();

            return mMetadataInfo.mCommitIndex;
        }
    }

    @Override
    public boolean isDurable(long index) {
        synchronized (mMetadataInfo) {
            return index <= mMetadataDurableIndex;
        }
    }

    @Override
    public boolean commitDurable(long index) throws IOException {
        synchronized (mMetadataInfo) {
            if (mClosed) {
                throw new IOException("Closed");
            }

            if (index <= mMetadataDurableIndex) {
                return false;
            }

            checkDurable(index, mMetadataHighestIndex);

            captureHighest(mMetadataInfo);

            checkDurable(index, mMetadataInfo.mCommitIndex);

            syncMetadata(index);

            return true;
        }
    }

    private static void checkDurable(long index, long highestIndex) {
        if (index > highestIndex) {
            throw new IllegalStateException("Commit index is too high: " + index
                                            + " > " + highestIndex);
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

        syncMetadata();
    }

    // Caller must be synchronized on mMetadataInfo.
    private void syncMetadata() throws IOException {
        syncMetadata(mMetadataDurableIndex);
    }

    // Caller must be synchronized on mMetadataInfo.
    private void syncMetadata(long durableIndex) throws IOException {
        if (mClosed) {
            return;
        }

        if (mMetadataInfo.mHighestIndex < durableIndex) {
            throw new IllegalStateException("Highest index is lower than durable index: " +
                                            mMetadataInfo.mHighestIndex + " < " + durableIndex);
        }

        int counter = mMetadataCounter + 1;
        int offset = (counter & 1) << SECTION_POW;

        MappedByteBuffer bb = mMetadataBuffer;

        bb.clear();
        bb.position(offset + COUNTER_OFFSET);
        bb.limit(offset + CRC_OFFSET);

        bb.putInt(counter);
        bb.putLong(mCurrentTerm);
        bb.putLong(mVotedForId);
        bb.putLong(mMetadataInfo.mTerm);
        bb.putLong(mMetadataInfo.mHighestIndex);
        bb.putLong(durableIndex);

        bb.position(offset);
        mMetadataCrc.reset();
        CRC32C.update(mMetadataCrc, bb);

        bb.limit(offset + METADATA_SIZE);
        bb.putInt((int) mMetadataCrc.getValue());

        bb.force();

        // Update field values only after successful file I/O.
        mMetadataCounter = counter;
        mMetadataHighestIndex = mMetadataInfo.mHighestIndex;
        mMetadataDurableIndex = durableIndex;
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

                mMetadataLock.close();

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
