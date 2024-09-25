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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import java.nio.file.StandardOpenOption;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.zip.Checksum;
import java.util.zip.CRC32C;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

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

      <base>                                       (metadata file)
      <base>.<term>.<start position>[.<prevTerm>]  (log files)

      Metadata file stores little-endian fields at offset 0 and 4096, alternating.

      0:  Magic number (long)
      8:  Encoding version (int)
      12: Metadata counter (int)
      16: Current term (long)
      24: Voted for (long)
      32: Highest prev log term (long)
      40: Highest log term (long)
      48: Highest contiguous log position (exclusive) (long)
      56: Durable commit log position (exclusive) (long)
      64: CRC32C (int)

      Example segment files with base of "mydata.repl":

      mydata.repl
      mydata.repl.1.0.0
      mydata.repl.1.4000
      mydata.repl.1.8000
      mydata.repl.2.9000.1

      The prevTerm field is not required if it matches the term.

    */

    private static final long MAGIC_NUMBER = 5267718596810043313L;
    private static final int ENCODING_VERSION = 20171004;
    private static final int SECTION_POW = 12;
    private static final int SECTION_SIZE = 1 << SECTION_POW;
    private static final int METADATA_SIZE = 68;
    private static final int METADATA_FILE_SIZE = SECTION_SIZE + METADATA_SIZE;
    private static final int COUNTER_OFFSET = 12;
    private static final int CURRENT_TERM_OFFSET = 16;
    private static final int HIGHEST_PREV_TERM_OFFSET = 32;
    private static final int CRC_OFFSET = METADATA_SIZE - 4;

    private final File mBase;
    private final Worker mWorker;
    private final FileTermLog.Caches mCaches;

    // Terms are keyed only by their start position.
    private final ConcurrentSkipListSet<LKey<TermLog>> mTermLogs;

    private final FileChannel mMetadataFile;
    private final FileLock mMetadataLock;
    private final ByteBuffer mMetadataBuffer;
    private final Checksum mMetadataCrc;
    private final LogInfo mMetadataInfo;
    private final Latch mMetadataLatch;
    private int mMetadataCounter;
    private long mMetadataHighestPosition;
    private volatile long mMetadataDurablePosition;

    private long mCurrentTerm;
    private long mVotedForId;

    private TermLog mHighestTermLog;
    private TermLog mCommitTermLog;
    private TermLog mContigTermLog;

    private boolean mClosed;

    static FileStateLog open(File base) throws IOException {
        return new FileStateLog(FileTermLog.checkBase(base));
    }

    private FileStateLog(File base) throws IOException {
        mBase = base;
        mWorker = Worker.make(10, 15, TimeUnit.SECONDS, null);
        mCaches = new FileTermLog.Caches();

        mTermLogs = new ConcurrentSkipListSet<>();

        mMetadataInfo = new LogInfo();
        mMetadataLatch = new Latch();

        mMetadataFile = FileChannel.open
            (base.toPath(),
             StandardOpenOption.READ,
             StandardOpenOption.WRITE,
             StandardOpenOption.CREATE);

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

        mMetadataBuffer = ByteBuffer.allocate(METADATA_SIZE).order(ByteOrder.LITTLE_ENDIAN);

        mMetadataCrc = new CRC32C();

        if (mdFileExists) {
            // Ensure existing contents are durable following a process restart.
            mMetadataFile.force(true);
        } else {
            // Prepare a new metadata file.
            cleanMetadata(0, 0);
            cleanMetadata(SECTION_SIZE, 1);
        }

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

        ByteBuffer bb = readMetadata(offset).position(CURRENT_TERM_OFFSET);
        long currentTerm = bb.getLong();
        long votedForId = bb.getLong();
        long highestPrevTerm = bb.getLong();
        long highestTerm = bb.getLong();
        long highestPosition = bb.getLong();
        long durablePosition = bb.getLong();

        if (currentTerm < highestTerm) {
            throw new IOException("Current term is lower than highest term: " +
                                  currentTerm + " < " + highestTerm);
        }
        if (highestPosition < durablePosition) {
            throw new IOException("Highest position is lower than durable position: " +
                                  highestPosition + " < " + durablePosition);
        }

        // Initialize mMetadataInfo with valid state. Note that the mCommitPosition field isn't
        // actually used at all when persisting metadata, but initialize it anyhow.
        mMetadataInfo.mTerm = highestTerm;
        mMetadataInfo.mHighestPosition = highestPosition;
        mMetadataInfo.mCommitPosition = durablePosition;

        // Seemingly redundant, these fields are updated after the metadata file is written.
        mMetadataHighestPosition = highestPosition;
        mMetadataDurablePosition = durablePosition;

        mCurrentTerm = currentTerm;
        mVotedForId = votedForId;

        // Open all the existing terms.
        var termFileNames = new TreeMap<Long, List<String>>();
        File parentFile = mBase.getParentFile();
        String[] fileNames = parentFile.list();

        if (fileNames != null && fileNames.length != 0) {
            // This pattern captures the term, but it discards the optional prevTerm.
            Pattern p = Pattern.compile(base.getName() + "\\.(\\d+)\\.\\d+(?:\\.\\d+)?");

            for (String name : fileNames) {
                Matcher m = p.matcher(name);
                if (m.matches()) {
                    long term = Long.parseLong(m.group(1));
                    if (term <= 0) {
                        throw new IOException("Illegal term: " + term);
                    }
                    if (term > highestTerm) {
                        // Delete all terms higher than the highest.
                        Utils.delete(new File(parentFile, name));
                    } else {
                        List<String> termNames = termFileNames.get(term);
                        if (termNames == null) {
                            termNames = new ArrayList<>();
                            termFileNames.put(term, termNames);
                        }
                        termNames.add(name);
                    }
                }
            }
        }

        long prevTerm = -1;
        for (Map.Entry<Long, List<String>> e : termFileNames.entrySet()) {
            long term = e.getKey();
            TermLog termLog = FileTermLog.openTerm
                (mCaches, mWorker, mBase, prevTerm, term, -1, 0, highestPosition, e.getValue());
            mTermLogs.add(termLog);
            prevTerm = term;
        }

        if (!mTermLogs.isEmpty()) {
            Iterator<LKey<TermLog>> it = mTermLogs.iterator();
            var termLog = (TermLog) it.next();

            while (true) {
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
                    termLog.finishTerm(next.startPosition());
                }

                termLog = next;
            }
        }

        if (mTermLogs.isEmpty()) {
            // Create a primordial term.
            mTermLogs.add(FileTermLog.newTerm
                          (mCaches, mWorker, mBase, highestPrevTerm, highestTerm,
                           highestPosition, durablePosition));
        }

        if (durablePosition > 0) {
            commit(durablePosition);
        }
    }

    private ByteBuffer readMetadata(int offset) throws IOException {
        ByteBuffer bb = mMetadataBuffer.clear();
        if (mMetadataFile.position(offset).read(bb) != METADATA_SIZE) {
            throw new IOException("Truncated metadata file");
        }
        return bb;
    }

    private void writeMetadata(int offset, ByteBuffer bb) throws IOException {
        mMetadataCrc.reset();
        mMetadataCrc.update(bb.position(0).limit(CRC_OFFSET));
        
        if (bb.position() != CRC_OFFSET) {
            throw new AssertionError();
        }

        bb.limit(METADATA_SIZE).putInt((int) mMetadataCrc.getValue());
        bb.position(0);

        if (mMetadataFile.position(offset).write(bb) != METADATA_SIZE) {
            throw new IOException("Truncated metadata file");
        }

        mMetadataFile.force(true);
    }

    private void cleanMetadata(int offset, int counter) throws IOException {
        ByteBuffer bb = mMetadataBuffer.position(0).limit(CRC_OFFSET);

        bb.putLong(MAGIC_NUMBER);
        bb.putInt(ENCODING_VERSION);
        bb.putInt(counter);

        // Initialize current term, voted for, highest prev log term, highest log term, highest
        // contiguous log position, and durable commit log position.
        if (bb.position() != CURRENT_TERM_OFFSET) {
            throw new AssertionError();
        }
        for (int i=0; i<6; i++) {
            bb.putLong(0);
        }

        if (bb.position() != CRC_OFFSET) {
            throw new AssertionError();
        }

        writeMetadata(offset, bb);
    }

    /**
     * @return uint32 counter value, or -1 if wrong magic number, or -2 if CRC doesn't match
     * @throws IOException if wrong magic number or encoding version
     */
    private long verifyMetadata(int offset) throws IOException {
        ByteBuffer bb = readMetadata(offset).clear().limit(CRC_OFFSET);

        if (bb.getLong() != MAGIC_NUMBER) {
            return -1;
        }

        mMetadataCrc.reset();
        mMetadataCrc.update(bb.position(0));

        bb.limit(METADATA_SIZE);
        int actual = bb.getInt();

        if (actual != (int) mMetadataCrc.getValue()) {
            return -2;
        }

        bb.position(8).limit(CRC_OFFSET);

        int encoding = bb.getInt();
        if (encoding != ENCODING_VERSION) {
            throw new IOException("Metadata encoding version is unknown: " + encoding);
        }

        return bb.getInt() & 0xffffffffL;
    }

    @Override
    public TermLog captureHighest(LogInfo info) {
        acquireShared();
        return doCaptureHighest(info, true);
    }

    // Must be called with shared latch held.
    private TermLog doCaptureHighest(LogInfo info, boolean releaseLatch) {
        TermLog highestLog = mHighestTermLog;

        if (highestLog == null) {
            if (mTermLogs.isEmpty()) {
                info.mTerm = 0;
                info.mHighestPosition = 0;
                info.mCommitPosition = 0;
                if (releaseLatch) {
                    releaseShared();
                }
                return null;
            }
            highestLog = (TermLog) mTermLogs.first();
        }

        while (true) {
            var nextLog = (TermLog) mTermLogs.higher(highestLog); // findGt

            highestLog.captureHighest(info);

            if (nextLog == null || info.mHighestPosition < nextLog.startPosition()) {
                if (highestLog != mHighestTermLog && tryUpgrade()) {
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
    public void commit(long commitPosition) {
        acquireShared();

        if (mCommitTermLog != null) {
            // Commit the known highest unfinished term.
            mCommitTermLog.commit(commitPosition);
            releaseShared();
            return;
        }

        if (mTermLogs.isEmpty()) {
            releaseShared();
            return;
        }

        // Pass the commit position to the terms in descending order, to prevent a race condition
        // with the doCaptureHighest method. It iterates in ascending order, assuming that the
        // commit position of a higher term cannot be lower. If the terms are committed in
        // ascending order, latch coupling would be required to prevent the doCaptureHighest
        // from going too far ahead. Descending order commit is simpler.

        Iterator<LKey<TermLog>> it = mTermLogs.descendingIterator();
        final var highestLog = (TermLog) it.next();
        TermLog termLog = highestLog;

        while (true) {
            termLog.commit(commitPosition);
            if (!it.hasNext()) {
                if (termLog == highestLog) {
                    break;
                }
                releaseShared();
                return;
            }
            var lowerLog = (TermLog) it.next();
            if (termLog == highestLog && lowerLog.isFinished()) {
                break;
            }
            termLog = lowerLog;
        }

        if (tryUpgrade()) {
            // There's no point in allocating an iterator each time, since this is the only
            // term which needs to be committed.
            mCommitTermLog = highestLog;
            releaseExclusive();
        } else {
            releaseShared();
        }
    }

    @Override
    public long potentialCommitPosition() {
        acquireShared();
        try {
            return mTermLogs.isEmpty() ? 0 : ((TermLog) mTermLogs.last()).potentialCommitPosition();
        } finally {
            releaseShared();
        }
    }

    @Override
    public long incrementCurrentTerm(int termIncrement, long candidateId) throws IOException {
        if (termIncrement <= 0) {
            throw new IllegalArgumentException();
        }
        mMetadataLatch.acquireExclusive();
        try {
            mCurrentTerm += termIncrement;
            mVotedForId = candidateId;
            doSync();
            return mCurrentTerm;
        } finally {
            mMetadataLatch.releaseExclusive();
        }
    }

    @Override
    public long checkCurrentTerm(long term) throws IOException {
        mMetadataLatch.acquireExclusive();
        try {
            if (term > mCurrentTerm) {
                mCurrentTerm = term;
                mVotedForId = 0;
                doSync();
            }
            return mCurrentTerm;
        } finally {
            mMetadataLatch.releaseExclusive();
        }
    }

    @Override
    public boolean checkCandidate(long candidateId) throws IOException {
        mMetadataLatch.acquireExclusive();
        try {
            if (mVotedForId == candidateId) {
                return true;
            }
            if (mVotedForId == 0) {
                mVotedForId = candidateId;
                doSync();
                return true;
            }
            return false;
        } finally {
            mMetadataLatch.releaseExclusive();
        }
    }

    @Override
    public void compact(long position) throws IOException {
        Iterator<LKey<TermLog>> it = mTermLogs.iterator();
        while (it.hasNext()) {
            var termLog = (TermLog) it.next();
            if (termLog.compact(position)) {
                termLog.close();
                it.remove();
            } else {
                break;
            }
        }
    }

    @Override
    public void truncateAll(long prevTerm, long term, long position) throws IOException {
        mMetadataLatch.acquireExclusive();
        try {
            if (mClosed) {
                throw new IOException("Closed");
            }

            TermLog highestLog;

            acquireExclusive();
            try {
                compact(Long.MAX_VALUE);

                // Create a new primordial term.
                highestLog = FileTermLog.newTerm(mCaches, mWorker, mBase, prevTerm, term,
                                                 position, position);
                mTermLogs.add(highestLog);
            } finally {
                releaseExclusive();
            }

            mMetadataInfo.mTerm = term;
            mMetadataInfo.mHighestPosition = position;
            mMetadataInfo.mCommitPosition = position;
            mMetadataHighestPosition = position;
            mMetadataDurablePosition = position;

            syncMetadata(highestLog);
        } finally {
            mMetadataLatch.releaseExclusive();
        }
    }

    @Override
    public boolean defineTerm(long prevTerm, long term, long position) throws IOException {
        return defineTermLog(prevTerm, term, position) != null;
    }

    // Must not be called if caller already holds the latch. Must call tryTermLogAt instead.
    @Override
    public TermLog termLogAt(long position) {
        TermLog term = tryTermLogAt(position);

        if (term == null) {
            // Try again with latch held, in case all terms have been concurrently removed by
            // the defineTermLog method, and the new one isn't inserted yet.
            acquireShared();
            term = tryTermLogAt(position);
            releaseShared();
        }

        return term;
    }

    private TermLog tryTermLogAt(long position) {
        return (TermLog) mTermLogs.floor(new LKey.Finder<>(position)); // findLe
    }

    @Override
    public void queryTerms(long startPosition, long endPosition, TermQuery results) {
        if (startPosition >= endPosition) {
            return;
        }

        LKey<TermLog> startKey = new LKey.Finder<>(startPosition);
        LKey<TermLog> endKey = new LKey.Finder<>(endPosition);

        LKey<TermLog> prev = mTermLogs.floor(startKey); // findLe
        if (prev != null && ((TermLog) prev).endPosition() > startPosition) {
            startKey = prev;
        }

        for (Object key : mTermLogs.subSet(startKey, endKey)) {
            TermLog termLog = (TermLog) key;
            results.term(termLog.prevTerm(), termLog.term(), termLog.startPosition());
        }
    }

    @Override
    public long checkForMissingData(long contigPosition, PositionRange results) {
        acquireShared();

        TermLog termLog = mContigTermLog;

        if (termLog == null) {
            if (mTermLogs.isEmpty()) {
                releaseShared();
                return 0;
            }
            termLog = (TermLog) mTermLogs.first();
        }

        final long originalContigPosition = contigPosition;

        TermLog nextLog;
        while (true) {
            nextLog = (TermLog) mTermLogs.higher(termLog); // findGt
            contigPosition = termLog.checkForMissingData(contigPosition, results);
            if (contigPosition < termLog.endPosition() || nextLog == null) {
                break;
            }
            termLog = nextLog;
        }

        if (termLog != mContigTermLog && tryUpgrade()) {
            mContigTermLog = termLog;
            if (nextLog == null) {
                releaseExclusive();
                return contigPosition;
            }
            downgrade();
        }

        if (contigPosition == originalContigPosition) {
            // Scan ahead into all remaining terms.
            while (nextLog != null) {
                nextLog.checkForMissingData(0, results);
                nextLog = (TermLog) mTermLogs.higher(nextLog); // findGt
            }
        }

        releaseShared();
        return contigPosition;
    }

    @Override
    public LogWriter openWriter(long prevTerm, long term, long position) throws IOException {
        TermLog termLog = defineTermLog(prevTerm, term, position);
        return termLog == null ? null : termLog.openWriter(position);
    }

    /**
     * @return null if not defined due to term mismatch
     */
    // package-private for testing
    TermLog defineTermLog(long prevTerm, long term, long position) throws IOException {
        LKey<TermLog> key = new LKey.Finder<>(position);

        int fullLatch = 0;
        acquireShared();
        try {
            TermLog termLog;
            defineTermLog: while (true) {
                termLog = (TermLog) mTermLogs.floor(key); // findLe

                while (true) {
                    if (termLog == null) {
                        break defineTermLog;
                    }
                    if (prevTerm == termLog.prevTermAt(position)) {
                        // Previous term matches.
                        break;
                    }
                    // Skip empty conflicting terms.
                    if (!termLog.hasPotentialCommit()) {
                        key = termLog; // update key for truncation
                        termLog = (TermLog) mTermLogs.lower(termLog); // findLt
                    } else {
                        checkCommitConflict(termLog, position);
                        termLog = null;
                    }
                }

                long actualTerm = termLog.term();

                if (term == actualTerm) {
                    // Term already exists.
                    break defineTermLog;
                }

                if (term < actualTerm || checkCommitConflict(termLog, position)) {
                    termLog = null;
                    break defineTermLog;
                }

                if (mClosed) {
                    throw new IOException("Closed");
                }

                if ((fullLatch = acquireFullLatch(fullLatch)) < 0) {
                    continue;
                }

                // Proceed to define a new term, and to delete conflicting terms.

                SortedSet<LKey<TermLog>> toDelete = mTermLogs.tailSet(key); // viewGe
                toDelete = new ConcurrentSkipListSet<>(toDelete); // need a copy, not a view
                mTermLogs.removeAll(toDelete);

                final TermLog newTermLog = FileTermLog.newTerm
                    (mCaches, mWorker, mBase, prevTerm, term,
                     position, termLog.potentialCommitPosition());
                mTermLogs.add(newTermLog);

                mHighestTermLog = fixTermRef(mHighestTermLog);
                mCommitTermLog = null; // re-assign when commit is called
                mContigTermLog = fixTermRef(mContigTermLog);

                // Same as doSync, except without re-acquiring any latches.
                {
                    TermLog highestLog = doCaptureHighest(mMetadataInfo, false);
                    highestLog.sync();
                    syncMetadata(highestLog);
                }

                // Can safely finish terms now that the metadata doesn't refer to them.

                termLog.finishTerm(position);

                // Delete all conflicting term logs. Iterator might see the term log just
                // finished, facilitating its deletion. Double finishing is harmless.
                Iterator<LKey<TermLog>> it = toDelete.iterator();
                while (it.hasNext()) {
                    termLog = (TermLog) it.next();
                    termLog.finishTerm(termLog.startPosition());
                    try {
                        termLog.close();
                    } catch (IOException e) {
                        // Ignore.
                    }
                }

                termLog = newTermLog;
                break defineTermLog;
            }

            return termLog;
        } finally {
            releaseFullLatch(fullLatch);
        }
    }

    /**
     * Called after adding a term, to make sure that the given term will reference a valid term
     * object.
     */
    private TermLog fixTermRef(TermLog termLog) {
        // When lower terms are extended, the reference might need to go lower, hence
        // findLe. The methods which examine terms are designed to start low, possibly at the
        // first term. Null implies that the first term should be examined, and so everything
        // should work fine even if this method always returns null. Finding a valid term
        // that's just lower avoids a full scan over the terms.
        return termLog == null ? null : (TermLog) mTermLogs.floor(termLog); // findLe
    }

    /**
     * @return true if conflict exists but state log is closed
     * @throws CommitConflictException if conflict exists and state log is open
     */
    private boolean checkCommitConflict(TermLog termLog, long position)
        throws CommitConflictException
    {
        if (termLog.hasPotentialCommit(position)) {
            if (mClosed) {
                return true;
            }
            if (!termLog.tryRollbackCommit(position)) {
                var termInfo = new LogInfo();
                termLog.captureHighest(termInfo);
                long durablePosition = mMetadataDurablePosition;
                throw new CommitConflictException(position, termInfo, durablePosition);
            }
        }
        return false;
    }

    /**
     * @param state 0: not full (shared), +/-1: full latch held
     * @return 1 or -1; if -1, caller must start over because latch was released briefly
     */
    private int acquireFullLatch(int state) {
        if (state == 0) {
            if (tryUpgrade()) {
                if (mMetadataLatch.tryAcquireExclusive()) {
                    return 1;
                }
                releaseExclusive();
            } else {
                releaseShared();
            }
            // Acquire in the canonical, deadlock-free order.
            mMetadataLatch.acquireExclusive();
            try {
                acquireExclusive();
            } catch (Throwable e) {
                mMetadataLatch.releaseExclusive();
                throw e;
            }
            return -1;
        }

        return 1;
    }

    /**
     * @param state 0: not full (shared), +/-1: full latch held
     */
    private void releaseFullLatch(int state) {
        if (state == 0) {
            releaseShared();
        } else {
            mMetadataLatch.releaseExclusive();
            releaseExclusive();
        }
    }

    @Override
    public LogReader openReader(long position) {
        LKey<TermLog> key = new LKey.Finder<>(position);

        acquireShared();
        try {
            if (mClosed) {
                throw new IllegalStateException("Closed");
            }

            var termLog = (TermLog) mTermLogs.floor(key); // findLe

            if (termLog != null) {
                return termLog.openReader(position);
            }

            termLog = (TermLog) mTermLogs.first();

            throw new InvalidReadException
                ("Position is lower than start position: " +
                 position + " < " + termLog.startPosition());
        } finally {
            releaseShared();
        }
    }

    @Override
    public boolean isReadable(long position) {
        LKey<TermLog> key = new LKey.Finder<>(position);

        acquireShared();
        try {
            if (mClosed) {
                throw new IllegalStateException("Closed");
            }
            var termLog = (TermLog) mTermLogs.floor(key); // findLe
            return termLog != null && termLog.isReadable(position);
        } finally {
            releaseShared();
        }
    }

    @Override
    public void sync() throws IOException {
        mMetadataLatch.acquireExclusive();
        try {
            doSync();
        } finally {
            mMetadataLatch.releaseExclusive();
        }
    }

    @Override
    public long syncCommit(long prevTerm, long term, long position) throws IOException {
        mMetadataLatch.acquireExclusive();
        try {
            if (mClosed) {
                throw new IOException("Closed");
            }

            TermLog highestLog;

            acquireShared();
            try {
                TermLog termLog = tryTermLogAt(position);

                if (termLog == null || term != termLog.term()
                    || prevTerm != termLog.prevTermAt(position))
                {
                    return -1;
                }

                highestLog = doCaptureHighest(mMetadataInfo, false);

                if (position > mMetadataInfo.mHighestPosition) {
                    return -1;
                }

                if (position <= mMetadataHighestPosition) {
                    // Nothing to do.
                    return mMetadataInfo.mCommitPosition;
                }

                highestLog.sync();
            } finally {
                releaseShared();
            }

            syncMetadata(highestLog);

            return mMetadataInfo.mCommitPosition;
        } finally {
            mMetadataLatch.releaseExclusive();
        }
    }

    @Override
    public boolean isDurable(long position) {
        mMetadataLatch.acquireShared();
        boolean result = position <= mMetadataDurablePosition;
        mMetadataLatch.releaseShared();
        return result;
    }

    @Override
    public boolean commitDurable(long position) throws IOException {
        mMetadataLatch.acquireExclusive();
        try {
            if (mClosed) {
                throw new IOException("Closed");
            }

            if (position <= mMetadataDurablePosition) {
                return false;
            }

            checkDurable(position, mMetadataHighestPosition);
            checkDurable(position, mMetadataInfo.mCommitPosition);

            syncMetadata(null, position);

            return true;
        } finally {
            mMetadataLatch.releaseExclusive();
        }
    }

    private static void checkDurable(long position, long highestPosition) {
        if (position > highestPosition) {
            throw new IllegalStateException("Commit position is too high: " + position
                                            + " > " + highestPosition);
        }
    }

    // Caller must exclusively hold mMetadataLatch.
    private void doSync() throws IOException {
        if (mClosed) {
            return;
        }

        TermLog highestLog;

        acquireShared();
        try {
            highestLog = doCaptureHighest(mMetadataInfo, false);
            if (highestLog != null) {
                highestLog.sync();
            }
        } finally {
            releaseShared();
        }

        syncMetadata(highestLog);
    }

    // Caller must exclusively hold mMetadataLatch.
    private void syncMetadata(TermLog highestLog) throws IOException {
        syncMetadata(highestLog, -1);
    }

    // Caller must exclusively hold mMetadataLatch.
    private void syncMetadata(TermLog highestLog, long durablePosition) throws IOException {
        if (mClosed) {
            return;
        }

        boolean durableOnly;
        if (durablePosition >= 0) {
            // Only updating the durable position (called by commitDurable).
            durableOnly = true;
        } else {
            durablePosition = mMetadataDurablePosition;
            durableOnly = false;
        }

        if (mMetadataInfo.mHighestPosition < durablePosition) {
            throw new IllegalStateException
                ("Highest position is lower than durable position: " +
                 mMetadataInfo.mHighestPosition + " < " + durablePosition);
        }

        ByteBuffer bb = mMetadataBuffer;
        int counter = mMetadataCounter;

        long highestPrevTerm, highestTerm, highestPosition;

        if (durableOnly) {
            // Leave the existing highest log field values alone, since no data was sync'd.
            bb.position(HIGHEST_PREV_TERM_OFFSET).limit(METADATA_SIZE);
            highestPrevTerm = bb.getLong();
            highestTerm = bb.getLong();
            highestPosition = bb.getLong();
        } else {
            highestTerm = mMetadataInfo.mTerm;
            highestPosition = mMetadataInfo.mHighestPosition;
            highestPrevTerm = highestLog == null ?
                highestTerm : highestLog.prevTermAt(highestPosition);
        }

        counter += 1;

        bb.position(COUNTER_OFFSET).limit(CRC_OFFSET);

        bb.putInt(counter);
        bb.putLong(mCurrentTerm);
        bb.putLong(mVotedForId);
        bb.putLong(highestPrevTerm);
        bb.putLong(highestTerm);
        bb.putLong(highestPosition);
        bb.putLong(durablePosition);

        if (bb.position() != CRC_OFFSET) {
            throw new AssertionError();
        }

        writeMetadata((counter & 1) << SECTION_POW, bb);

        // Update field values only after successful file I/O.
        mMetadataCounter = counter;

        if (durableOnly) {
            mMetadataDurablePosition = durablePosition;
        } else {
            mMetadataHighestPosition = mMetadataInfo.mHighestPosition;
        }
    }

    @Override
    public void close() throws IOException {
        mMetadataLatch.acquireExclusive();
        try {
            acquireExclusive();
            try {
                if (mClosed) {
                    return;
                }

                mClosed = true;

                try {
                    mMetadataLock.close();
                } catch (Throwable e) {
                    // Ignore.
                }

                Utils.closeQuietly(mMetadataFile);

                for (Object key : mTermLogs) {
                    ((TermLog) key).close();
                }
            } finally {
                releaseExclusive();
            }
        } finally {
            mMetadataLatch.releaseExclusive();
        }

        synchronized (mWorker) {
            mWorker.join(true);
        }
    }
}
