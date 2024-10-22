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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.io.File;
import java.io.InterruptedIOException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.NavigableSet;
import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;
import java.util.SortedSet;

import java.util.concurrent.ConcurrentSkipListSet;

import java.util.function.BiFunction;
import java.util.function.LongConsumer;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.cojen.tupl.io.FileIO;
import org.cojen.tupl.io.LengthOption;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Parker;
import org.cojen.tupl.util.Worker;

/**
 * Standard TermLog implementation which stores data in file segments.
 *
 * @author Brian S O'Neill
 */
final class FileTermLog extends Latch implements TermLog {
    private static final int EOF = -1;
    private static final int WAIT_TERM_END = EOF;
    private static final int WAIT_TIMEOUT = -2;

    // For sizing LCache instances.
    private static final int MIN_CACHE_SIZE = 10;

    private static final ThreadLocal<CommitWaiter> cLocalWaiter = new ThreadLocal<>();

    private final Worker mWorker;
    private final File mBase;
    private final long mLogPrevTerm;
    private final long mLogTerm;
    private final long mLogStartPosition;

    private volatile long mLogVersion;

    /*
      In general, legal position values are bounded as follows:

        start <= commit <= highest <= contig <= end

      Commit position field can be larger than the highest position, but
      the appliable position is: min(commit, highest).
    */

    private long mLogCommitPosition;
    private long mLogHighestPosition;
    private long mLogContigPosition;
    private long mLogEndPosition;

    // Segments are keyed only by their start position.
    private final NavigableSet<LKey<Segment>> mSegments;

    private final PriorityQueue<SegmentWriter> mNonContigWriters;

    private final Caches mCaches;

    private LongConsumer[] mCommitListeners;
    private long mLastCommitListenerPos;

    private final PriorityQueue<CommitCallback> mCommitTasks;

    private final Latch mSyncLatch;
    private final Latch mDirtyLatch;
    private Segment mFirstDirty;
    private Segment mLastDirty;

    private boolean mLogClosed;

    static final VarHandle cLogClosedHandle;

    static {
        try {
            cLogClosedHandle =
                MethodHandles.lookup().findVarHandle
                (FileTermLog.class, "mLogClosed", boolean.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    static class Caches {
        final LCache<Segment, FileTermLog> mSegments = new LCache<>(MIN_CACHE_SIZE);
        final LCache<SegmentWriter, FileTermLog> mWriters = new LCache<>(MIN_CACHE_SIZE);
        final LCache<SegmentReader, FileTermLog> mReaders = new LCache<>(MIN_CACHE_SIZE);
    }

    /**
     * Create a new term.
     *
     * @param base can pass null to persist nothing, and reads are unsupported
     */
    static TermLog newTerm(Caches caches, Worker worker, File base, long prevTerm, long term,
                           long startPosition, long commitPosition)
        throws IOException
    {
        if (base != null) {
            base = checkBase(base);
        }

        var termLog = new FileTermLog
            (caches, worker, base, prevTerm, term,
             startPosition, commitPosition, startPosition, null);

        return termLog;
    }

    /**
     * Create or open an existing term.
     *
     * @param prevTerm pass -1 to discover the prev term
     * @param startPosition pass -1 to discover the start position
     * @param segmentFileNames pass null to discover segment files
     */
    static TermLog openTerm(Caches caches, Worker worker, File base, long prevTerm, long term,
                            long startPosition, long commitPosition, long highestPosition,
                            List<String> segmentFileNames)
        throws IOException
    {
        base = checkBase(base);

        if (segmentFileNames == null) {
            String[] namesArray = base.getParentFile().list();

            if (namesArray != null && namesArray.length != 0) {
                // This pattern matches the term, and it discards the optional prevTerm.
                Pattern p = Pattern.compile(base.getName() + "\\." + term + "\\.\\d+(?:\\.\\d+)?");

                segmentFileNames = new ArrayList<>();
                for (String name : namesArray) {
                    if (p.matcher(name).matches()) {
                        segmentFileNames.add(name);
                    }
                }
            }
        }

        var termLog = new FileTermLog
            (caches, worker, base, prevTerm, term,
             startPosition, commitPosition, highestPosition, segmentFileNames);

        return termLog;
    }

    static File checkBase(File base) {
        base = base.getAbsoluteFile();

        if (base.isDirectory()) {
            throw new IllegalArgumentException("Base file is a directory: " + base);
        }

        if (!base.getParentFile().exists()) {
            throw new IllegalArgumentException("Parent file doesn't exist: " + base);
        }

        return base;
    }

    /**
     * @param prevTerm pass -1 to discover the prev term
     * @param startPosition pass -1 to discover the start position
     * @param segmentFileNames pass null when creating a term
     */
    private FileTermLog(Caches caches, Worker worker, File base, long prevTerm, long term,
                        long startPosition, final long commitPosition, final long highestPosition,
                        List<String> segmentFileNames)
        throws IOException
    {
        if (term < 0) {
            throw new IllegalArgumentException("Illegal term: " + term);
        }

        if (commitPosition > highestPosition) {
            throw new IllegalArgumentException("Commit position is higher than highest position: "
                                               + commitPosition + " > " + highestPosition);
        }

        mWorker = worker;
        mBase = base;
        mLogTerm = term;

        mSegments = new ConcurrentSkipListSet<>();

        if (segmentFileNames != null && segmentFileNames.size() != 0) {
            // Open the existing segments.

            File parent = base.getParentFile();

            // This pattern captures required start position and the optional prevTerm.
            Pattern p = Pattern.compile(base.getName() + "\\." + term + "\\.(\\d+)(?:\\.(\\d+))?");

            boolean anyMatches = false;

            for (String name : segmentFileNames) {
                Matcher m = p.matcher(name);

                if (!m.matches()) {
                    continue;
                }

                anyMatches = true;

                long start = Long.parseLong(m.group(1));

                String prevTermStr = m.group(2);

                if (prevTermStr != null) {
                    long parsedPrevTerm = Long.parseLong(prevTermStr);
                    if (prevTerm < 0) {
                        prevTerm = parsedPrevTerm;
                    } else if (prevTerm != parsedPrevTerm) {
                        throw new IllegalStateException
                            ("Mismatched previous term: " + prevTerm + " != " + prevTermStr);
                    }
                }

                // Start with the desired max length, and then truncate on the second pass.
                long maxLength = Math.max(maxSegmentLength(), new File(parent, name).length());

                mSegments.add(new Segment(start, maxLength));
            }

            if (prevTerm < 0 && anyMatches) {
                prevTerm = term;
            }
        }

        if (prevTerm < 0) {
            throw new IllegalStateException("Unable to determine previous term");
        }

        if (startPosition == -1) {
            if (mSegments.isEmpty()) {
                throw new IllegalStateException("No segment files exist for term: " + term);
            }
            startPosition = ((Segment) mSegments.first()).mStartPosition;
        } else if (startPosition < highestPosition) {
            if (mSegments.isEmpty()
                || ((Segment) mSegments.first()).mStartPosition > startPosition)
            {
                throw new IllegalStateException
                    ("Missing start segment: " + startPosition + ", term=" + term);
            }
        }

        mLogPrevTerm = prevTerm;
        mLogStartPosition = startPosition;
        mLogCommitPosition = commitPosition;
        mLogHighestPosition = highestPosition;
        mLogContigPosition = highestPosition;
        mLogEndPosition = Long.MAX_VALUE;

        // Contiguous segments must exist from start to highest.

        forEachSegment(mSegments.tailSet(new LKey.Finder<>(startPosition)), (seg, next) -> {
            if (seg.mStartPosition >= highestPosition) {
                return false;
            }
            if (next != null) {
                File file = seg.file();
                long segHighest = seg.mStartPosition + file.length();
                if (segHighest < highestPosition && segHighest < next.mStartPosition) {
                    throw new IllegalStateException("Incomplete segment: " + file);
                }
            }
            return true;
        });

        // Truncate the segments if necessary, based on the start position of the successor.

        forEachSegment(mSegments, (seg, next) -> {
            if (next != null
                && seg.endPosition() > next.mStartPosition
                && seg.setEndPosition(next.mStartPosition)
                && seg.file().length() > seg.mMaxLength)
            {
                try {
                    seg.truncate();
                } catch (IOException e) {
                    throw Utils.rethrow(e);
                }
            }
            return true;
        });

        // Delete segments which are out of bounds.
        
        forEachSegment(mSegments, (seg, next) -> {
            if (seg.endPosition() <= mLogStartPosition
                || seg.mStartPosition >= mLogHighestPosition)
            {
                try {
                    Utils.delete(seg.file());
                } catch (IOException e) {
                    throw Utils.rethrow(e);
                }
                mSegments.remove(seg);
            }
            return true;
        });

        mCaches = caches;

        mSyncLatch = new Latch();
        mDirtyLatch = new Latch();

        mNonContigWriters = new PriorityQueue<>();
        mCommitTasks = new PriorityQueue<>();
    }

    /**
     * Iterates over each segment, also providing the successor, if it exists.
     *
     * @param pairConsumer return false to stop iterating
     */
    private static void forEachSegment(SortedSet<LKey<Segment>> segments,
                                       BiFunction<Segment, Segment, Boolean> pairConsumer)
    {
        Iterator<LKey<Segment>> it = segments.iterator();

        if (it.hasNext()) {
            var seg = (Segment) it.next();
            while (true) {
                Segment next;
                if (it.hasNext()) {
                    next = (Segment) it.next();
                } else {
                    next = null;
                }
                if (!pairConsumer.apply(seg, next)) {
                    return;
                }
                if (next == null) {
                    break;
                }
                seg = next;
            }
        }
    }

    @Override
    public long prevTerm() {
        return mLogPrevTerm;
    }

    @Override
    public long term() {
        return mLogTerm;
    }

    @Override
    public long startPosition() {
        return mLogStartPosition;
    }

    @Override
    public long prevTermAt(long position) {
        return position <= mLogStartPosition ? mLogPrevTerm : mLogTerm;
    }

    @Override
    public boolean compact(long startPosition) throws IOException {
        acquireShared();
        boolean full = startPosition >= mLogEndPosition;
        releaseShared();

        Iterator<LKey<Segment>> it = mSegments.iterator();
        while (it.hasNext()) {
            var seg = (Segment) it.next();

            long endPosition = seg.endPosition();
            if (endPosition > startPosition) {
                break;
            }

            seg.acquireExclusive();
            try {
                seg.close(true);
            } finally {
                seg.releaseExclusive();
            }

            File segFile = seg.file();

            if (segFile != null && !segFile.delete() && segFile.exists()) {
                // If segment can't be deleted for some reason, try again later instead of
                // creating a potential segment gap.
                break;
            }

            it.remove();

            mCaches.mSegments.remove(seg.cacheKey(), this);
        }

        return full && mSegments.isEmpty();
    }

    @Override
    public long potentialCommitPosition() {
        acquireShared();
        long position = mLogCommitPosition;
        releaseShared();
        return position;
    }

    @Override
    public long endPosition() {
        acquireShared();
        long position = mLogEndPosition;
        releaseShared();
        return position;
    }

    @Override
    public void captureHighest(LogInfo info) {
        info.mTerm = mLogTerm;
        acquireShared();
        doCaptureHighest(info);
        releaseShared();
    }

    @Override
    public boolean isFinished() {
        acquireShared();
        boolean result = doAppliableCommitPosition() >= mLogEndPosition;
        releaseShared();
        return result;
    }

    // Caller must hold any latch.
    private void doCaptureHighest(LogInfo info) {
        info.mHighestPosition = mLogHighestPosition;
        info.mCommitPosition = doAppliableCommitPosition();
    }

    long appliableCommitPosition() {
        acquireShared();
        long commitPosition = doAppliableCommitPosition();
        releaseShared();
        return commitPosition;
    }

    // Caller must hold any latch.
    private long doAppliableCommitPosition() {
        return Math.min(mLogCommitPosition, mLogHighestPosition);
    }

    @Override
    public void commit(long commitPosition) {
        acquireExclusive();
        if (commitPosition > mLogCommitPosition) {
            long endPosition = mLogEndPosition;
            if (commitPosition > endPosition) {
                commitPosition = endPosition;
            }
            mLogCommitPosition = commitPosition;
            if (mLogHighestPosition < commitPosition) {
                mLogHighestPosition = Math.min(commitPosition, mLogContigPosition);
            }
            notifyCommitTasks(doAppliableCommitPosition());
            return;
        }
        releaseExclusive();
    }

    void addCommitListener(LongConsumer listener) {
        Objects.requireNonNull(listener);

        acquireExclusive();

        if (doAppliableCommitPosition() >= mLogEndPosition) {
            downgrade();
            try {
                listener.accept(mLogEndPosition);
                listener.accept(WAIT_TERM_END);
            } finally {
                releaseShared();
            }
            return;
        }

        try {
            if (mCommitListeners == null) {
                mCommitListeners = new LongConsumer[1];
            } else {
                mCommitListeners = Arrays.copyOf(mCommitListeners, mCommitListeners.length + 1);
            }
            mCommitListeners[mCommitListeners.length - 1] = listener;
        } finally {
            releaseExclusive();
        }

    }

    @Override
    public long waitForCommit(long position, long nanosTimeout) throws InterruptedIOException {
        return waitForCommit(position, nanosTimeout, this);
    }

    long waitForCommit(long position, long nanosTimeout, Object waiter)
        throws InterruptedIOException
    {
        boolean exclusive = false;
        acquireShared();
        while (true) {
            long commitPosition = doAppliableCommitPosition();
            if (commitPosition >= position) {
                release(exclusive);
                return commitPosition;
            }
            if (position > mLogEndPosition || mLogClosed) {
                release(exclusive);
                return WAIT_TERM_END;
            }
            if (exclusive || tryUpgrade()) {
                break;
            }
            releaseShared();
            acquireExclusive();
            exclusive = true;
        }

        CommitWaiter cwaiter;
        try {
            cwaiter = cLocalWaiter.get();
            if (cwaiter == null) {
                cwaiter = new CommitWaiter(position, Thread.currentThread(), waiter);
                cLocalWaiter.set(cwaiter);
            } else {
                cwaiter.mPosition = position;
                cwaiter.mWaiter = waiter;
                cwaiter.mAppliablePosition = 0;
            }

            mCommitTasks.add(cwaiter);
        } finally {
            releaseExclusive();
        }

        long endNanos = nanosTimeout > 0 ? (System.nanoTime() + nanosTimeout) : 0;

        while (true) {
            if (nanosTimeout < 0) {
                Parker.park(waiter);
            } else {
                Parker.parkNanos(waiter, nanosTimeout);
            }
            if (Thread.interrupted()) {
                throw new InterruptedIOException();
            }
            long commitPosition = cwaiter.mAppliablePosition;
            if (commitPosition < 0) {
                return commitPosition;
            }
            if (commitPosition >= position) {
                return commitPosition;
            }
            if (nanosTimeout == 0
                || (nanosTimeout > 0 && (nanosTimeout = endNanos - System.nanoTime()) <= 0))
            {
                return WAIT_TIMEOUT;
            }
        }
    }

    void signalClosed(Object waiter) {
        acquireShared();
        try {
            for (CommitCallback task : mCommitTasks) {
                if (task instanceof CommitWaiter cwaiter && cwaiter.mWaiter == waiter) {
                    reached(cwaiter, WAIT_TERM_END);
                }
            }
        } finally {
            releaseShared();
        }
    }

    static class CommitWaiter extends CommitCallback {
        final Thread mThread;
        Object mWaiter;
        volatile long mAppliablePosition;

        CommitWaiter(long position, Thread thread, Object waiter) {
            super(position);
            mThread = thread;
            mWaiter = waiter;
        }

        @Override
        public void reached(long position) {
            mWaiter = null;
            mAppliablePosition = position;
            Parker.unpark(mThread);
        }
    }

    @Override
    public void uponCommit(CommitCallback task) {
        if (task == null) {
            throw new NullPointerException();
        }

        uponExclusive(() -> {
            long commitPosition = doAppliableCommitPosition();

            final long waitFor = task.mPosition;

            if (commitPosition < waitFor) {
                if (mLogClosed || waitFor > mLogEndPosition) {
                    commitPosition = WAIT_TERM_END;
                } else {
                    mCommitTasks.add(task);
                    return;
                }
            }

            // Note that shared latch is still held, and that exclusive latch must be acquired
            // first. See notifyCommitTasks.
            downgrade();
            reached(task, commitPosition);
        });
    }

    @Override
    public boolean tryRollbackCommit(long position) {
        acquireExclusive();
        try {
            if (position < mLogStartPosition || position < doAppliableCommitPosition()) {
                return false;
            }
            mLogCommitPosition = position;
            return true;
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public void finishTerm(long endPosition) {
        LongConsumer[] commitListeners = null;
        List<CommitCallback> removedTasks;

        acquireExclusive();
        try {
            long commitPosition = mLogCommitPosition;
            if (endPosition < commitPosition && commitPosition > mLogStartPosition) {
                throw new IllegalStateException
                    ("Cannot finish term below commit position: " + endPosition
                     + " < " + commitPosition + "; term: " + mLogTerm);
            }

            if (endPosition == mLogEndPosition) {
                releaseExclusive();
                return;
            }

            mLogVersion++;

            if (endPosition >= mLogEndPosition) {
                // Wait for all work tasks to complete, ensuring that any lingering truncate
                // operations are finished before extending the term.
                synchronized (mWorker) {
                    mWorker.join(false);
                }
                mLogEndPosition = endPosition;
                releaseExclusive();
                return;
            }

            for (LKey<Segment> key : mSegments) {
                var segment = (Segment) key;
                segment.acquireExclusive();
                boolean shouldTruncate = segment.setEndPosition(endPosition);
                segment.releaseExclusive();
                if (shouldTruncate && !mLogClosed) {
                    truncate(segment);
                }
            }

            mLogEndPosition = endPosition;

            if (endPosition < mLogContigPosition) {
                mLogContigPosition = endPosition;
            }

            if (endPosition < mLogHighestPosition) {
                mLogHighestPosition = endPosition;
            }

            if (!mNonContigWriters.isEmpty()) {
                List<SegmentWriter> replacements = null;

                Iterator<SegmentWriter> it = mNonContigWriters.iterator();
                while (it.hasNext()) {
                    SegmentWriter writer = it.next();
                    if (writer.mWriterStartPosition >= endPosition) {
                        // The writer is completely ahead of the term end position.
                        it.remove();
                        continue;
                    }

                    if (endPosition < writer.mWriterHighestPosition) {
                        writer.mWriterHighestPosition = endPosition;
                    }

                    if (endPosition <= writer.mWriterPosition) {
                        // The writer overlaps the term end position. Remove it from the queue
                        // and replace it with a non-cached instance to be used for gap filling
                        // later. If the writer isn't removed, and the term is later extended,
                        // then the contiguous region might falsely advance too far ahead.
                        it.remove();

                        var replacement = new SegmentWriter();
                        replacement.mWriterVersion = writer.mWriterVersion;
                        replacement.mWriterPrevTerm = writer.mWriterPrevTerm;
                        replacement.mWriterStartPosition = writer.mWriterStartPosition;
                        replacement.mWriterPosition = endPosition;
                        replacement.mWriterHighestPosition = writer.mWriterHighestPosition;

                        if (replacements == null) {
                            replacements = new ArrayList<>();
                        }

                        replacements.add(replacement);
                    }
                }

                if (replacements != null) {
                    for (SegmentWriter writer : replacements) {
                        mNonContigWriters.add(writer);
                    }
                }
            }

            removedTasks = new ArrayList<>();

            if (endPosition <= mLastCommitListenerPos) {
                commitListeners = mCommitListeners;
                mCommitListeners = null;
            }

            mCommitTasks.removeIf(task -> {
                if (task.mPosition > endPosition) {
                    removedTasks.add(task);
                    return true;
                }
                return false;
            });
        } catch (Throwable e) {
            releaseExclusive();
            throw e;
        }

        downgrade();
        try {
            // Call with shared latch held for consistency. See notifyCommitTasks.

            if (commitListeners != null) {
                notifyCommitListeners(commitListeners, WAIT_TERM_END);
            }

            for (CommitCallback task : removedTasks) {
                reached(task, WAIT_TERM_END);
            }
        } finally {
            releaseShared();
        }
    }

    @Override
    public long contigPosition() {
        acquireShared();
        long position = mLogContigPosition;
        releaseShared();
        return position;
    }

    @Override
    public long checkForMissingData(long contigPosition, PositionRange results) {
        acquireShared();
        try {
            if (contigPosition < mLogStartPosition || mLogContigPosition == contigPosition) {
                long expectedPosition = mLogEndPosition;
                if (expectedPosition == Long.MAX_VALUE) {
                    expectedPosition = mLogCommitPosition;
                }

                long missingStartPosition = mLogContigPosition;

                if (!mNonContigWriters.isEmpty()) {
                    SegmentWriter[] writers = mNonContigWriters.toArray
                        (new SegmentWriter[mNonContigWriters.size()]);
                    Arrays.sort(writers);

                    for (SegmentWriter writer : writers) {
                        long missingEndPosition = writer.mWriterStartPosition;
                        if (missingStartPosition < missingEndPosition) {
                            results.range(missingStartPosition, missingEndPosition);
                        }
                        missingStartPosition = writer.mWriterPosition;
                    }
                }

                if (missingStartPosition < expectedPosition) {
                    results.range(missingStartPosition, expectedPosition);
                }
            }

            return mLogContigPosition;
        } finally {
            releaseShared();
        }
    }

    @Override
    public LogWriter openWriter(long startPosition) {
        SegmentWriter writer = mCaches.mWriters.remove(startPosition, this);

        if (writer == null) {
            writer = new SegmentWriter();
            acquireExclusive();
            try {
                init(writer, startPosition);
            } finally {
                releaseExclusive();
            }
        }

        return writer;
    }

    // Caller must hold exclusive latch.
    private void init(SegmentWriter writer, long startPosition) {
        writer.mWriterVersion = mLogVersion;
        writer.mWriterPrevTerm = startPosition == mLogStartPosition ? mLogPrevTerm : mLogTerm;
        writer.mWriterStartPosition = startPosition;
        writer.mWriterPosition = startPosition;
        writer.mWriterHighestPosition = 0;

        if (startPosition > mLogContigPosition && startPosition < mLogEndPosition) {
            mNonContigWriters.add(writer);
            // Boost the cache size to track all the non-contiguous writers. Note that the
            // cache size isn't reduced when removing non-contiguous writers, although it could
            // be. The call here effectively reduces the cache size instead.
            mCaches.mWriters.maxSize(Math.max(MIN_CACHE_SIZE, mNonContigWriters.size()));
        }
    }

    @Override
    public LogReader openReader(long startPosition) {
        SegmentReader reader = mCaches.mReaders.remove(startPosition, this);

        if (reader == null) {
            acquireShared();
            long prevTerm = startPosition <= mLogStartPosition ? mLogPrevTerm : mLogTerm;
            releaseShared();
            reader = new SegmentReader(prevTerm, startPosition);
        }

        return reader;
    }

    @Override
    public boolean isReadable(long position) {
        return position >= mLogStartPosition && position <= appliableCommitPosition();
    }

    @Override
    public void sync() throws IOException {
        IOException ex = null;

        mSyncLatch.acquireExclusive();
        doSync: {
            mDirtyLatch.acquireExclusive();

            Segment segment = mFirstDirty;
            if (segment == null) {
                mDirtyLatch.releaseExclusive();
                break doSync;
            }

            Segment last = mLastDirty;
            Segment next = segment.mNextDirty;

            mFirstDirty = next;
            if (next == null) {
                mLastDirty = null;
            } else {
                segment.mNextDirty = null;
            }

            mDirtyLatch.releaseExclusive();

            while (true) {
                try {
                    segment.sync();
                } catch (IOException e) {
                    if (ex == null) {
                        ex = e;
                    }
                }

                if (segment == last) {
                    break doSync;
                }

                segment = next;

                mDirtyLatch.acquireExclusive();

                next = segment.mNextDirty;
                mFirstDirty = next;
                if (next == null) {
                    mLastDirty = null;
                } else {
                    segment.mNextDirty = null;
                }

                mDirtyLatch.releaseExclusive();
            }
        }

        mSyncLatch.releaseExclusive();

        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public void close() throws IOException {
        mSyncLatch.acquireShared();
        try {
            List<CommitCallback> waiterTasks;

            acquireExclusive();
            try {
                // Wait for any pending truncate tasks to complete first. New tasks cannot be
                // enqueued with exclusive latch held.
                synchronized (mWorker) {
                    mWorker.join(false);
                }

                cLogClosedHandle.setVolatile(this, true);

                for (LKey<Segment> key : mSegments) {
                    var segment = (Segment) key;
                    segment.acquireExclusive();
                    try {
                        segment.close(true);
                    } finally {
                        segment.releaseExclusive();
                    }
                }

                waiterTasks = new ArrayList<>();

                mCommitTasks.removeIf(task -> {
                    if (task instanceof CommitWaiter) {
                        // Wake up blocked threads.
                        waiterTasks.add(task);
                    }
                    return true;
                });
            } catch (Throwable e) {
                releaseExclusive();
                throw e;
            }

            downgrade();
            try {
                // Call with shared latch held for consistency. See notifyCommitTasks.
                for (CommitCallback task : waiterTasks) {
                    reached(task, WAIT_TERM_END);
                }
            } finally {
                releaseShared();
            }
        } finally {
            mSyncLatch.releaseShared();
        }
    }

    @Override
    public String toString() {
        return "TermLog{prevTerm=" + mLogPrevTerm + ", term=" + mLogTerm +
            ", startPosition=" + mLogStartPosition + ", commitPosition=" + mLogCommitPosition +
            ", highestPosition=" + mLogHighestPosition + ", contigPosition=" + mLogContigPosition +
            ", endPosition=" + mLogEndPosition + '}';
    }

    /**
     * Add to the dirty list of segments, but caller must ensure that segment isn't already in
     * the dirty list.
     */
    void addToDirtyList(Segment segment) {
        mDirtyLatch.acquireExclusive();
        Segment last = mLastDirty;
        if (last == null) {
            mFirstDirty = segment;
        } else {
            last.mNextDirty = segment;
        }
        mLastDirty = segment;
        mDirtyLatch.releaseExclusive();
    }

    /**
     * @return null if position is at or higher than end position, or if segment doesn't exist
     * and is lower than the commit position
     */
    Segment segmentForWriting(SegmentWriter writer, long position) throws IOException {
        LKey<Segment> key = new LKey.Finder<>(position);

        acquireExclusive();
        try {
            if (writer.mWriterVersion != mLogVersion) {
                // Re-init the writer as if it was just opened, starting from where the
                // in-progress write operation began. This prevents creation of false
                // contiguous data when the term is truncated and later extended.
                mNonContigWriters.remove(writer);
                init(writer, writer.mWriterPosition);
            }

            if (position >= mLogEndPosition) {
                return null;
            }

            var startSegment = (Segment) mSegments.floor(key); // findLe

            if (startSegment != null && position < startSegment.endPosition()) {
                mCaches.mSegments.remove(startSegment.cacheKey(), this);
                cRefCountHandle.getAndAdd(startSegment, 1);
                return startSegment;
            }

            if (position < Math.max(mLogStartPosition, doAppliableCommitPosition())) {
                // Don't create segments for committed data.
                return null;
            }

            if (mLogClosed) {
                throw new IOException("Closed");
            }

            long maxLength = mBase != null ? maxSegmentLength() : (1L << 32);
            long startPosition = position;

            if (startSegment != null) {
                long segMaxLength = startSegment.mMaxLength;
                long segEndPosition = startSegment.mStartPosition + segMaxLength;
                startPosition = segEndPosition
                    + ((position - segEndPosition) / maxLength) * maxLength;
            }

            // Don't allow segment to encroach on next segment or go beyond term end.
            var nextSegment = (Segment) mSegments.higher(key); // findGt
            long endPosition = nextSegment == null ? mLogEndPosition : nextSegment.mStartPosition;
            maxLength = Math.min(maxLength, endPosition - startPosition);

            var segment = new Segment(startPosition, maxLength);

            if (!mSegments.add(segment)) {
                // Untruncate the startSegment and keep using it.
                if (startPosition != startSegment.mStartPosition || startSegment.mMaxLength != 0) {
                    throw new AssertionError(startSegment);
                }
                startSegment.untruncate(maxLength);
                segment = startSegment;
            }

            return segment;
        } finally {
            releaseExclusive();
        }
    }

    private long maxSegmentLength() {
        // 1, 2, 4, 8, 16, 32 or 64 MiB
        return (1024 * 1024L) << Math.min(6, mSegments.size());
    }

    /**
     * @return null if segment doesn't exist
     */
    Segment segmentForReading(long position) throws IOException {
        LKey<Segment> key = new LKey.Finder<>(position);

        acquireExclusive();
        try {
            var segment = (Segment) mSegments.floor(key); // findLe
            if (segment != null && position < segment.endPosition()) {
                cRefCountHandle.getAndAdd(segment, 1);
                return segment;
            }

            long commitPosition = Math.max(mLogStartPosition, doAppliableCommitPosition());

            if (position < commitPosition) {
                throw new InvalidReadException
                    ("Position is too low: " + position + " < " + commitPosition +
                     "; term: " + mLogTerm);
            }
        } finally {
            releaseExclusive();
        }

        return null;
    }

    /**
     * Called by SegmentWriter.
     */
    void writeFinished(SegmentWriter writer, long currentPosition, long highestPosition) {
        acquireExclusive();

        long commitPosition = mLogCommitPosition;
        if (highestPosition < commitPosition) {
            long allowedHighestPosition = Math.min(commitPosition, mLogContigPosition);
            if (highestPosition < allowedHighestPosition) {
                highestPosition = allowedHighestPosition;
            }
        }

        long endPosition = mLogEndPosition;
        if (currentPosition > endPosition) {
            currentPosition = endPosition;
        }
        if (highestPosition > endPosition) {
            highestPosition = endPosition;
        }

        // Ensure that other threads can read the position safely.
        cWriterPositionHandle.setOpaque(writer, currentPosition);

        if (currentPosition > writer.mWriterStartPosition) {
            writer.mWriterPrevTerm = mLogTerm;
        }

        if (highestPosition > writer.mWriterHighestPosition) {
            writer.mWriterHighestPosition = highestPosition;
        }

        long contigPosition = mLogContigPosition;
        if (writer.mWriterStartPosition <= contigPosition) {
            // Writer is in the contiguous region -- check if it's growing now.
            if (currentPosition > contigPosition) {
                contigPosition = currentPosition;

                long fallbackHighest = 0;

                // Remove non-contiguous writers that are now in the contiguous region.
                while (true) {
                    SegmentWriter next = mNonContigWriters.peek();
                    if (next == null || next.mWriterStartPosition > contigPosition) {
                        break;
                    }

                    SegmentWriter removed = mNonContigWriters.remove();
                    assert removed == next;

                    if (next.mWriterPosition > contigPosition) {
                        contigPosition = next.mWriterPosition;
                    }

                    // Advance the highest position, if possible.
                    long nextHighest = next.mWriterHighestPosition;
                    if (nextHighest > highestPosition && highestPosition <= contigPosition) {
                        highestPosition = nextHighest;
                    }

                    fallbackHighest = Math.max(fallbackHighest, nextHighest);
                }

                if (contigPosition > endPosition) {
                    // Although the contigPosition could be clamped to the endPosition, this
                    // case shouldn't happen unless there's something wrong with the term
                    // extension logic.
                    releaseExclusive();
                    throw new AssertionError(contigPosition + " > " + endPosition);
                }

                mLogContigPosition = contigPosition;

                if (highestPosition > contigPosition) {
                    // It won't be applied if too high, so try to use a highest position which
                    // was observed earlier.
                    highestPosition = fallbackHighest;
                }
            }

            applyHighest: {
                if (contigPosition == endPosition || contigPosition <= commitPosition) {
                    // The contig position is guaranteed to be a valid highest position (no message
                    // tearing is possible), so allow the appliable commit position to advance.
                    highestPosition = contigPosition;
                } else if (highestPosition > contigPosition) {
                    // Can't apply higher than what's available.
                    break applyHighest;
                }
                if (highestPosition > mLogHighestPosition) {
                    mLogHighestPosition = highestPosition;
                    doCaptureHighest(writer);
                    notifyCommitTasks(doAppliableCommitPosition());
                    return;
                }
            }
        }

        doCaptureHighest(writer);
        releaseExclusive();
    }

    /**
     * Caller must acquire exclusive latch, which is always released by this method.
     */
    @SuppressWarnings("unchecked")
    private void notifyCommitTasks(long commitPosition) {
        if (commitPosition <= mLastCommitListenerPos) {
            releaseExclusive();
            return;
        }

        Object commitTasks = null;

        while (true) {
            CommitCallback task = mCommitTasks.peek();
            if (task == null || commitPosition < task.mPosition) {
                break;
            }
            CommitCallback removed = mCommitTasks.remove();
            assert removed == task;
            if (commitTasks == null) {
                commitTasks = task;
            } else {
                List<Object> list;
                if (commitTasks instanceof List) {
                    list = (List<Object>) commitTasks;
                } else {
                    list = new ArrayList<>();
                    list.add(commitTasks);
                    commitTasks = list;
                }

                try {
                    list.add(task);
                } catch (Throwable e) {
                    // Rollback.
                    mCommitTasks.add(task);
                    for (var t : list) {
                        mCommitTasks.add((CommitCallback) t);
                    }
                    releaseExclusive();
                    throw e;
                }
            }
        }

        mLastCommitListenerPos = commitPosition;

        LongConsumer[] commitListeners = mCommitListeners;
        if (commitPosition >= mLogEndPosition) {
            mCommitListeners = null;
        }

        downgrade();

        // Note that the shared latch is held the whole time as callbacks are invoked. This
        // isn't ideal, but it's the most efficient. The callbacks should either complete
        // quickly or handoff work to another thread anyhow. Keeping the shared latch held has
        // other benefits, especially for the commit listeners. They can be certain that only
        // one thread will ever call it at a time, preventing race conditions.

        if (commitListeners != null) {
            notifyCommitListeners(commitListeners, commitPosition);
            if (mCommitListeners == null) {
                notifyCommitListeners(commitListeners, -1);
            }
        }

        if (commitTasks != null) {
            if (commitTasks instanceof List tasks) {
                for (var t : tasks) {
                    reached((CommitCallback) t, commitPosition);
                }
            } else {
                reached((CommitCallback) commitTasks, commitPosition);
            }
        }

        releaseShared();
    }

    /**
     * Calls listener.accept and catches all exceptions.
     */
    private static void notifyCommitListeners(LongConsumer[] commitListeners, long position) {
        for (LongConsumer listener : commitListeners) {
            try {
                listener.accept(position);
            } catch (Throwable e) {
                Utils.uncaught(e);
            }
        }
    }

    /**
     * Calls task.reached and catches all exceptions.
     */
    private static void reached(CommitCallback task, long position) {
        try {
            task.reached(position);
        } catch (Throwable e) {
            Utils.uncaught(e);
        }
    }

    void release(SegmentWriter writer, boolean recycle) {
        if (recycle) {
            if (writer.mWriterVersion != mLogVersion) {
                writer.close();
                return;
            }
            writer = mCaches.mWriters.add(writer);
        }

        if (writer != null) {
            Segment segment = writer.mWriterSegment;
            if (segment != null) {
                writer.mWriterSegment = null;
                unreferenced(segment);
            }
        }
    }

    void release(SegmentReader reader, boolean recycle) {
        if (recycle) {
            reader = mCaches.mReaders.add(reader);
        }

        if (reader != null) {
            Segment segment = reader.mReaderSegment;
            if (segment != null) {
                reader.mReaderSegment = null;
                unreferenced(segment);
            }
        }
    }

    void unreferenced(Segment segment) {
        if (((int) cRefCountHandle.getAndAdd(segment, -1)) > 0) {
            return;
        }

        // Unmap the segment and close the least recently used.

        Segment toClose = mCaches.mSegments.add(segment);

        var task = new Worker.Task() {
            @Override
            public void run() {
                try {
                    doUnreferenced(segment, toClose);
                } catch (IOException e) {
                    uncaught(e);
                }
            }
        };

        synchronized (mWorker) {
            mWorker.enqueue(task);
        }
    }

    void doUnreferenced(Segment segment, Segment toClose) throws IOException {
        segment.acquireExclusive();
        try {
            if (segment.mRefCount < 0) {
                segment.unmap();
            }
        } finally {
            segment.releaseExclusive();
        }

        if (toClose != null) {
            toClose.acquireExclusive();
            try {
                if (toClose.mRefCount < 0) {
                    toClose.close(false);
                } else {
                    // In use still, but at least unmap it.
                    toClose.unmap();
                }
            } finally {
                toClose.releaseExclusive();
            }
        }
    }

    void truncate(Segment segment) {
        var task = new Worker.Task() {
            @Override
            public void run() {
                cRefCountHandle.getAndAdd(segment, 1);

                try {
                    segment.truncate();
                } catch (IOException e) {
                    uncaught(e);
                }

                if (((int) cRefCountHandle.getAndAdd(segment, -1)) <= 0) {
                    try {
                        doUnreferenced(segment, mCaches.mSegments.add(segment));
                    } catch (IOException e) {
                        uncaught(e);
                    }
                }
            }
        };

        synchronized (mWorker) {
            mWorker.enqueue(task);
        }
    }

    void uncaught(IOException e) {
        var closed = (boolean) cLogClosedHandle.getVolatile(this);
        if (!closed) {
            Utils.uncaught(e);
        }
    }

    static final VarHandle cWriterPositionHandle;

    static {
        try {
            cWriterPositionHandle =
                MethodHandles.lookup().findVarHandle
                (SegmentWriter.class, "mWriterPosition", long.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    final class SegmentWriter extends LogWriter
        implements LKey<SegmentWriter>, LCache.Entry<SegmentWriter, FileTermLog>
    {
        long mWriterVersion;
        long mWriterPrevTerm;
        long mWriterStartPosition;
        long mWriterPosition;
        long mWriterHighestPosition;
        Segment mWriterSegment;

        private volatile boolean mWriterClosed;

        private SegmentWriter mCacheNext;
        private SegmentWriter mCacheMoreUsed;
        private SegmentWriter mCacheLessUsed;

        @Override
        public long key() {
            return mWriterStartPosition;
        }

        @Override
        public long prevTerm() {
            return mWriterPrevTerm;
        }

        @Override
        public long term() {
            return mLogTerm;
        }

        @Override
        public long termStartPosition() {
            return FileTermLog.this.startPosition();
        }

        @Override
        public long termEndPosition() {
            return FileTermLog.this.endPosition();
        }

        @Override
        public long position() {
            // Opaque access prevents word tearing, and it ensures visible progress.
            return (long) cWriterPositionHandle.getOpaque(this);
        }

        @Override
        public long commitPosition() {
            return FileTermLog.this.appliableCommitPosition();
        }

        @Override
        public void addCommitListener(LongConsumer listener) {
            FileTermLog.this.addCommitListener(listener);
        }

        @Override
        public int write(byte[] prefix,
                         byte[] data, int offset, int length,
                         long highestPosition)
            throws IOException
        {
            long position = mWriterPosition;
            Segment segment = mWriterSegment;

            if (segment == null) {
                segment = segmentForWriting(position);
                if (segment == null) {
                    return -1;
                }
                mWriterSegment = segment;
            }

            int result = 1;

            doWrite: {
                if (prefix != null) {
                    int prefixOffset = 0;
                    int prefixLength = prefix.length;
                    while (true) {
                        int amt = segment.write(position, prefix, prefixOffset, prefixLength);
                        position += amt;
                        prefixLength -= amt;
                        if (prefixLength <= 0) {
                            break;
                        }
                        prefixOffset += amt;
                        mWriterSegment = null;
                        unreferenced(segment);
                        segment = segmentForWriting(position);
                        if (segment == null) {
                            result = -1;
                            break doWrite;
                        }
                        mWriterSegment = segment;
                    }
                }

                while (true) {
                    int amt = segment.write(position, data, offset, length);
                    position += amt;
                    length -= amt;
                    if (length <= 0) {
                        break;
                    }
                    offset += amt;
                    mWriterSegment = null;
                    unreferenced(segment);
                    segment = segmentForWriting(position);
                    if (segment == null) {
                        result = -1;
                        break doWrite;
                    }
                    mWriterSegment = segment;
                }
            }

            writeFinished(this, position, highestPosition);

            return result;
        }

        private Segment segmentForWriting(long position) throws IOException {
            if (mWriterClosed) {
                throw new IOException("Closed");
            }
            return FileTermLog.this.segmentForWriting(this, position);
        }

        @Override
        public long waitForCommit(long position, long nanosTimeout) throws InterruptedIOException {
            return FileTermLog.this.waitForCommit(position, nanosTimeout, this);
        }

        @Override
        public void uponCommit(CommitCallback task) {
            FileTermLog.this.uponCommit(task);
        }

        @Override
        public void release() {
            FileTermLog.this.release(this, true);
        }

        @Override
        public void close() {
            mWriterClosed = true;
            FileTermLog.this.release(this, false);
            signalClosed(this);
        }

        @Override
        public long cacheKey() {
            return mWriterPosition;
        }

        @Override
        public boolean cacheCheck(FileTermLog check) {
            return check == FileTermLog.this;
        }

        @Override
        public SegmentWriter cacheNext() {
            return mCacheNext;
        }

        @Override
        public void cacheNext(SegmentWriter next) {
            mCacheNext = next;
        }

        @Override
        public SegmentWriter cacheMoreUsed() {
            return mCacheMoreUsed;
        }

        @Override
        public void cacheMoreUsed(SegmentWriter more) {
            mCacheMoreUsed = more;
        }

        @Override
        public SegmentWriter cacheLessUsed() {
            return mCacheLessUsed;
        }

        @Override
        public void cacheLessUsed(SegmentWriter less) {
            mCacheLessUsed = less;
        }

        @Override
        public String toString() {
            return "LogWriter: {prevTerm=" + mWriterPrevTerm + ", term=" + term() +
                ", startPosition=" + mWriterStartPosition + ", position=" + mWriterPosition +
                ", highestPosition=" + mWriterHighestPosition +
                ", segment=" + mWriterSegment + '}';
        }
    }

    static final VarHandle cReaderPositionHandle;

    static {
        try {
            cReaderPositionHandle =
                MethodHandles.lookup().findVarHandle
                (SegmentReader.class, "mReaderPosition", long.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    final class SegmentReader implements LogReader, LCache.Entry<SegmentReader, FileTermLog> {
        private long mReaderPrevTerm;
        long mReaderPosition;
        private long mReaderCommitPosition;
        private long mReaderContigPosition;
        Segment mReaderSegment;

        private volatile boolean mReaderClosed;

        private SegmentReader mCacheNext;
        private SegmentReader mCacheMoreUsed;
        private SegmentReader mCacheLessUsed;

        SegmentReader(long prevTerm, long position) {
            mReaderPrevTerm = prevTerm;
            cReaderPositionHandle.setOpaque(this, position);
        }

        @Override
        public long prevTerm() {
            return mReaderPrevTerm;
        }

        @Override
        public long term() {
            return mLogTerm;
        }

        @Override
        public long termStartPosition() {
            return FileTermLog.this.startPosition();
        }

        @Override
        public long termEndPosition() {
            return FileTermLog.this.endPosition();
        }

        @Override
        public long position() {
            // Opaque access prevents word tearing, and it ensures visible progress.
            return (long) cReaderPositionHandle.getOpaque(this);
        }

        @Override
        public long commitPosition() {
            return FileTermLog.this.appliableCommitPosition();
        }

        @Override
        public void addCommitListener(LongConsumer listener) {
            FileTermLog.this.addCommitListener(listener);
        }

        @Override
        public int read(byte[] buf, int offset, int length) throws IOException {
            long position = mReaderPosition;
            long commitPosition = mReaderCommitPosition;
            long avail = commitPosition - position;

            if (avail <= 0) {
                if (length == 0) {
                    return 0;
                }
                commitPosition = waitForCommit(position + 1, -1);
                if (commitPosition < 0) {
                    if (mReaderClosed) {
                        throw new IOException("Closed");
                    }
                    return EOF;
                }
                mReaderCommitPosition = commitPosition;
                avail = commitPosition - position;
            }

            return doRead(position, buf, offset, (int) Math.min(length, avail));
        }

        @Override
        public int tryRead(byte[] buf, int offset, int length) throws IOException {
            long position = mReaderPosition;
            long commitPosition = mReaderCommitPosition;
            long avail = commitPosition - position;

            if (avail <= 0) {
                FileTermLog.this.acquireShared();
                commitPosition = doAppliableCommitPosition();
                long endPosition = mLogEndPosition;
                FileTermLog.this.releaseShared();

                mReaderCommitPosition = commitPosition;
                avail = commitPosition - position;

                if (avail <= 0) {
                    return commitPosition == endPosition ? EOF : 0;
                }
            }

            return doRead(position, buf, offset, (int) Math.min(length, avail));
        }

        @Override
        public int tryReadAny(byte[] buf, int offset, int length) throws IOException {
            long position = mReaderPosition;
            long contigPosition = mReaderContigPosition;
            long avail = contigPosition - position;

            if (avail <= 0) {
                FileTermLog.this.acquireShared();
                contigPosition = mLogContigPosition;
                long endPosition = mLogEndPosition;
                FileTermLog.this.releaseShared();

                mReaderContigPosition = contigPosition;
                avail = contigPosition - position;

                if (avail <= 0) {
                    return contigPosition == endPosition ? EOF : 0;
                }
            }

            return doRead(position, buf, offset, (int) Math.min(length, avail));
        }

        private int doRead(long position, byte[] buf, int offset, int length) throws IOException {
            Segment segment = mReaderSegment;
            if (segment == null) {
                if (length == 0) {
                    // Return now to avoid mReaderPrevTerm side-effect assignment.
                    return 0;
                }
                segment = segmentForReading(position);
                if (segment == null) {
                    return EOF;
                }
                mReaderSegment = segment;
                mReaderPrevTerm = term();
            }

            int amt = segment.read(position, buf, offset, length);

            if (amt <= 0) {
                if (length == 0) {
                    return 0;
                }
                mReaderSegment = null;
                unreferenced(segment);
                segment = segmentForReading(position);
                if (segment == null) {
                    return EOF;
                }
                mReaderSegment = segment;
                amt = segment.read(position, buf, offset, length);
            }

            // Ensure that other threads can read the position safely.
            cReaderPositionHandle.setOpaque(this, position + amt);

            return amt;
        }

        private Segment segmentForReading(long position) throws IOException {
            if (mReaderClosed) {
                throw new IOException("Closed");
            }
            return FileTermLog.this.segmentForReading(position);
        }

        @Override
        public long waitForCommit(long position, long nanosTimeout) throws InterruptedIOException {
            return FileTermLog.this.waitForCommit(position, nanosTimeout, this);
        }

        @Override
        public void uponCommit(CommitCallback task) {
            FileTermLog.this.uponCommit(task);
        }

        @Override
        public void release() {
            FileTermLog.this.release(this, true);
        }

        @Override
        public void close() {
            mReaderClosed = true;
            FileTermLog.this.release(this, false);
            signalClosed(this);
        }

        @Override
        public long cacheKey() {
            return mReaderPosition;
        }

        @Override
        public boolean cacheCheck(FileTermLog check) {
            return check == FileTermLog.this;
        }

        @Override
        public SegmentReader cacheNext() {
            return mCacheNext;
        }

        @Override
        public void cacheNext(SegmentReader next) {
            mCacheNext = next;
        }

        @Override
        public SegmentReader cacheMoreUsed() {
            return mCacheMoreUsed;
        }

        @Override
        public void cacheMoreUsed(SegmentReader more) {
            mCacheMoreUsed = more;
        }

        @Override
        public SegmentReader cacheLessUsed() {
            return mCacheLessUsed;
        }

        @Override
        public void cacheLessUsed(SegmentReader less) {
            mCacheLessUsed = less;
        }

        @Override
        public String toString() {
            return "LogReader: {term=" + mLogTerm + ", position=" + mReaderPosition +
                ", segment=" + mReaderSegment + '}';
        }
    }

    static final VarHandle cRefCountHandle, cDirtyHandle;

    static {
        try {
            cRefCountHandle =
                MethodHandles.lookup().findVarHandle
                (Segment.class, "mRefCount", int.class);

            cDirtyHandle =
                MethodHandles.lookup().findVarHandle
                (Segment.class, "mDirty", int.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    final class Segment extends Latch implements LKey<Segment>, LCache.Entry<Segment, FileTermLog> {
        private static final int OPEN_HANDLE_COUNT = 8;

        final long mStartPosition;
        volatile long mMaxLength;
        // Zero-based reference count.
        volatile int mRefCount;
        private FileIO mFileIO;
        private boolean mSegmentClosed;

        volatile int mDirty;
        Segment mNextDirty;

        private Segment mCacheNext;
        private Segment mCacheMoreUsed;
        private Segment mCacheLessUsed;

        Segment(long startPosition, long maxLength) {
            mStartPosition = startPosition;
            mMaxLength = maxLength;
        }

        File file() {
            File base = mBase;

            if (base == null) {
                return null;
            }

            long prevTerm = prevTermAt(mStartPosition);
            long term = term();

            var b = new StringBuilder();
            b.append(base.getPath()).append('.');

            b.append(term).append('.').append(mStartPosition);

            if (prevTerm != term) {
                b.append('.').append(prevTerm);
            }

            return new File(b.toString());
        }

        @Override
        public long key() {
            return mStartPosition;
        }

        /**
         * Returns the exclusive end position.
         */
        long endPosition() {
            return mStartPosition + mMaxLength;
        }

        /**
         * @param position absolute position
         * @return actual amount written; is less when out of segment bounds
         */
        int write(long position, byte[] data, int offset, int length) throws IOException {
            position -= mStartPosition;
            if (position < 0) {
                return 0;
            }
            long amt = Math.min(mMaxLength - position, length);
            if (amt <= 0) {
                return 0;
            }
            length = (int) amt;

            FileIO io = mFileIO;

            while (true) {
                if (io != null || (io = fileIO()) != null) tryWrite: {
                    try {
                        io.write(position, data, offset, length);
                    } catch (IOException e) {
                        acquireExclusive();
                        if (mFileIO != io) {
                            break tryWrite;
                        }
                        releaseExclusive();
                        throw e;
                    }

                    if (mDirty == 0 && ((int) cDirtyHandle.getAndSet(this, 1)) == 0) {
                        addToDirtyList(this);
                    }

                    amt = Math.min(mMaxLength - position, length);

                    if (length > amt) {
                        // Wrote too much.
                        length = (int) Math.max(0, amt);
                        truncate();
                    }

                    return length;
                }

                try {
                    amt = Math.min(mMaxLength - position, length);
                    if (amt <= 0) {
                        return 0;
                    }
                    io = openForWriting();
                } finally {
                    releaseExclusive();
                }

                if (io == null) {
                    return 0;
                }

                io.map();
                length = (int) amt;
            }
        }

        /**
         * @param position absolute position
         * @return actual amount read; is less when end of segment is reached
         */
        int read(long position, byte[] buf, int offset, int length) throws IOException {
            position -= mStartPosition;
            if (position < 0) {
                throw new IllegalArgumentException();
            }
            long amt = Math.min(mMaxLength - position, length);
            if (amt <= 0) {
                return 0;
            }
            length = (int) amt;

            FileIO io = mFileIO;

            while (true) {
                if (io != null || (io = fileIO()) != null) {
                    try {
                        io.read(position, buf, offset, length);
                        return length;
                    } catch (IOException e) {
                        acquireExclusive();
                        if (mFileIO == io) {
                            releaseExclusive();
                            throw e;
                        }
                    }
                }

                try {
                    amt = Math.min(mMaxLength - position, length);
                    if (amt <= 0) {
                        return 0;
                    }
                    io = openForReading();
                } finally {
                    releaseExclusive();
                }

                length = (int) amt;
            }
        }

        /**
         * @return null if mFileIO is null, and exclusive latch is now held
         */
        private FileIO fileIO() {
            acquireShared();
            FileIO io = mFileIO;
            if (io != null) {
                releaseShared();
            } else if (!tryUpgrade()) {
                releaseShared();
                acquireExclusive();
                io = mFileIO;
                if (io != null) {
                    releaseExclusive();
                }
            }
            return io;
        }

        /**
         * Opens or re-opens the segment file if it was closed. Caller must hold exclusive latch.
         *
         * @return null if permanently closed
         */
        FileIO openForWriting() throws IOException {
            FileIO io = mFileIO;

            if (io == null) {
                if (mSegmentClosed) {
                    var closed = (boolean) cLogClosedHandle.getVolatile(FileTermLog.this);
                    if (closed) {
                        throw new IOException("Closed");
                    }
                    return null;
                }

                EnumSet<OpenOption> options = EnumSet.noneOf(OpenOption.class);
                int handles = 1;
                if (mMaxLength > 0) {
                    options.add(OpenOption.CREATE);
                    handles = OPEN_HANDLE_COUNT;
                }
                io = FileIO.open(file(), options, handles);

                try {
                    io.expandLength(mMaxLength, LengthOption.PREALLOCATE_OPTIONAL);
                } catch (IOException e) {
                    Utils.closeQuietly(io);
                    throw e;
                }

                mFileIO = io;
            }

            return io;
        }

        /**
         * Opens or re-opens the segment file if it was closed. Caller must hold exclusive latch.
         */
        FileIO openForReading() throws IOException {
            FileIO io = mFileIO;

            if (io == null) {
                if (mSegmentClosed) {
                    var closed = (boolean) cLogClosedHandle.getVolatile(FileTermLog.this);
                    if (closed) {
                        throw new IOException("Closed");
                    }
                    throw new InvalidReadException("Log compacted");
                }

                EnumSet<OpenOption> options = EnumSet.noneOf(OpenOption.class);
                int handles = 1;
                if (mMaxLength > 0) {
                    handles = OPEN_HANDLE_COUNT;
                }
                mFileIO = io = FileIO.open(file(), options, handles);
            }

            return io;
        }

        /**
         * Caller must hold exclusive latch.
         *
         * @return true if segment should be truncated or deleted
         */
        boolean setEndPosition(long endPosition) {
            long start = mStartPosition;
            if ((start + mMaxLength) <= endPosition) {
                return false;
            }
            mMaxLength = Math.max(0, endPosition - start);
            return true;
        }

        void sync() throws IOException {
            if (mDirty != 0 && ((int) cDirtyHandle.getAndSet(this, 0)) != 0) {
                cRefCountHandle.getAndAdd(this, 1);
                try {
                    doSync();
                } catch (IOException e) {
                    if (mDirty == 0 && ((int) cDirtyHandle.getAndSet(this, 1)) == 0) {
                        addToDirtyList(this);
                    }
                    throw e;
                } finally {
                    unreferenced(this);
                }
            }
        }

        private void doSync() throws IOException {
            FileIO io = mFileIO;

            while (true) {
                if (io != null || (io = fileIO()) != null) {
                    try {
                        io.sync(false);
                        return;
                    } catch (IOException e) {
                        acquireExclusive();
                        if (mFileIO == io) {
                            releaseExclusive();
                            throw e;
                        }
                    }
                }

                try {
                    if (mMaxLength == 0 || (io = openForWriting()) == null) {
                        return;
                    }
                } finally {
                    releaseExclusive();
                }
            }
        }

        /**
         * Truncates or deletes the file, according to the max length.
         */
        void truncate() throws IOException {
            acquireExclusive();

            while (true) {
                FileIO io;
                long maxLength;

                try {
                    maxLength = mMaxLength;
                    if (maxLength == 0) {
                        close(false);
                        io = null;
                    } else if ((io = openForWriting()) == null) {
                        return;
                    }
                } finally {
                    releaseExclusive();
                }

                if (io == null) {
                    Utils.delete(file());
                    return;
                }

                try {
                    io.truncateLength(maxLength);
                    return;
                } catch (IOException e) {
                    acquireExclusive();
                    if (mFileIO == io) {
                        releaseExclusive();
                        throw e;
                    }
                }
            }
        }

        /**
         * @throws IOException if permanently closed
         */
        void untruncate(long maxLength) throws IOException {
            acquireExclusive();
            try {
                if (mSegmentClosed) {
                    throw new IOException("Closed");
                }
                mMaxLength = maxLength;
            } finally {
                releaseExclusive();
            }
        }

        // Caller must hold exclusive latch.
        void unmap() throws IOException {
            if (mFileIO != null) {
                mFileIO.unmap();
            }
        }

        // Caller must hold exclusive latch.
        void close(boolean permanent) throws IOException {
            if (mFileIO != null) {
                mFileIO.close();
                mFileIO = null;
            }
            if (permanent) {
                mSegmentClosed = true;
            }
        }

        @Override
        public long cacheKey() {
            return key();
        }

        @Override
        public boolean cacheCheck(FileTermLog check) {
            return check == FileTermLog.this;
        }

        @Override
        public Segment cacheNext() {
            return mCacheNext;
        }

        @Override
        public void cacheNext(Segment next) {
            mCacheNext = next;
        }

        @Override
        public Segment cacheMoreUsed() {
            return mCacheMoreUsed;
        }

        @Override
        public void cacheMoreUsed(Segment more) {
            mCacheMoreUsed = more;
        }

        @Override
        public Segment cacheLessUsed() {
            return mCacheLessUsed;
        }

        @Override
        public void cacheLessUsed(Segment less) {
            mCacheLessUsed = less;
        }

        @Override
        public String toString() {
            var b = new StringBuilder().append("Segment: {");

            File file = file();
            if (file != null) {
                b.append("file=").append(file).append(", ");
            }

            return b.append("startPosition=").append(mStartPosition)
                .append(", maxLength=").append(mMaxLength)
                .append('}').toString();
        }
    }
}
