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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.PriorityQueue;
import java.util.TreeSet;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import java.util.concurrent.locks.LockSupport;

import org.cojen.tupl.io.FileIO;
import org.cojen.tupl.io.LengthOption;
import org.cojen.tupl.io.OpenOption;
import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Worker;

/**
 * Standard TermLog implementation which stores data in file segments.
 *
 * @author Brian S O'Neill
 */
final class FileTermLog extends Latch implements TermLog {
    private static final int MAX_CACHED_SEGMENTS = 10;
    private static final int MAX_CACHED_WRITERS = 10;
    private static final int MAX_CACHED_READERS = 10;

    private static final ThreadLocal<DelayedWaiter> cLocalDelayed = new ThreadLocal<>();

    private final Worker mWorker;
    private final File mBase;
    private final long mLogPrevTerm;
    private final long mLogTerm;

    /*
      In general, legal index values are bounded as follows:

        start <= commit <= highest <= contig <= end

      Commit index field can be larger than the highest index, but the actual reported value is
        min(commit, highest).
    */

    private final long mLogStartIndex;
    private long mLogCommitIndex;
    private long mLogHighestIndex;
    private long mLogContigIndex;
    private long mLogEndIndex;

    // Segments are keyed only by their start index.
    private final TreeSet<LKey<Segment>> mSegments;

    private final PriorityQueue<SegmentWriter> mNonContigWriters;

    private final LCache<Segment> mSegmentCache;
    private final LCache<SegmentWriter> mWriterCache;
    private final LCache<SegmentReader> mReaderCache;

    private final PriorityQueue<Delayed> mCommitTasks;

    private boolean mClosed;

    /**
     * @param base include the term in the name, for example: "mydata.repl.4"
     */
    FileTermLog(Worker worker, File base, long prevTerm, long term,
                long startIndex, long commitIndex, long highestIndex)
        throws IOException
    {
        mWorker = worker;
        mBase = base;
        mLogPrevTerm = prevTerm;
        mLogTerm = term;

        mSegments = new TreeSet<>();
        // TODO: alloc on demand; null out when finished and empty
        mNonContigWriters = new PriorityQueue<>();
        // TODO: alloc on demand; null out when finished and empty
        mCommitTasks = new PriorityQueue<>();

        mLogStartIndex = startIndex;
        mLogCommitIndex = commitIndex;
        mLogHighestIndex = highestIndex;
        mLogContigIndex = highestIndex;
        mLogEndIndex = Long.MAX_VALUE;

        mSegmentCache = new LCache<>(MAX_CACHED_SEGMENTS);
        mWriterCache = new LCache<>(MAX_CACHED_WRITERS);
        mReaderCache = new LCache<>(MAX_CACHED_READERS);

        // FIXME: open segments
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
    public long startIndex() {
        return mLogStartIndex;
    }

    @Override
    public long endIndex() {
        acquireShared();
        long index = mLogEndIndex;
        releaseShared();
        return index;
    }

    @Override
    public void captureHighest(LogInfo info) {
        info.mTerm = mLogTerm;
        acquireShared();
        doCaptureHighest(info);
        releaseShared();
    }

    // Caller must hold any latch.
    private void doCaptureHighest(LogInfo info) {
        info.mHighestIndex = mLogHighestIndex;
        info.mCommitIndex = actualCommitIndex();
    }

    // Caller must hold any latch.
    private long actualCommitIndex() {
        return Math.min(mLogCommitIndex, mLogHighestIndex);
    }

    @Override
    public void commit(long commitIndex) {
        acquireExclusive();
        if (commitIndex > mLogCommitIndex) {
            long endIndex = mLogEndIndex;
            if (commitIndex > endIndex) {
                commitIndex = endIndex;
            }
            mLogCommitIndex = commitIndex;
            notifyCommitTasks(actualCommitIndex());
            return;
        }
        releaseExclusive();
    }

    @Override
    public long waitForCommit(long index) throws IOException {
        boolean exclusive = false;
        acquireShared();
        while (true) {
            long commitIndex = actualCommitIndex();
            if (commitIndex >= index) {
                release(exclusive);
                return commitIndex;
            }
            if (index > mLogEndIndex) {
                release(exclusive);
                return -1;
            }
            if (exclusive || tryUpgrade()) {
                break;
            }
            releaseShared();
            acquireExclusive();
            exclusive = true;
        }

        DelayedWaiter waiter;
        try {
            waiter = cLocalDelayed.get();
            if (waiter == null) {
                waiter = new DelayedWaiter(index, Thread.currentThread());
                cLocalDelayed.set(waiter);
            } else {
                waiter.mCounter = index;
            }

            mCommitTasks.add(waiter);
        } finally {
            releaseExclusive();
        }

        while (true) {
            LockSupport.park(waiter);
            if (Thread.interrupted()) {
                throw new InterruptedIOException();
            }
            long commitIndex = waiter.mActualIndex;
            if (commitIndex < 0) {
                return commitIndex;
            }
            if (commitIndex >= index) {
                return commitIndex;
            }
        }
    }

    static class DelayedWaiter extends Delayed {
        private final Thread mWaiter;
        volatile long mActualIndex;

        DelayedWaiter(long index, Thread waiter) {
            super(index);
            mWaiter = waiter;
        }

        @Override
        protected void doRun(long index) {
            mActualIndex = index;
            LockSupport.unpark(mWaiter);
        }
    }

    @Override
    public void uponCommit(Delayed task) {
        if (task == null) {
            throw new NullPointerException();
        }

        acquireShared();

        long commitIndex = actualCommitIndex();
        if (commitIndex >= task.mCounter) {
            releaseShared();
            task.run(commitIndex);
            return;
        }

        if (!tryUpgrade()) {
            releaseShared();
            acquireExclusive();
            commitIndex = actualCommitIndex();
            if (commitIndex >= task.mCounter) {
                releaseExclusive();
                task.run(commitIndex);
                return;
            }
        }

        try {
            mCommitTasks.add(task);
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public void finishTerm(long endIndex) throws IOException {
        acquireExclusive();
        try {
            long commitIndex = actualCommitIndex();
            if (endIndex < commitIndex) {
                throw new IllegalArgumentException
                    ("Cannot finish term below commit index: " + endIndex + " < " + commitIndex);
            }

            if (endIndex == mLogEndIndex) {
                return;
            }

            if (endIndex > mLogEndIndex) {
                throw new IllegalStateException
                    ("Term is already finished: " + endIndex + " > " + mLogEndIndex);
            }

            for (LKey<Segment> key : mSegments) {
                Segment segment = (Segment) key;
                segment.acquireExclusive();
                boolean shouldTruncate = segment.setEndIndex(endIndex);
                segment.releaseExclusive();
                if (shouldTruncate) {
                    truncate(segment);
                }
            }

            mLogEndIndex = endIndex;

            if (endIndex < mLogContigIndex) {
                mLogContigIndex = endIndex;
            }

            if (endIndex < mLogHighestIndex) {
                mLogHighestIndex = endIndex;
            }

            if (!mNonContigWriters.isEmpty()) {
                Iterator<SegmentWriter> it = mNonContigWriters.iterator();
                while (it.hasNext()) {
                    SegmentWriter writer = it.next();
                    if (writer.mWriterStartIndex >= endIndex) {
                        it.remove();
                    }
                }
            }

            mCommitTasks.removeIf(task -> {
                if (task.mCounter > endIndex) {
                    task.run(-1);
                    return true;
                }
                return false;
            });
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public long checkForMissingData(long contigIndex, IndexRange results) {
        acquireShared();
        try {
            if (contigIndex < mLogStartIndex || mLogContigIndex == contigIndex) {
                long expectedIndex = mLogEndIndex;
                if (expectedIndex == Long.MAX_VALUE) {
                    expectedIndex = mLogCommitIndex;
                }

                long missingStartIndex = mLogContigIndex;

                if (!mNonContigWriters.isEmpty()) {
                    SegmentWriter[] writers = mNonContigWriters.toArray
                        (new SegmentWriter[mNonContigWriters.size()]);
                    Arrays.sort(writers);

                    for (SegmentWriter writer : writers) {
                        long missingEndIndex = writer.mWriterStartIndex;
                        if (missingStartIndex != missingEndIndex) {
                            results.range(missingStartIndex, missingEndIndex);
                        }
                        missingStartIndex = writer.mWriterIndex;
                    }
                }

                if (expectedIndex > missingStartIndex) {
                    results.range(missingStartIndex, expectedIndex);
                }
            }

            return mLogContigIndex;
        } finally {
            releaseShared();
        }
    }

    @Override
    public LogWriter openWriter(long startIndex) throws IOException {
        SegmentWriter writer = mWriterCache.remove(startIndex);

        if (writer == null) {
            writer = new SegmentWriter();
            writer.mWriterPrevTerm = startIndex == mLogStartIndex ? mLogPrevTerm : mLogTerm;
            writer.mWriterStartIndex = startIndex;
            writer.mWriterIndex = startIndex;

            acquireExclusive();
            try {
                if (startIndex > mLogContigIndex && startIndex < mLogEndIndex) {
                    mNonContigWriters.add(writer);
                }
            } finally {
                releaseExclusive();
            }
        }

        return writer;
    }

    @Override
    public LogReader openReader(long startIndex) throws IOException {
        SegmentReader reader = mReaderCache.remove(startIndex);

        if (reader == null) {
            long prevTerm = startIndex == mLogStartIndex ? mLogPrevTerm : mLogTerm;
            reader = new SegmentReader(prevTerm, startIndex);
        }

        return reader;
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
            mClosed = true;

            for (LKey<Segment> key : mSegments) {
                Segment segment = (Segment) key;
                segment.acquireExclusive();
                try {
                    segment.close(true);
                } finally {
                    segment.releaseExclusive();
                }
            }

            mWorker.join(false);
        } finally {
            releaseExclusive();
        }
    }

    @Override
    public String toString() {
        return "TermLog: {prevTerm=" + mLogPrevTerm + ", term=" + mLogTerm +
            ", startIndex=" + mLogStartIndex + ", commitIndex=" + mLogCommitIndex +
            ", highestIndex=" + mLogHighestIndex + ", contigIndex=" + mLogContigIndex +
            ", endIndex=" + mLogEndIndex + '}';
    }

    /**
     * @return null if index is at or higher than end index
     */
    Segment segmentForWriting(long index) throws IOException {
        indexCheck(index);

        LKey<Segment> key = new LKey.Finder<>(index);

        acquireExclusive();
        find: try {
            if (index >= mLogEndIndex) {
                releaseExclusive();
                return null;
            }

            Segment startSegment = (Segment) mSegments.floor(key); // findLe

            if (startSegment != null && index < startSegment.endIndex()) {
                mSegmentCache.remove(startSegment.cacheKey());
                Segment.cRefCountUpdater.getAndIncrement(startSegment);
                return startSegment;
            }

            if (mClosed) {
                throw new IOException("closed");
            }

            // 1, 2, 4, 8, 16, 32 or 64 MiB
            long maxLength = (1024 * 1024L) << Math.min(6, mSegments.size());
            long startIndex = index;
            if (startSegment != null) {
                startIndex = startSegment.endIndex()
                    + ((index - startSegment.endIndex()) / maxLength) * maxLength;
            }

            // Don't allow segment to encroach on next segment or go beyond term end.
            Segment nextSegment = (Segment) mSegments.higher(key); // findGt
            long endIndex = nextSegment == null ? mLogEndIndex : nextSegment.mStartIndex;
            maxLength = Math.min(maxLength, endIndex - startIndex);

            File file = new File(mBase.getPath() + '.' + startIndex);
            Segment segment = new Segment(file, startIndex, maxLength);
            mSegments.add(segment);
            return segment;
        } finally {
            releaseExclusive();
        }
    }

    /**
     * @return null if segment doesn't exist
     */
    Segment segmentForReading(long index) throws IOException {
        indexCheck(index);

        LKey<Segment> key = new LKey.Finder<>(index);

        acquireExclusive();
        try {
            Segment segment = (Segment) mSegments.floor(key); // findLe
            if (segment != null && index < segment.endIndex()) {
                Segment.cRefCountUpdater.getAndIncrement(segment);
                return segment;
            }
        } finally {
            releaseExclusive();
        }

        return null;
    }

    private void indexCheck(long index) throws IllegalArgumentException {
        if (index < mLogStartIndex) {
            throw new IllegalArgumentException
                ("Index is too low: " + index + " < " + mLogStartIndex);
        }
    }

    /**
     * Called by SegmentWriter.
     */
    void writeFinished(SegmentWriter writer, long currentIndex, long highestIndex) {
        acquireExclusive();

        long commitIndex = mLogCommitIndex;
        if (highestIndex < commitIndex) {
            long allowedHighestIndex = Math.min(commitIndex, mLogContigIndex);
            if (highestIndex < allowedHighestIndex) {
                highestIndex = allowedHighestIndex;
            }
        }

        long endIndex = mLogEndIndex;
        if (currentIndex > endIndex) {
            currentIndex = endIndex;
        }
        if (highestIndex > endIndex) {
            highestIndex = endIndex;
        }

        writer.mWriterIndex = currentIndex;

        if (currentIndex > writer.mWriterStartIndex) {
            writer.mWriterPrevTerm = mLogTerm;
        }

        if (highestIndex > writer.mWriterHighestIndex) {
            writer.mWriterHighestIndex = highestIndex;
        }

        long contigIndex = mLogContigIndex;
        if (writer.mWriterStartIndex <= contigIndex) {
            // Writer is in the contiguous region -- check if it's growing now.
            if (currentIndex > contigIndex) {
                contigIndex = currentIndex;

                // Remove non-contiguous writers that are now in the contiguous region.
                while (true) {
                    SegmentWriter next = mNonContigWriters.peek();
                    if (next == null || next.mWriterStartIndex > contigIndex) {
                        break;
                    }

                    SegmentWriter removed = mNonContigWriters.remove();
                    assert removed == next;

                    if (next.mWriterIndex > contigIndex) {
                        contigIndex = next.mWriterIndex;
                    }

                    // Advance the highest index, if possible.
                    long nextHighest = next.mWriterHighestIndex;
                    if (nextHighest > highestIndex && highestIndex <= contigIndex) {
                        highestIndex = nextHighest;
                    }
                }

                mLogContigIndex = contigIndex;
            }

            applyHighest: {
                if (endIndex < Long.MAX_VALUE) {
                    // Term has ended, which is always at a valid highest index. The contiguous
                    // index can be used as the highest, allowing the commit index to advance.
                    highestIndex = contigIndex;
                } else if (highestIndex <= mLogHighestIndex || highestIndex > contigIndex) {
                    // Cannot apply just yet.
                    break applyHighest;
                }

                mLogHighestIndex = highestIndex;
                doCaptureHighest(writer);
                notifyCommitTasks(actualCommitIndex());
                return;
            }
        }

        doCaptureHighest(writer);
        releaseExclusive();
    }

    /**
     * Caller must acquire exclusive latch, which is always released by this method.
     */
    private void notifyCommitTasks(long commitIndex) {
        PriorityQueue<Delayed> tasks = mCommitTasks;

        while (true) {
            Delayed task = tasks.peek();
            if (task == null || commitIndex < task.mCounter) {
                releaseExclusive();
                return;
            }
            Delayed removed = tasks.remove();
            assert removed == task;
            boolean empty = tasks.isEmpty();
            releaseExclusive();
            task.run(commitIndex);
            if (empty) {
                return;
            }
            acquireExclusive();
            commitIndex = actualCommitIndex();
        }
    }

    void release(SegmentWriter writer) {
        writer = mWriterCache.add(writer);

        if (writer != null) {
            Segment segment = writer.mWriterSegment;
            if (segment != null) {
                writer.mWriterSegment = null;
                unreferenced(segment);
            }
        }
    }

    void release(SegmentReader reader) {
        reader = mReaderCache.add(reader);

        if (reader != null) {
            Segment segment = reader.mReaderSegment;
            if (segment != null) {
                reader.mReaderSegment = null;
                unreferenced(segment);
            }
        }
    }

    void unreferenced(Segment segment) {
        if (Segment.cRefCountUpdater.getAndDecrement(segment) > 0) {
            return;
        }

        // Unmap the segement and close the least recently used.

        Segment toClose = mSegmentCache.add(segment);

        Worker.Task task = new Worker.Task() {
            @Override
            public void run() {
                try {
                    doUnreferenced(segment, toClose);
                } catch (IOException e) {
                    Utils.uncaught(e);
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
                if (segment.mRefCount < 0) {
                    segment.close(false);
                } else {
                    // In use still, but at least unmap it.
                    segment.unmap();
                }
            } finally {
                toClose.releaseExclusive();
            }
        }
    }

    void truncate(Segment segment) {
        Worker.Task task = new Worker.Task() {
            @Override
            public void run() {
                Segment.cRefCountUpdater.getAndIncrement(segment);

                try {
                    segment.truncate();
                } catch (IOException e) {
                    Utils.uncaught(e);
                }

                if (Segment.cRefCountUpdater.getAndDecrement(segment) <= 0) {
                    try {
                        doUnreferenced(segment, mSegmentCache.add(segment));
                    } catch (IOException e) {
                        Utils.uncaught(e);
                    }
                }
            }
        };

        synchronized (mWorker) {
            mWorker.enqueue(task);
        }
    }

    final class SegmentWriter extends LogWriter
        implements LKey<SegmentWriter>, LCache.Entry<SegmentWriter>
    {
        long mWriterPrevTerm;
        long mWriterStartIndex;
        long mWriterIndex;
        long mWriterHighestIndex;
        Segment mWriterSegment;

        private SegmentWriter mCacheNext;
        private SegmentWriter mCacheMoreUsed;
        private SegmentWriter mCacheLessUsed;

        @Override
        public long key() {
            return mWriterStartIndex;
        }

        @Override
        long prevTerm() {
            return mWriterPrevTerm;
        }

        @Override
        long term() {
            return mLogTerm;
        }

        @Override
        long index() {
            return mWriterIndex;
        }

        @Override
        int write(byte[] data, int offset, int length, long highestIndex) throws IOException {
            long index = mWriterIndex;
            Segment segment = mWriterSegment;

            if (segment == null) {
                segment = segmentForWriting(index);
                if (segment == null) {
                    return 0;
                }
                mWriterSegment = segment;
            }

            int total = 0;

            while (true) {
                int amt = segment.write(index, data, offset, length);
                index += amt;
                total += amt;
                length -= amt;
                if (length <= 0) {
                    break;
                }
                offset += amt;
                mWriterSegment = null;
                unreferenced(segment);
                segment = segmentForWriting(index);
                if (segment == null) {
                    break;
                }
                mWriterSegment = segment;
            }

            writeFinished(this, index, highestIndex);

            return total;
        }

        @Override
        void release() {
            FileTermLog.this.release(this);
        }

        @Override
        public long cacheKey() {
            return mWriterIndex;
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
                ", startIndex=" + mWriterStartIndex + ", index=" + mWriterIndex +
                ", highestIndex=" + mWriterHighestIndex +
                ", segment=" + mWriterSegment + '}';
        }
    }

    final class SegmentReader implements LogReader, LCache.Entry<SegmentReader> {
        private long mReaderPrevTerm;
        private long mReaderIndex;
        private long mReaderCommitIndex;
        private long mReaderContigIndex;
        Segment mReaderSegment;

        private SegmentReader mCacheNext;
        private SegmentReader mCacheMoreUsed;
        private SegmentReader mCacheLessUsed;

        SegmentReader(long prevTerm, long index) {
            mReaderPrevTerm = prevTerm;
            mReaderIndex = index;
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
        public long index() {
            return mReaderIndex;
        }

        @Override
        public int read(byte[] buf, int offset, int length) throws IOException {
            long index = mReaderIndex;
            long commitIndex = mReaderCommitIndex;
            long avail = commitIndex - index;

            if (avail <= 0) {
                commitIndex = waitForCommit(index + 1);
                if (commitIndex == -1) {
                    return -1;
                }
                mReaderCommitIndex = commitIndex;
                avail = commitIndex - index;
            }

            return doRead(index, buf, offset, (int) Math.min(length, avail));
        }

        @Override
        public int readAny(byte[] buf, int offset, int length) throws IOException {
            long index = mReaderIndex;
            long contigIndex = mReaderContigIndex;
            long avail = contigIndex - index;

            if (avail <= 0) {
                FileTermLog.this.acquireShared();
                contigIndex = mLogContigIndex;
                long endIndex = mLogEndIndex;
                FileTermLog.this.releaseShared();

                mReaderContigIndex = contigIndex;
                avail = contigIndex - index;

                if (avail <= 0) {
                    return contigIndex == endIndex ? -1 : 0;
                }
            }

            return doRead(index, buf, offset, (int) Math.min(length, avail));
        }

        private int doRead(long index, byte[] buf, int offset, int length) throws IOException {
            Segment segment = mReaderSegment;
            if (segment == null) {
                if (length == 0) {
                    // Return now to avoid mReaderPrevTerm side-effect assignment.
                    return 0;
                }
                segment = segmentForReading(index);
                if (segment == null) {
                    return -1;
                }
                mReaderSegment = segment;
                mReaderPrevTerm = term();
            }

            int amt = segment.read(index, buf, offset, length);

            if (amt <= 0) {
                if (length == 0) {
                    return 0;
                }
                mReaderSegment = null;
                unreferenced(segment);
                segment = segmentForReading(index);
                if (segment == null) {
                    return -1;
                }
                mReaderSegment = segment;
                amt = segment.read(index, buf, offset, length);
            }

            mReaderIndex = index + amt;
            return amt;
        }

        @Override
        public void release() {
            FileTermLog.this.release(this);
        }

        @Override
        public long cacheKey() {
            return mReaderIndex;
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
            return "LogReader: {term=" + mLogTerm + ", index=" + mReaderIndex +
                ", segment=" + mReaderSegment + '}';
        }
    }

    static final class Segment extends Latch implements LKey<Segment>, LCache.Entry<Segment> {
        private static final int OPEN_HANDLE_COUNT = 8;

        static final AtomicIntegerFieldUpdater<Segment> cRefCountUpdater =
            AtomicIntegerFieldUpdater.newUpdater(Segment.class, "mRefCount");

        private final File mFile;
        final long mStartIndex;
        volatile long mMaxLength;
        // Zero-based reference count.
        volatile int mRefCount;
        private FileIO mFileIO;
        private boolean mClosed;

        private Segment mCacheNext;
        private Segment mCacheMoreUsed;
        private Segment mCacheLessUsed;

        Segment(File file, long startIndex, long maxLength) {
            mFile = file;
            mStartIndex = startIndex;
            mMaxLength = maxLength;
        }

        @Override
        public long key() {
            return mStartIndex;
        }

        /**
         * Returns the exclusive end index.
         */
        long endIndex() {
            return mStartIndex + mMaxLength;
        }

        /**
         * @param index absolute index
         * @return actual amount written; is less when out of segment bounds
         */
        int write(long index, byte[] data, int offset, int length) throws IOException {
            index -= mStartIndex;
            if (index < 0) {
                return 0;
            }
            long amt = Math.min(mMaxLength - index, length);
            if (amt <= 0) {
                return 0;
            }
            length = (int) amt;

            FileIO io = mFileIO;

            while (true) {
                if (io != null || (io = fileIO()) != null) tryWrite: {
                    try {
                        io.write(index, data, offset, length);
                    } catch (IOException e) {
                        acquireExclusive();
                        if (mFileIO != io) {
                            break tryWrite;
                        }
                        releaseExclusive();
                        throw e;
                    }

                    amt = Math.min(mMaxLength - index, length);

                    if (length > amt) {
                        // Wrote too much.
                        length = (int) Math.max(0, amt);
                        truncate();
                    }

                    return length;
                }

                try {
                    amt = Math.min(mMaxLength - index, length);
                    if (amt <= 0) {
                        return 0;
                    }
                    openForWriting();
                    io = mFileIO;
                } finally {
                    releaseExclusive();
                }

                length = (int) amt;
            }
        }

        /**
         * @param index absolute index
         * @return actual amount read; is less when end of segment is reached
         */
        int read(long index, byte[] buf, int offset, int length) throws IOException {
            index -= mStartIndex;
            if (index < 0) {
                throw new IllegalArgumentException();
            }
            long amt = Math.min(mMaxLength - index, length);
            if (amt <= 0) {
                return 0;
            }
            length = (int) amt;

            FileIO io = mFileIO;

            while (true) {
                if (io != null || (io = fileIO()) != null) tryRead: {
                    try {
                        io.read(index, buf, offset, length);
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
                    amt = Math.min(mMaxLength - index, length);
                    if (amt <= 0) {
                        return 0;
                    }
                    openForReading();
                    io = mFileIO;
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
         */
        void openForWriting() throws IOException {
            if (mFileIO == null) {
                if (mMaxLength <= 0) {
                    return;
                }
                checkClosed();
                EnumSet<OpenOption> options = EnumSet.of
                    (OpenOption.CREATE, OpenOption.CLOSE_DONTNEED);
                FileIO io = FileIO.open(mFile, options, OPEN_HANDLE_COUNT);
                try {
                    io.setLength(mMaxLength, LengthOption.PREALLOCATE_OPTIONAL);
                } catch (IOException e) {
                    Utils.closeQuietly(null, io);
                    throw e;
                }
                mFileIO = io;
            }

            mFileIO.map();
        }

        /**
         * Opens or re-opens the segment file if it was closed. Caller must hold exclusive latch.
         */
        void openForReading() throws IOException {
            if (mFileIO == null) {
                checkClosed();
                EnumSet<OpenOption> options = EnumSet.of(OpenOption.CLOSE_DONTNEED);
                mFileIO = FileIO.open(mFile, options, OPEN_HANDLE_COUNT);
            }
        }

        private void checkClosed() throws IOException {
            if (mClosed) {
                throw new IOException("closed");
            }
        }

        /**
         * Caller must hold exclusive latch.
         *
         * @return true if segment should be truncated or deleted
         */
        boolean setEndIndex(long endIndex) {
            long start = mStartIndex;
            if ((start + mMaxLength) <= endIndex) {
                return false;
            }
            mMaxLength = Math.max(0, endIndex - start);
            return true;
        }

        /**
         * Truncates or deletes the file, according to the max length.
         */
        void truncate() throws IOException {
            FileIO io;
            long maxLength;

            acquireExclusive();
            try {
                maxLength = mMaxLength;
                if (maxLength == 0) {
                    close(true);
                    io = null;
                } else {
                    openForWriting();
                    io = mFileIO;
                }
            } finally {
                releaseExclusive();
            }

            if (io == null) {
                mFile.delete();
            } else {
                io.setLength(maxLength);
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
                if (permanent) {
                    mClosed = true;
                }
            }
        }

        @Override
        public long cacheKey() {
            return key();
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
            return "Segment: {file=" + mFile + ", startIndex=" + mStartIndex +
                ", maxLength=" + mMaxLength + '}';
        }
    }
}
