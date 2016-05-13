/*
 *  Copyright 2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl.io;

import java.io.InterruptedIOException;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.util.EnumSet;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import org.cojen.tupl.util.Latch;

import static org.cojen.tupl.io.Utils.rethrow;

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class AbstractFileIO extends FileIO {
    private static final int MAPPING_SHIFT = 30;
    private static final int MAPPING_SIZE = 1 << MAPPING_SHIFT;

    // If sync is taking longer than 10 seconds, start slowing down access.
    private static final long SYNC_YIELD_THRESHOLD_NANOS = 10L * 1000 * 1000 * 1000;

    private static final AtomicIntegerFieldUpdater<AbstractFileIO> cSyncCountUpdater =
        AtomicIntegerFieldUpdater.newUpdater(AbstractFileIO.class, "mSyncCount");

    private final boolean mReadOnly;

    private final Latch mRemapLatch;
    private final Latch mMappingLatch;
    private Mapping[] mMappings;
    private int mLastMappingSize;

    private final Latch mSyncLatch;
    private volatile int mSyncCount;
    private volatile long mSyncStartNanos;

    protected volatile Throwable mCause;

    AbstractFileIO(EnumSet<OpenOption> options) {
        mReadOnly = options.contains(OpenOption.READ_ONLY);
        mRemapLatch = new Latch();
        mMappingLatch = new Latch();
        mSyncLatch = new Latch();
    }

    @Override
    public final boolean isReadOnly() {
        return mReadOnly;
    }

    @Override
    public final long length() throws IOException {
        mMappingLatch.acquireShared();
        try {
            return doLength();
        } catch (IOException e) {
            throw rethrow(e, mCause);
        } finally {
            mMappingLatch.releaseShared();
        }
    }

    @Override
    public final void setLength(long length) throws IOException {
        mRemapLatch.acquireExclusive();
        try {
            // Length reduction screws up the mapping on Linux, causing a hard
            // process crash when accessing anything beyond the file length.
            boolean remap = mMappings != null && length < length();

            // Windows will ignore the length reduction entirely to prevent the crash. Need to
            // explicitly unmap first.
            if (remap) {
                doUnmap(true);
            }

            try {
                doSetLength(length);
            } catch (IOException e) {
                // Ignore.
            } finally {
                if (remap) {
                    doMap(true);
                }
            }
        } finally {
            mRemapLatch.releaseExclusive();
        }
    }

    @Override
    public final void read(long pos, byte[] buf, int offset, int length) throws IOException {
        access(true, pos, buf, offset, length);
    }

    @Override
    public final void read(long pos, ByteBuffer bb) throws IOException {
        access(true, pos, bb);
    }

    @Override
    public final void read(long pos, long ptr, int offset, int length) throws IOException {
        access(true, pos, ptr + offset, length);
    }

    @Override
    public final void write(long pos, byte[] buf, int offset, int length) throws IOException {
        access(false, pos, buf, offset, length);
    }

    @Override
    public final void write(long pos, ByteBuffer bb) throws IOException {
        access(false, pos, bb);
    }

    @Override
    public final void write(long pos, long ptr, int offset, int length) throws IOException {
        access(false, pos, ptr + offset, length);
    }

    private void access(boolean read, long pos, byte[] buf, int offset, int length)
        throws IOException
    {
        syncWait();

        try {
            mMappingLatch.acquireShared();
            try {
                Mapping[] mappings = mMappings;
                if (mappings != null) {
                    while (true) {
                        int mi = (int) (pos >> MAPPING_SHIFT);
                        int mlen = mappings.length;
                        if (mi >= mlen) {
                            break;
                        }

                        Mapping mapping = mappings[mi];
                        int mpos = (int) (pos & (MAPPING_SIZE - 1));
                        int mavail;

                        if (mi == (mlen - 1)) {
                            mavail = mLastMappingSize - mpos;
                            if (mavail <= 0) {
                                break;
                            }
                        } else {
                            mavail = MAPPING_SIZE - mpos;
                        }

                        if (mavail > length) {
                            mavail = length;
                        }

                        if (read) {
                            mapping.read(mpos, buf, offset, mavail);
                        } else {
                            mapping.write(mpos, buf, offset, mavail);
                        }

                        length -= mavail;
                        if (length <= 0) {
                            return;
                        }

                        pos += mavail;
                        offset += mavail;
                    }
                }
            } finally {
                mMappingLatch.releaseShared();
            }

            if (read) {
                doRead(pos, buf, offset, length);
            } else {
                doWrite(pos, buf, offset, length);
            }
        } catch (IOException e) {
            throw rethrow(e, mCause);
        }
    }

    private void access(boolean read, long pos, ByteBuffer bb) throws IOException {
        if (bb.remaining() <= 0) {
            return;
        }

        syncWait();

        try {
            mMappingLatch.acquireShared();
            try {
                Mapping[] mappings = mMappings;
                if (mappings != null) {
                    while (true) {
                        int mi = (int) (pos >> MAPPING_SHIFT);
                        int mlen = mappings.length;
                        if (mi >= mlen) {
                            break;
                        }

                        Mapping mapping = mappings[mi];
                        int mpos = (int) (pos & (MAPPING_SIZE - 1));
                        int mavail;

                        if (mi == (mlen - 1)) {
                            mavail = mLastMappingSize - mpos;
                            if (mavail <= 0) {
                                break;
                            }
                        } else {
                            mavail = MAPPING_SIZE - mpos;
                        }

                        if (mavail >= bb.remaining()) {
                            if (read) {
                                mapping.read(mpos, bb);
                            } else {
                                mapping.write(mpos, bb);
                            }
                            return;
                        }

                        int limit = bb.limit();
                        bb.limit(bb.position() + mavail);
                        try {
                            if (read) {
                                mapping.read(mpos, bb);
                            } else {
                                mapping.write(mpos, bb);
                            }
                        } finally {
                            bb.limit(limit);
                        }

                        pos += mavail;
                    }
                }
            } finally {
                mMappingLatch.releaseShared();
            }

            if (read) {
                doRead(pos, bb);
            } else {
                doWrite(pos, bb);
            }
        } catch (IOException e) {
            throw rethrow(e, mCause);
        }
    }

    private void access(boolean read, long pos, long ptr, int length) throws IOException {
        if (length > 0) {
            access(read, pos, DirectAccess.ref(ptr, length));
        }
    }

    @Override
    public final void sync(boolean metadata) throws IOException {
        if (mReadOnly) {
            return;
        }

        int count = cSyncCountUpdater.getAndIncrement(this);
        try {
            if (count == 0) {
                mSyncStartNanos = System.nanoTime();
            }

            mSyncLatch.acquireShared();
            try {
                mMappingLatch.acquireShared();
                try {
                    Mapping[] mappings = mMappings;
                    if (mappings != null) {
                        for (Mapping m : mappings) {
                            // Save metadata sync for last.
                            m.sync(false);
                        }
                    }
                } finally {
                    mMappingLatch.releaseShared();
                }

                doSync(metadata);
            } catch (IOException e) {
                throw rethrow(e, mCause);
            } finally {
                mSyncLatch.releaseShared();
            }
        } finally {
            cSyncCountUpdater.decrementAndGet(this);
        }
    }

    @Override
    public final void map() throws IOException {
        mRemapLatch.acquireExclusive();
        try {
            doMap(false);
        } finally {
            mRemapLatch.releaseExclusive();
        }
    }

    @Override
    public final void remap() throws IOException {
        mRemapLatch.acquireExclusive();
        try {
            doMap(true);
        } finally {
            mRemapLatch.releaseExclusive();
        }
    }

    @Override
    public final void unmap() throws IOException {
        unmap(true);
    }

    protected void unmap(boolean reopen) throws IOException {
        mRemapLatch.acquireExclusive();
        try {
            doUnmap(reopen);
        } finally {
            mRemapLatch.releaseExclusive();
        }
    }

    // Caller must hold mRemapLatch exclusively.
    private void doUnmap(boolean reopen) throws IOException {
        mMappingLatch.acquireExclusive();
        try {
            Mapping[] mappings = mMappings;
            if (mappings == null) {
                return;
            }

            mMappings = null;
            mLastMappingSize = 0;

            IOException ex = null;
            for (Mapping m : mappings) {
                ex = Utils.closeQuietly(ex, m);
            }

            // Need to replace all the open files. There's otherwise no guarantee that any
            // changes to the mapped files will be visible.

            if (reopen) {
                try {
                    reopen();
                } catch (IOException e) {
                    if (ex == null) {
                        ex = e;
                    }
                }
            }

            if (ex != null) {
                throw ex;
            }
        } finally {
            mMappingLatch.releaseExclusive();
        }
    }

    // Caller must hold mRemapLatch exclusively.
    private void doMap(boolean remap) throws IOException {
        Mapping[] oldMappings;
        int oldMappingDiscardPos;
        Mapping[] newMappings;
        int newLastSize;

        mMappingLatch.acquireShared();
        try {
            oldMappings = mMappings;
            if (oldMappings == null && remap) {
                // Don't map unless already mapped.
                return;
            }

            long length = length();

            if (oldMappings != null) {
                long oldMappedLength = oldMappings.length == 0 ? 0 :
                    (oldMappings.length - 1) * (long) MAPPING_SIZE + mLastMappingSize;
                if (length == oldMappedLength) {
                    return;
                }
            }

            long count = (length + (MAPPING_SIZE - 1)) / MAPPING_SIZE;

            if (count > Integer.MAX_VALUE) {
                throw new IOException("Mapping is too large");
            }

            try {
                newMappings = new Mapping[(int) count];
            } catch (OutOfMemoryError e) {
                throw new IOException("Mapping is too large");
            }

            oldMappings = mMappings;
            oldMappingDiscardPos = 0;

            int i = 0;
            long pos = 0;

            if (oldMappings != null && oldMappings.length > 0) {
                i = oldMappings.length;
                if (mLastMappingSize != MAPPING_SIZE) {
                    i--;
                    oldMappingDiscardPos = i;
                }
                System.arraycopy(oldMappings, 0, newMappings, 0, i);
                pos = i * (long) MAPPING_SIZE;
            }

            while (i < count - 1) {
                newMappings[i++] = openMapping(mReadOnly, pos, MAPPING_SIZE);
                pos += MAPPING_SIZE;
            }

            if (count == 0) {
                newLastSize = 0;
            } else {
                newLastSize = (int) (MAPPING_SIZE - (count * MAPPING_SIZE - length));
                newMappings[i] = openMapping(mReadOnly, pos, newLastSize);
            }
        } finally {
            mMappingLatch.releaseShared();
        }

        mMappingLatch.acquireExclusive();
        mMappings = newMappings;
        mLastMappingSize = newLastSize;
        mMappingLatch.releaseExclusive();

        if (oldMappings != null) {
            IOException ex = null;
            while (oldMappingDiscardPos < oldMappings.length) {
                ex = Utils.closeQuietly(ex, oldMappings[oldMappingDiscardPos++]);
            }
            if (ex != null) {
                throw ex;
            }
        }
    }
 
    protected void syncWait() throws InterruptedIOException {
        if (mSyncCount != 0) {
            long syncTimeNanos = System.nanoTime() - mSyncStartNanos;
            if (syncTimeNanos > SYNC_YIELD_THRESHOLD_NANOS) {
                // Yield 1ms for each second that sync has been running. Use a latch instead
                // of a sleep, preventing prolonged sleep after sync finishes.
                long sleepNanos = syncTimeNanos / 1000L;
                try {
                    if (mSyncLatch.tryAcquireExclusiveNanos(sleepNanos)) {
                        mSyncLatch.releaseExclusive();
                    }
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }
        }
    }

    protected abstract long doLength() throws IOException;

    protected abstract void doSetLength(long length) throws IOException;

    protected abstract void doRead(long pos, byte[] buf, int offset, int length)
        throws IOException;

    protected abstract void doRead(long pos, ByteBuffer bb)
        throws IOException;

    protected abstract void doRead(long pos, long ptr, int length)
        throws IOException;

    protected abstract void doWrite(long pos, byte[] buf, int offset, int length)
        throws IOException;

    protected abstract void doWrite(long pos, ByteBuffer bb)
        throws IOException;

    protected abstract void doWrite(long pos, long ptr, int length)
        throws IOException;

    protected abstract Mapping openMapping(boolean readOnly, long pos, int size)
        throws IOException;

    protected abstract void reopen() throws IOException;

    protected abstract void doSync(boolean metadata) throws IOException;
}
