/*
 *  Copyright 2015 Brian S O'Neill
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

import java.io.File;
import java.io.IOException;

import java.nio.ByteBuffer;

import java.nio.channels.ClosedChannelException;

import java.util.EnumSet;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.sun.jna.Platform;

import org.cojen.tupl.DatabaseFullException;

/**
 * {@link PageArray} implementation which accesses a fixed sized file, fully mapped to memory.
 *
 * @author Brian S O'Neill
 */
public abstract class MappedPageArray extends PageArray {
    private final long mPageCount;
    private final boolean mReadOnly;
    private final ReentrantReadWriteLock mLock;

    private long mMappingPtr;
    private Throwable mCause;

    public static MappedPageArray open(int pageSize, long pageCount,
                                       File file, EnumSet<OpenOption> options)
        throws IOException
    {
        if (pageSize < 1 || pageCount < 0 || pageCount > Long.MAX_VALUE / pageSize) {
            throw new IllegalArgumentException();
        }

        if (options == null) {
            options = EnumSet.noneOf(OpenOption.class);
        }

        if (Platform.isWindows()) {
            return new WindowsMappedPageArray(pageSize, pageCount, file, options);
        } else {
            return new PosixMappedPageArray(pageSize, pageCount, file, options);
        }
    }

    MappedPageArray(int pageSize, long pageCount, EnumSet<OpenOption> options) {
        super(pageSize);
        mPageCount = pageCount;
        mReadOnly = options.contains(OpenOption.READ_ONLY);
        mLock = new ReentrantReadWriteLock();
    }

    @Override
    public final boolean isReadOnly() {
        return mReadOnly;
    }

    @Override
    public boolean isEmpty() {
        return getPageCount() == 0;
    }

    @Override
    public long getPageCount() {
        return mPageCount;
    }

    @Override
    public void setPageCount(long count) {
    }

    @Override
    public void readPage(long index, byte[] buf, int offset, int length)
        throws IOException
    {
        readCheck(index);

        Lock lock = mLock.readLock();
        lock.lock();
        try {
            ByteBuffer src = DirectAccess.ref(mappingPtr() + index * mPageSize, length);
            src.get(buf, 0, length);
        } finally {
            lock.unlock();
        }
    }

    public void readPage(long index, long ptr, int offset, int length)
        throws IOException
    {
        readCheck(index);

        Lock lock = mLock.readLock();
        lock.lock();
        try {
            ByteBuffer src = DirectAccess.ref(mappingPtr() + index * mPageSize, length);
            ByteBuffer dst = DirectAccess.ref2(ptr + offset, length);
            dst.put(src);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void writePage(long index, byte[] buf, int offset) throws IOException {
        writeCheck(index);

        Lock lock = mLock.readLock();
        lock.lock();
        try {
            int length = mPageSize;
            ByteBuffer dst = DirectAccess.ref(mappingPtr() + index * mPageSize, length);
            dst.put(buf, 0, length);
        } finally {
            lock.unlock();
        }
    }

    public void writePage(long index, long ptr, int offset) throws IOException {
        writeCheck(index);

        Lock lock = mLock.readLock();
        lock.lock();
        try {
            int length = mPageSize;
            ByteBuffer dst = DirectAccess.ref(mappingPtr() + index * mPageSize, length);
            ByteBuffer src = DirectAccess.ref2(ptr + offset, length);
            dst.put(src);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        Lock lock = mLock.readLock();
        lock.lock();
        try {
            doSync(mappingPtr(), metadata);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void syncPage(long index) throws IOException {
        writeCheck(index);

        Lock lock = mLock.readLock();
        lock.lock();
        try {
            doSyncPage(mappingPtr(), index);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public final void close(Throwable cause) throws IOException {
        long ptr;

        mLock.writeLock().lock();
        try {
            ptr = mMappingPtr;
            if (ptr == 0) {
                return;
            }
            mMappingPtr = 0;
            mCause = cause;
        } finally {
            mLock.writeLock().unlock();
        }

        doClose(ptr);
    }

    void setMappingPtr(long ptr) throws IOException {
        mLock.writeLock().lock();
        try {
            if (mMappingPtr != 0) {
                throw new IllegalStateException();
            }
            mMappingPtr = ptr;
            mCause = null;
        } finally {
            mLock.writeLock().unlock();
        }
    }

    abstract void doSync(long mappingPtr, boolean metadata) throws IOException;

    abstract void doSyncPage(long mappingPtr, long index) throws IOException;

    abstract void doClose(long mappingPtr) throws IOException;

    Lock sharedLock() {
        return mLock.readLock();
    }

    // Must be called with lock held.
    long mappingPtr() throws IOException {
        long mappingPtr = mMappingPtr;
        if (mappingPtr == 0) {
            ClosedChannelException cce = new ClosedChannelException();
            cce.initCause(mCause);
            throw cce;
        }
        return mappingPtr;
    }

    private void readCheck(long index) throws IOException {
        if (index < 0) {
            throw new IOException("Negative page index: " + index);
        }
        if (index >= mPageCount) {
            throw new IOException("Page index too high: " + index + " > " + mPageCount);
        }
    }

    private void writeCheck(long index) throws IOException {
        if (index < 0) {
            throw new IOException("Negative page index: " + index);
        }
        if (index >= mPageCount) {
            throw new DatabaseFullException
                ("Mapped file length limit reached: " + (mPageCount * mPageSize));
        }
    }
}
