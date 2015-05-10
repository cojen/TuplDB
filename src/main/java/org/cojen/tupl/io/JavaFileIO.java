/*
 *  Copyright 2012-2013 Brian S O'Neill
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

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.ByteBuffer;

import java.nio.channels.FileChannel;

import java.util.EnumSet;

import java.util.concurrent.TimeUnit;

import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReadWriteLock;

import static org.cojen.tupl.io.Utils.*;

/**
 * Basic FileIO implementation which uses the Java RandomAccessFile class,
 * unless a more suitable implementation is available.
 *
 * @author Brian S O'Neill
 */
final class JavaFileIO extends FileIO {
    private static final int MAPPING_SHIFT = 30;
    private static final int MAPPING_SIZE = 1 << MAPPING_SHIFT;

    // If sync is taking longer than 10 seconds, start slowing down access.
    private static final long SYNC_YIELD_THRESHOLD_NANOS = 10L * 1000 * 1000 * 1000;

    private static final AtomicIntegerFieldUpdater<JavaFileIO> cSyncCountUpdater =
        AtomicIntegerFieldUpdater.newUpdater(JavaFileIO.class, "mSyncCount");

    private final File mFile;
    private final String mMode;

    // Access these fields while synchronized on mFilePool.
    private final FileAccess[] mFilePool;
    private int mFilePoolTop;
    private final boolean mReadOnly;

    private final Object mRemapLock;
    private final ReadWriteLock mMappingLock;
    private Mapping[] mMappings;
    private int mLastMappingSize;

    private volatile Throwable mCause;

    private final ReadWriteLock mSyncLock;
    private volatile int mSyncCount;
    private volatile long mSyncStartNanos;

    JavaFileIO(File file, EnumSet<OpenOption> options, int openFileCount) throws IOException {
        mFile = file;

        String mode;
        if ((mReadOnly = options.contains(OpenOption.READ_ONLY))) {
            mode = "r";
        } else {
            if (!options.contains(OpenOption.CREATE) && !file.exists()) {
                throw new FileNotFoundException(file.getPath());
            }
            if (options.contains(OpenOption.SYNC_IO)) {
                mode = "rwd";
            } else {
                mode = "rw";
            }
        }

        mMode = mode;

        if (openFileCount < 1) {
            openFileCount = 1;
        }

        mFilePool = new FileAccess[openFileCount];

        mRemapLock = new Object();
        mMappingLock = new ReentrantReadWriteLock(false);
        mSyncLock = new ReentrantReadWriteLock(false);

        try {
            synchronized (mFilePool) {
                for (int i=0; i<openFileCount; i++) {
                    mFilePool[i] = openRaf(file, mode);
                }
            }
        } catch (Throwable e) {
            throw closeOnFailure(this, e);
        }

        if (options.contains(OpenOption.MAPPED)) {
            map();
        }
    }

    @Override
    public boolean isReadOnly() {
        return mReadOnly;
    }

    @Override
    public long length() throws IOException {
        RandomAccessFile file;

        Lock lock = mMappingLock.readLock();
        lock.lock();
        try {
            file = accessFile();
        } finally {
            lock.unlock();
        }

        try {
            return file.length();
        } catch (IOException e) {
            throw rethrow(e, mCause);
        } finally {
            yieldFile(file);
        }
    }

    @Override
    public void setLength(long length) throws IOException {
        RandomAccessFile file;

        Lock lock = mMappingLock.readLock();
        lock.lock();
        try {
            file = accessFile();
        } finally {
            lock.unlock();
        }

        try {
            file.setLength(length);
        } catch (IOException e) {
            // Ignore.
        } finally {
            yieldFile(file);
        }
    }

    @Override
    public void read(long pos, byte[] buf, int offset, int length) throws IOException {
        access(true, pos, buf, offset, length);
    }

    @Override
    public void read(long pos, long ptr, int offset, int length) throws IOException {
        access(true, pos, ptr + offset, length);
    }

    @Override
    public void write(long pos, byte[] buf, int offset, int length) throws IOException {
        access(false, pos, buf, offset, length);
    }

    @Override
    public void write(long pos, long ptr, int offset, int length) throws IOException {
        access(false, pos, ptr + offset, length);
    }

    private void access(boolean read, long pos, byte[] buf, int offset, int length)
        throws IOException
    {
        syncWait();

        try {
            RandomAccessFile file;

            Lock lock = mMappingLock.readLock();
            lock.lock();
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

                file = accessFile();
            } finally {
                lock.unlock();
            }

            try {
                file.seek(pos);
                if (read) {
                    file.readFully(buf, offset, length);
                } else {
                    file.write(buf, offset, length);
                }
            } finally {
                yieldFile(file);
            }
        } catch (IOException e) {
            if (e instanceof EOFException && read) {
                EOFException eof = new EOFException("Attempt to read past end of file: " + pos);
                eof.initCause(mCause);
                throw eof;
            }
            throw rethrow(e, mCause);
        }
    }

    private void access(boolean read, long pos, long ptr, int length) throws IOException {
        if (length <= 0) {
            return;
        }

        syncWait();

        ByteBuffer bb = DirectAccess.ref(ptr, length);
        try {
            RandomAccessFile file;

            Lock lock = mMappingLock.readLock();
            lock.lock();
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
                            mapping.read(mpos, bb);
                        } else {
                            mapping.write(mpos, bb);
                        }

                        if (!bb.hasRemaining()) {
                            return;
                        }

                        pos += mavail;
                    }
                }

                file = accessFile();
            } finally {
                lock.unlock();
            }

            try {
                FileChannel channel = file.getChannel();
                while (true) {
                    int amt;
                    if (read) {
                        amt = channel.read(bb, pos);
                        if (amt < 0) {
                            throw new EOFException("Attempt to read past end of file: " + pos);
                        }
                    } else {
                        amt = channel.write(bb, pos);
                    }
                    length -= amt;
                    if (length <= 0) {
                        break;
                    }
                    pos += amt;
                }
            } finally {
                yieldFile(file);
            }
        } catch (IOException e) {
            throw rethrow(e, mCause);
        } finally {
            DirectAccess.unref(bb);
        }
    }

    private void syncWait() throws InterruptedIOException {
        if (mSyncCount != 0) {
            long syncTimeNanos = System.nanoTime() - mSyncStartNanos;
            if (syncTimeNanos > SYNC_YIELD_THRESHOLD_NANOS) {
                // Yield 1ms for each second that sync has been running. Use a RW lock instead
                // of a sleep, preventing prolonged sleep after sync finishes.
                long sleepMillis = syncTimeNanos / (1000L * 1000 * 1000);
                try {
                    Lock lock = mSyncLock.writeLock();
                    if (lock.tryLock(sleepMillis, TimeUnit.MILLISECONDS)) {
                        lock.unlock();
                    }
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }
        }
    }

    @Override
    public void map() throws IOException {
        map(false);
    }

    @Override
    public void remap() throws IOException {
        map(true);
    }

    private void map(boolean remap) throws IOException {
        synchronized (mRemapLock) {
            Mapping[] oldMappings;
            int oldMappingDiscardPos;
            Mapping[] newMappings;
            int newLastSize;

            Lock lock = mMappingLock.readLock();
            lock.lock();
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
                    newMappings[i++] = Mapping.open(mFile, mReadOnly, pos, MAPPING_SIZE);
                    pos += MAPPING_SIZE;
                }

                if (count == 0) {
                    newLastSize = 0;
                } else {
                    newLastSize = (int) (MAPPING_SIZE - (count * MAPPING_SIZE - length));
                    newMappings[i] = Mapping.open(mFile, mReadOnly, pos, newLastSize);
                }
            } finally {
                lock.unlock();
            }

            lock = mMappingLock.writeLock();
            lock.lock();
            mMappings = newMappings;
            mLastMappingSize = newLastSize;
            lock.unlock();

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
    }

    @Override
    public void unmap() throws IOException {
        synchronized (mRemapLock) {
            Lock lock = mMappingLock.writeLock();
            lock.lock();
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

                for (int i=0; i<mFilePool.length; i++) {
                    try {
                        accessFile().close();
                    } catch (IOException e) {
                        if (ex == null) {
                            ex = e;
                        }
                    }
                }

                for (int i=0; i<mFilePool.length; i++) {
                    try {
                        yieldFile(openRaf(mFile, mMode));
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
                lock.unlock();
            }
        }
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        if (mReadOnly) {
            return;
        }

        int count = cSyncCountUpdater.getAndIncrement(this);
        try {
            if (count == 0) {
                mSyncStartNanos = System.nanoTime();
            }

            mSyncLock.readLock().lock();
            try {
                RandomAccessFile file;

                Lock lock = mMappingLock.readLock();
                lock.lock();
                try {
                    Mapping[] mappings = mMappings;
                    if (mappings != null) {
                        for (Mapping m : mappings) {
                            // Save metadata sync for last.
                            m.sync(false);
                        }
                    }

                    file = accessFile();
                } finally {
                    lock.unlock();
                }

                try {
                    file.getChannel().force(metadata);
                } catch (IOException e) {
                    throw rethrow(e, mCause);
                } finally {
                    yieldFile(file);
                }
            } finally {
                mSyncLock.readLock().unlock();
            }
        } finally {
            cSyncCountUpdater.decrementAndGet(this);
        }
    }

    @Override
    public void close() throws IOException {
        close(null);
    }

    @Override
    public void close(Throwable cause) throws IOException {
        if (cause != null && mCause == null) {
            mCause = cause;
        }

        IOException ex = null;
        try {
            unmap();
        } catch (IOException e) {
            ex = e;
        }

        RandomAccessFile[] pool = mFilePool;
        synchronized (pool) {
            for (RandomAccessFile file : pool) {
                ex = closeQuietly(ex, file, cause);
            }
        }

        if (ex != null) {
            throw ex;
        }
    }

    private RandomAccessFile accessFile() throws InterruptedIOException {
        RandomAccessFile[] pool = mFilePool;
        synchronized (pool) {
            int top;
            while ((top = mFilePoolTop) == pool.length) {
                try {
                    pool.wait();
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }
            RandomAccessFile file = pool[top];
            mFilePoolTop = top + 1;
            return file;
        }
    }

    private void yieldFile(RandomAccessFile file) {
        RandomAccessFile[] pool = mFilePool;
        synchronized (pool) {
            pool[--mFilePoolTop] = file;
            pool.notify();
        }
    }

    static FileAccess openRaf(File file, String mode) throws IOException {
        try {
            return new FileAccess(file, mode);
        } catch (FileNotFoundException e) {
            String message = null;

            if (file.isDirectory()) {
                message = "File is a directory";
            } else if (!file.isFile()) {
                message = "Not a normal file";
            } else if ("r".equals(mode)) {
                if (!file.exists()) {
                    message = "File does not exist";
                } else if (!file.canRead()) {
                    message = "File cannot be read";
                }
            } else {
                if (!file.canRead()) {
                    if (!file.canWrite()) {
                        message = "File cannot be read or written";
                    } else {
                        message = "File cannot be read";
                    }
                } else if (!file.canWrite()) {
                    message = "File cannot be written";
                }
            }

            if (message == null) {
                throw e;
            }

            String path = file.getPath();

            String originalMessage = e.getMessage();
            if (originalMessage.indexOf(path) < 0) {
                message = message + ": " + file.getPath() + ' ' + originalMessage;
            } else {
                message = message + ": " + originalMessage;
            }

            throw new FileNotFoundException(message);
        }
    }

    static class FileAccess extends RandomAccessFile {
        private long mPosition;

        FileAccess(File file, String mode) throws IOException {
            super(file, mode);
            seek(0);
        }

        @Override
        public void seek(long pos) throws IOException {
            if (pos != mPosition) {
                super.seek(pos);
                mPosition = pos;
            }
        }

        @Override
        public int read(byte[] buf) throws IOException {
            return read(buf, 0, buf.length);
        }

        @Override
        public int read(byte[] buf, int offset, int length) throws IOException {
            int amt = super.read(buf, offset, length);
            if (amt > 0) {
                mPosition += amt;
            }
            return amt;
        }

        @Override
        public void write(byte[] buf) throws IOException {
            write(buf, 0, buf.length);
        }

        @Override
        public void write(byte[] buf, int offset, int length) throws IOException {
            super.write(buf, offset, length);
            mPosition += length;
        }
    }
}
