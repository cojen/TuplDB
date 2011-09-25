/*
 *  Copyright 2011 Brian S O'Neill
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

package org.cojen.tupl;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InterruptedIOException;
import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Implementation which maps pages to a single file.
 *
 * @author Brian S O'Neill
 */
class FilePageArray implements PageArray {
    private final File mFile;

    private final boolean mReadOnly;
    private final int mPageSize;

    // Access these fields while synchronized on mFilePool.
    private final RandomAccessFile[] mFilePool;
    private int mFilePoolTop;

    private final Object mFileLengthLock;
    private long mFileLength;

    FilePageArray(File file, boolean readOnly, int pageSize, int openFileCount)
        throws IOException
    {
        if (pageSize < 1) {
            throw new IllegalArgumentException("Page size must be at least 1: " + pageSize);
        }
        if (openFileCount < 1) {
            throw new IllegalArgumentException
                ("Open file count must be at least 1: " + openFileCount);
        }

        mReadOnly = readOnly;
        mFilePool = new RandomAccessFile[openFileCount];

        try {
            file = file.getCanonicalFile();
            mFile = file;

            synchronized (mFileLengthLock = new Object()) {
                synchronized (mFilePool) {
                    for (int i=0; i<openFileCount; i++) {
                        mFilePool[i] = open(file, readOnly);
                    }
                    mFileLength = mFilePool[0].length();
                }
            }

            int readPageSize = readPageSize(mFilePool[0]);
            if (readPageSize > 0) {
                pageSize = readPageSize;
            }
        } catch (Throwable e) {
            throw Utils.closeOnFailure(this, e);
        }

        mPageSize = pageSize;
    }

    /**
     * @return 0 to use default
     */
    protected int readPageSize(RandomAccessFile raf) throws IOException {
        return 0;
    }

    @Override
    public boolean isReadOnly() {
        return mReadOnly;
    }

    @Override
    public int pageSize() {
        return mPageSize;
    }

    @Override
    public long getPageCount() throws IOException {
        synchronized (mFilePool) {
            // Always round page count down. A partial last page effectively doesn't exist.
            return mFileLength / mPageSize;
        }
    }

    @Override
    public void setPageCount(long count, boolean grow) throws IOException {
        if (count < 0) {
            throw new IllegalArgumentException(String.valueOf(count));
        }

        long endPos = count * mPageSize;

        synchronized (mFileLengthLock) {
            if (endPos > mFileLength) {
                mFileLength = endPos;
                if (grow && (count & 31) == 0) {
                    RandomAccessFile file = accessFile();
                    try {
                        file.seek(endPos - 1);
                        file.write(-1);
                    } finally {
                        yieldFile(file);
                    }
                }
            } else if (endPos < mFileLength) {
                RandomAccessFile file = accessFile();
                try {
                    file.setLength(endPos);
                } finally {
                    yieldFile(file);
                }
                mFileLength = endPos;
            }
        }
    }

    @Override
    public void readPage(long index, byte[] buf) throws IOException {
        readPage(index, buf, 0);
    }

    @Override
    public void readPage(long index, byte[] buf, int offset) throws IOException {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }

        int pageSize = mPageSize;

        try {
            RandomAccessFile file = accessFile();
            try {
                file.seek(index * pageSize);
                file.readFully(buf, offset, pageSize);
            } finally {
                yieldFile(file);
            }
        } catch (EOFException e) {
            throw new EOFException("Attempt to read page past end of file: " + index);
        }
    }

    @Override
    public int readPartial(long index, int start, byte[] buf, int offset, int length)
        throws IOException
    {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }

        int pageSize = mPageSize;
        int remaining = pageSize - start;
        length = remaining <= 0 ? 0 : Math.min(remaining, length);

        try {
            RandomAccessFile file = accessFile();
            try {
                file.seek(index * pageSize + start);
                file.readFully(buf, offset, length);
                return length;
            } finally {
                yieldFile(file);
            }
        } catch (EOFException e) {
            throw new EOFException("Attempt to read page past end of file: " + index);
        }
    }

    @Override
    public void writePage(long index, byte[] buf) throws IOException {
        writePage(index, buf, 0);
    }

    @Override
    public void writePage(long index, byte[] buf, int offset) throws IOException {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }

        int pageSize = mPageSize;
        long pos = index * pageSize;

        synchronized (mFileLengthLock) {
            if ((pos + pageSize) > mFileLength) {
                mFileLength = pos + pageSize;
            }
        }

        RandomAccessFile file = accessFile();
        try {
            file.seek(pos);
            file.write(buf, offset, pageSize);
        } finally {
            yieldFile(file);
        }
    }

    @Override
    public void flush() throws IOException {
        RandomAccessFile file = accessFile();
        try {
            file.getFD().sync();
        } finally {
            yieldFile(file);
        }
    }

    @Override
    public void close() throws IOException {
        IOException ex = null;

        RandomAccessFile[] pool = mFilePool;
        synchronized (pool) {
            for (RandomAccessFile file : pool) {
                if (file != null) {
                    try {
                        file.close();
                    } catch (IOException e) {
                        if (ex == null) {
                            ex = e;
                        }
                    }
                }
            }
        }

        if (ex != null) {
            throw ex;
        }
    }

    @Override
    public String toString() {
        return "FilePageArray {file=\"" + mFile + "\"}";
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

    private static RandomAccessFile open(File file, boolean readOnly) throws IOException {
        try {
            return new RandomAccessFile(file, readOnly ? "r" : "rw");
        } catch (FileNotFoundException e) {
            String message = null;

            if (file.isDirectory()) {
                message = "File is a directory";
            } else if (!file.isFile()) {
                message = "Not a normal file";
            } else if (readOnly) {
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
}
