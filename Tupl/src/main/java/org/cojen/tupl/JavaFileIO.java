/*
 *  Copyright 2012 Brian S O'Neill
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

import java.util.EnumSet;

/**
 * Basic FileIO implementation which uses the Java RandomAccessFile class,
 * unless a more suitable implementation is available.
 *
 * @author Brian S O'Neill
 */
class JavaFileIO implements FileIO {
    static FileIO open(File file, EnumSet<OpenOption> options)
        throws IOException
    {
        return open(file, options, 32);
    }

    static FileIO open(File file, EnumSet<OpenOption> options, int openFileCount)
        throws IOException
    {
        return new JavaFileIO(file, options, openFileCount);
    }

    // Access these fields while synchronized on mFilePool.
    private final RandomAccessFile[] mFilePool;
    private int mFilePoolTop;
    private final boolean mReadOnly;

    private JavaFileIO(File file, EnumSet<OpenOption> options, int openFileCount)
        throws IOException
    {
        String mode;
        if ((mReadOnly = options.contains(OpenOption.READ_ONLY))) {
            mode = "r";
        } else {
            if (!options.contains(OpenOption.CREATE) && !file.exists()) {
                throw new FileNotFoundException(file.getPath());
            }
            mode = options.contains(OpenOption.SYNC_IO) ? "rwd" : "rw";
        }

        if (openFileCount < 1) {
            openFileCount = 1;
        }

        mFilePool = new RandomAccessFile[openFileCount];

        try {
            synchronized (mFilePool) {
                for (int i=0; i<openFileCount; i++) {
                    mFilePool[i] = openRaf(file, mode);
                }
            }
        } catch (Throwable e) {
            throw Utils.closeOnFailure(this, e);
        }
    }

    public boolean isReadOnly() {
        return mReadOnly;
    }

    public long length() throws IOException {
        RandomAccessFile file = accessFile();
        try {
            return file.length();
        } finally {
            yieldFile(file);
        }
    }

    public void setLength(long length) throws IOException {
        RandomAccessFile file = accessFile();
        try {
            file.setLength(length);
        } catch (IOException e) {
            // Ignore.
        } finally {
            yieldFile(file);
        }
    }

    public void read(long pos, byte[] buf, int offset, int length) throws IOException {
        try {
            RandomAccessFile file = accessFile();
            try {
                file.seek(pos);
                file.readFully(buf, offset, length);
            } finally {
                yieldFile(file);
            }
        } catch (EOFException e) {
            throw new EOFException("Attempt to read past end of file: " + pos);
        }
    }

    public void write(long pos, byte[] buf, int offset, int length) throws IOException {
        RandomAccessFile file = accessFile();
        try {
            file.seek(pos);
            file.write(buf, offset, length);
        } finally {
            yieldFile(file);
        }
    }

    public void sync(boolean metadata) throws IOException {
        if (mReadOnly) {
            return;
        }
        RandomAccessFile file = accessFile();
        try {
            file.getChannel().force(metadata);
        } finally {
            yieldFile(file);
        }
    }

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

    private static RandomAccessFile openRaf(File file, String mode) throws IOException {
        try {
            return new RandomAccessFile(file, mode);
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
}
