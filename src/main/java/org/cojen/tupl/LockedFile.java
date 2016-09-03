/*
 *  Copyright 2012-2015 Cojen.org
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

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class LockedFile implements Closeable {
    private final RandomAccessFile mRaf;
    private final FileLock mLock;

    LockedFile(File file, boolean readOnly) throws IOException {
        try {
            file = file.getCanonicalFile();
        } catch (IOException e) {
        }

        RandomAccessFile raf;
        FileLock lock;

        try {
            raf = new RandomAccessFile(file, readOnly ? "r" : "rw");
            lock = raf.getChannel().tryLock(0, Long.MAX_VALUE, readOnly);
            if (lock == null) {
                throw new DatabaseException("Database is open and locked by another process");
            }
        } catch (FileNotFoundException e) {
            if (readOnly) {
                raf = null;
                lock = null;
            } else {
                throw e;
            }
        } catch (OverlappingFileLockException e) {
            throw new DatabaseException("Database is already open by current process");
        }

        mRaf = raf;
        mLock = lock;
    }

    @Override
    public void close() throws IOException {
        if (mLock != null) {
            mLock.close();
        }
        if (mRaf != null) {
            mRaf.close();
        }
    }
}
