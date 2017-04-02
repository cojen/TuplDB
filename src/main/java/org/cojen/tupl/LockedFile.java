/*
 *  Copyright (C) 2011-2017 Cojen.org
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
