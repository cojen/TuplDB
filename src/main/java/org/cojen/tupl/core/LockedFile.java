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

package org.cojen.tupl.core;

import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;

import org.cojen.tupl.DatabaseException;

/**
 * Utility to acquire a file lock and report a better exception when the lock cannot be
 * acquired.
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

        RandomAccessFile raf = null;
        FileLock lock;

        try {
            try {
                raf = new RandomAccessFile(file, readOnly ? "r" : "rw");
                lock = raf.getChannel().tryLock(8, Long.MAX_VALUE - 8, readOnly);
                if (lock == null) {
                    String message = "Database is open and locked by another process";
                    try {
                        message = message + ": " + raf.readLong();
                    } catch (EOFException e) {
                        // Ignore.
                    }
                    throw new DatabaseException(message);
                }
            } catch (FileNotFoundException e) {
                if (readOnly) {
                    raf = null;
                    lock = null;
                } else {
                    throw e;
                }
            } catch (OverlappingFileLockException e) {
                throw new DatabaseException("Database is already open in the current process");
            }
        } catch (Throwable e) {
            Utils.closeQuietly(raf);
            throw e;
        }

        mRaf = raf;
        mLock = lock;

        if (!readOnly) {
            raf.writeLong(ProcessHandle.current().pid());
        }
    }

    @Override
    public void close() throws IOException {
        if (mLock != null) {
            try {
                mLock.close();
            } catch (ClosedChannelException e) {
                // Ignore.
            }
        }

        if (mRaf != null) {
            mRaf.close();
        }
    }

    /**
     * Quietly closes the file and deletes it.
     *
     * @param ex returned if non-null
     * @return IOException which was caught, unless first was non-null
     */
    IOException delete(String path, IOException ex) {
        ex = Utils.closeQuietly(ex, this);

        try {
            Utils.delete(new File(path));
        } catch (IOException e) {
            if (ex == null) {
                ex = e;
            } else {
                ex.addSuppressed(e);
            }
        }

        return ex;
    }
}
