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
import java.io.IOException;

import java.util.HashMap;
import java.util.Map;

import org.cojen.tupl.io.CauseCloseable;
import org.cojen.tupl.io.FileFactory;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class TempFileManager implements CauseCloseable, ShutdownHook {
    private File mBaseFile;
    private final FileFactory mFileFactory;
    private long mCount;
    private Map<File, Closeable> mFiles;

    private Throwable mCause;

    /**
     * @param factory optional
     */
    TempFileManager(File baseFile, FileFactory factory) throws IOException {
        mBaseFile = baseFile;
        mFileFactory = factory;

        // Delete old files.
        Utils.deleteNumberedFiles(baseFile, ".temp.");
    }

    File createTempFile() throws IOException {
        while (true) {
            File file;
            synchronized (this) {
                if (mBaseFile == null) {
                    throw new IOException("Shutting down", mCause);
                }
                file = new File(mBaseFile.getPath() + ".temp." + (mCount++));
                if (mFiles == null) {
                    mFiles = new HashMap<>(4);
                }
                if (mFiles.containsKey(file)) {
                    continue;
                }
                mFiles.put(file, null);
            }

            // Note: File.deleteOnExit should never be used, since it leaks memory.

            if (mFileFactory == null) {
                if (file.createNewFile()) {
                    return file;
                }
            } else if (mFileFactory.createFile(file)) {
                return file;
            }

            synchronized (this) {
                mFiles.remove(file);
            }
        }
    }

    /**
     * Register object to close when file is deleted. Can be CauseCloseable to receive cause.
     */
    synchronized void register(File file, Closeable c) throws IOException {
        if (mFiles == null || !mFiles.containsKey(file)) {
            if (mBaseFile == null) {
                throw new IOException("Shutting down", mCause);
            }
            return;
        }
        if (mFiles.get(file) != null) {
            // Only one registration allowed.
            throw new IllegalStateException();
        }
        mFiles.put(file, c);
    }

    void deleteTempFile(File file) {
        Closeable c;
        synchronized (this) {
            if (mFiles == null || !mFiles.containsKey(file)) {
                return;
            }
            c = mFiles.remove(file);
        }
        Utils.closeQuietly(c);
        file.delete();
    }

    @Override
    public void close() {
        close(null);
    }

    @Override
    public void close(Throwable cause) {
        Map<File, Closeable> files;
        synchronized (this) {
            mBaseFile = null;
            if (cause != null) {
                mCause = cause;
            }
            if (mFiles == null) {
                files = null;
            } else {
                files = new HashMap<>(mFiles);
                mFiles = null;
            }
        }

        if (files != null) {
            for (Closeable c : files.values()) {
                Utils.closeQuietly(c, cause);
            }
            for (File f : files.keySet()) {
                f.delete();
            }
        }
    }

    @Override
    public void shutdown() {
        close();
    }
}
