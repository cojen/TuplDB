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

package org.cojen.tupl.io;

import java.lang.foreign.ValueLayout;

import java.io.File;
import java.io.IOException;

import java.util.EnumSet;

import java.nio.channels.FileChannel;

import java.nio.file.FileSystemException;
import java.nio.file.StandardOpenOption;

import org.cojen.tupl.core.CheckedSupplier;

/**
 * Lowest I/O interface to a file or device.
 *
 * @author Brian S O'Neill
 */
public abstract sealed class FileIO implements CauseCloseable permits AbstractFileIO {
    private static final int IO_TYPE; // 0: platform independent, 1: POSIX, 2: Windows

    static {
        boolean isWindows = System.getProperty("os.name").startsWith("Windows");
        IO_TYPE = ValueLayout.ADDRESS.byteSize() < 8 ? 0 : (isWindows ? 2 : 1);
    }

    public static FileIO open(File file, EnumSet<OpenOption> options)
        throws IOException
    {
        CheckedSupplier.check(2);
        return doOpen(file, options, -4); // 4 * number of available processors
    }

    public static FileIO open(File file, EnumSet<OpenOption> options, int openFileCount)
        throws IOException
    {
        CheckedSupplier.check(2);
        return doOpen(file, options, openFileCount);
    }

    private static FileIO doOpen(File file, EnumSet<OpenOption> options, int openFileCount)
        throws IOException
    {
        if (options == null) {
            options = EnumSet.noneOf(OpenOption.class);
        }
        return switch (IO_TYPE) {
            default -> new JavaFileIO(file, options, openFileCount);
            case 1 -> new PosixFileIO(file, options);
            case 2 -> new WindowsFileIO(file, options, openFileCount);
        };
    }

    FileIO() {
    }

    public abstract boolean isDirectIO();

    public abstract boolean isReadOnly();

    public abstract long length() throws IOException;

    /**
     * Attempt to truncate the length of the file. It isn't critical that the operation
     * succeed, and so any exceptions can be suppressed.
     */
    public abstract void truncateLength(long length) throws IOException;

    /**
     * Attempt to expand the length of the file. It isn't critical that the operation succeed,
     * and so any exceptions can be suppressed.
     */
    public void expandLength(long length) throws IOException {
        expandLength(length, LengthOption.PREALLOCATE_NEVER);
    }

    /**
     * Attempt to expand the length of the file. It isn't critical that the operation succeed,
     * and so any exceptions can be suppressed.
     */
    public abstract void expandLength(long length, LengthOption option) throws IOException;

    /**
     * @param pos zero-based position in file
     * @param buf receives read data
     * @param offset offset into data buffer
     * @param length amount of data to read
     * @throws IllegalArgumentException
     */
    public abstract void read(long pos, byte[] buf, int offset, int length) throws IOException;

    public void read(long pos, long addr, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * @param pos zero-based position in file
     * @param buf data to write
     * @param offset offset into data buffer
     * @param length amount of data
     * @throws IllegalArgumentException
     */
    public abstract void write(long pos, byte[] buf, int offset, int length) throws IOException;

    public void write(long pos, long addr, int offset, int length) throws IOException {
        throw new UnsupportedOperationException();
    }

    /**
     * Maps or remaps the file into main memory, up to the current file length.
     */
    public abstract void map() throws IOException;

    /**
     * If file is mapped, remaps it if the file length has changed. Method does nothing if not
     * already mapped.
     */
    public abstract void remap() throws IOException;

    /**
     * Unmaps the file from main memory, leaving the file open. Method does nothing if not
     * already mapped.
     */
    public abstract void unmap() throws IOException;

    /**
     * Durably flushes all writes to the underlying device.
     *
     * @param metadata pass true to flush all file metadata
     */
    public abstract void sync(boolean metadata) throws IOException;

    @Override
    public void close() throws IOException {
        close(null);
    }

    public abstract boolean isClosed();

    /**
     * Durably flushes the given directory, if required. If the given file is not a directory,
     * the parent directory is flushed.
     */
    public static void dirSync(File file) throws IOException {
        if (IO_TYPE == 2) { // Windows
            return;
        }

        while (!file.isDirectory()) {
            file = file.getParentFile();
            if (file == null) {
                return;
            }
        }

        if (IO_TYPE == 1) {
            int fd = PosixFileIO.openFd(file, EnumSet.of(OpenOption.READ_ONLY));
            try {
                PosixFileIO.fsyncFd(fd);
            } finally {
                PosixFileIO.closeFd(fd);
            }
        } else {
            try (FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
                fc.force(true);
            } catch (FileSystemException e) {
                // Not supported.
            }
        }
    }
}
