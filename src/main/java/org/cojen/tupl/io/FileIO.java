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

import java.io.File;
import java.io.IOException;

import java.util.EnumSet;

/**
 * Lowest I/O interface to a file or device.
 *
 * @author Brian S O'Neill
 */
public abstract class FileIO implements CauseCloseable {
    public static FileIO open(File file, EnumSet<OpenOption> options)
        throws IOException
    {
        return open(file, options, 32);
    }

    public static FileIO open(File file, EnumSet<OpenOption> options, int openFileCount)
        throws IOException
    {
        return new JavaFileIO(file, options, openFileCount);
    }

    public abstract boolean isReadOnly();

    public abstract long length() throws IOException;

    /**
     * Attempt to set the length of the file. It isn't critical that the
     * operation succeed, and so any exceptions can be suppressed.
     */
    public abstract void setLength(long length) throws IOException;

    /**
     * @param pos zero-based position in file
     * @param buf receives read data
     * @param offset offset into data buffer
     * @param length amount of data to read
     * @throws IllegalArgumentException
     */
    public abstract void read(long pos, byte[] buf, int offset, int length) throws IOException;

    public void read(long pos, long ptr, int offset, int length) throws IOException {
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

    public void write(long pos, long ptr, int offset, int length) throws IOException {
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
}
