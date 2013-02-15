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

package org.cojen.tupl;

import java.io.Closeable;
import java.io.IOException;

/**
 * Lowest IO interface to a file or device.
 *
 * @author Brian S O'Neill
 */
interface FileIO extends Closeable {
    public boolean isReadOnly();

    public long length() throws IOException;

    /**
     * Attempt to set the length of the file. It isn't critical that the
     * operation succeed, and so any exceptions can be suppressed.
     */
    public void setLength(long length) throws IOException;

    /**
     * @param pos zero-based position in file
     * @param buf receives read data
     * @param offset offset into data buffer
     * @param length amount of data to read
     * @throws IllegalArgumentException
     */
    public void read(long pos, byte[] buf, int offset, int length) throws IOException;

    /**
     * @param pos zero-based position in file
     * @param buf data to write
     * @param offset offset into data buffer
     * @param length amount of data
     * @throws IllegalArgumentException
     */
    public void write(long pos, byte[] buf, int offset, int length) throws IOException;

    /**
     * @param pos zero-based position in file
     * @param buf data to write
     * @param offset offset into data buffer
     * @param length amount of data
     * @throws IllegalArgumentException
     */
    public void writeDurably(long pos, byte[] buf, int offset, int length) throws IOException;

    /**
     * Durably flushes all writes to the underlying device.
     *
     * @param metadata pass true to flush all file metadata
     */
    public void sync(boolean metadata) throws IOException;
}
