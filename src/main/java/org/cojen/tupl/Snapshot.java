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
import java.io.OutputStream;

import org.cojen.tupl.ext.ReplicationManager;

/**
 * Control object used to capture a database snapshot.
 *
 * @author Brian S O'Neill
 * @see Database#beginSnapshot Database.beginSnapshot
 */
public interface Snapshot extends Closeable {
    /**
     * Returns total amount of bytes expected to be written to the snapshot
     * stream.
     */
    public long length();

    /**
     * Returns the {@link ReplicationManager#start position} that the snapsnot applies to.
     */
    public long position();

    /**
     * Writes out snapshot data, and then closes this object. Snapshot aborts
     * if the OutputStream throws an exception or if another thread closes this
     * Snapshot instance.
     *
     * @param out snapshot destination; does not require extra buffering; not auto-closed
     */
    public void writeTo(OutputStream out) throws IOException;

    /**
     * Can be called by another thread to abort the snapshot, causing any
     * thread in the writeTo method to throw an exception.
     */
    public void close() throws IOException;
}
