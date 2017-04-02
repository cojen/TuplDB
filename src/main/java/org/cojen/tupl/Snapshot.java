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
     * Returns the {@link ReplicationManager#start position} that the snapshot applies to.
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
