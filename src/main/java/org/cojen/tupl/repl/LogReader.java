/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

import java.io.IOException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
interface LogReader extends StreamReplicator.Reader {
    /**
     * Returns the term at the previous reader index.
     */
    long prevTerm();

    /**
     * Reads whatever log data is available, never higher than a commit index, never higher
     * than a term, and never blocking.
     *
     * @return amount of bytes read, or EOF (-1) if the term end has been reached
     * @throws IllegalStateException if log data was deleted (index is too low)
     */
    int tryRead(byte[] buf, int offset, int length) throws IOException;

    /**
     * Reads whatever log data is available, possibly higher than a commit index, never higher
     * than a term, and never blocking.
     *
     * @return amount of bytes read, or EOF (-1) if the term end has been reached
     * @throws IllegalStateException if log data was deleted (index is too low)
     */
    int tryReadAny(byte[] buf, int offset, int length) throws IOException;

    /**
     * Indicate that the reader isn't intended to be used again, allowing file handles to be
     * closed. Reading again will reopen them.
     */
    void release();
}
