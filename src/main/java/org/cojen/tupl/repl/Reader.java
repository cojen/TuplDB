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

import java.io.Closeable;
import java.io.IOException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface Reader extends Closeable {
    /**
     * Returns the fixed term this reader is accessing.
     */
    long term();

    /**
     * Returns the next log index which can be read from.
     */
    long index();

    /**
     * Blocks until log messages are available, never reading past a commit index or term.
     *
     * @return amount of bytes read, or EOF (-1) if the term end has been reached
     * @throws IllegalStateException if log was deleted (index is too low)
     */
    default int read(byte[] buf) throws IOException {
        return read(buf, 0, buf.length);
    }

    /**
     * Blocks until log messages are available, never reading past a commit index or term.
     *
     * @return amount of bytes read, or EOF (-1) if the term end has been reached
     * @throws IllegalStateException if log was deleted (index is too low)
     */
    int read(byte[] buf, int offset, int length) throws IOException;

    @Override
    void close();
}
