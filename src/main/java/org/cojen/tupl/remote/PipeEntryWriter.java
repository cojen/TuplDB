/*
 *  Copyright (C) 2023 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.remote;

import java.io.IOException;

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.Entry;
import org.cojen.tupl.Scanner;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see PipeEntryScanner
 */
final class PipeEntryWriter {
    /**
     * Writes all scanner entries to the pipe and then recycles the pipe if possible. The
     * scanner is always closed as a side-effect of calling this method.
     */
    static void writeAll(Scanner<Entry> scanner, Pipe pipe, boolean recycle) throws IOException {
        try {
            pipe.writeLong(scanner.estimateSize());
            pipe.writeInt(scanner.characteristics());

            for (Entry row = scanner.row(); row != null; row = scanner.step(row)) {
                pipe.writeObject(row.key());
                pipe.writeObject(row.value());
            }

            pipe.writeNull();
            pipe.flush();

            if (recycle && pipe.read() >= 0) { // wait for ack
                pipe.recycle();
            }
        } catch (Throwable e) {
            Utils.closeQuietly(pipe);
            Utils.closeQuietly(scanner);
            throw e;
        }
    }
}
