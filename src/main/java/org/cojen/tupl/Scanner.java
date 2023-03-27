/*
 *  Copyright 2021 Cojen.org
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

import java.util.Spliterator;

import java.util.function.Consumer;

import org.cojen.tupl.io.Utils;

/**
 * Support for scanning through all rows in a table. Any exception thrown when acting upon a
 * scanner automatically closes it.
 *
 * <p>Scanner instances can only be safely used by one thread at a time, and they must be
 * closed when no longer needed. Instances can be exchanged by threads, as long as a
 * happens-before relationship is established. Without proper exclusion, multiple threads
 * interacting with a Scanner instance may cause database corruption.
 *
 * @author Brian S O'Neill
 * @see Table#newScanner Table.newScanner
 * @see Updater
 *
 * @author Brian S O'Neill
 */
public interface Scanner<R> extends Spliterator<R>, Closeable {
    /**
     * Returns a reference to the current row, which is null if the scanner is closed.
     */
    R row();

    /**
     * Step to the next row.
     *
     * @return the next row or null if no more rows remain and scanner has been closed
     */
    default R step() throws IOException {
        return step(null);
    }

    /**
     * Step to the next row.
     *
     * @param row use this for the next row instead of creating a new one; if null is passed
     * in, a new instance will be created if necessary
     * @return the next row or null if no more rows remain and scanner has been closed
     */
    R step(R row) throws IOException;

    /**
     * {@inheritDoc}
     */
    @Override
    default boolean tryAdvance(Consumer<? super R> action) {
        try {
            R row = row();
            if (row == null) {
                return false;
            }
            // Step to the next row before calling the action, in case an exception is
            // thrown. It would be odd if an exception is thrown after a successful accept.
            step();
            action.accept(row);
            return true;
        } catch (Throwable e) {
            Utils.closeQuietly(this);
            throw Utils.rethrow(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    default void forEachRemaining(Consumer<? super R> action) {
        try {
            for (R row = row(); row != null; row = step()) {
                action.accept(row);
            }
        } catch (Throwable e) {
            Utils.closeQuietly(this);
            Utils.rethrow(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    default Spliterator<R> trySplit() {
        return null;
    }

    @Override
    void close() throws IOException;
}
