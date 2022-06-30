/*
 *  Copyright (C) 2022 Cojen.org
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

package org.cojen.tupl.rows;

import java.util.Spliterator;

import java.util.function.Consumer;

import org.cojen.tupl.RowScanner;

/**
 * 
 *
 * @author Brian S O'Neill
 */
interface BaseRowScanner<R> extends RowScanner<R> {
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
            RowUtils.closeQuietly(this);
            throw RowUtils.rethrow(e);
        }
    }

    @Override
    default void forEachRemaining(Consumer<? super R> action) {
        try {
            for (R row = row(); row != null; row = step()) {
                action.accept(row);
            }
        } catch (Throwable e) {
            RowUtils.closeQuietly(this);
            RowUtils.rethrow(e);
        }
    }

    @Override
    default Spliterator<R> trySplit() {
        return null;
    }

    @Override
    default long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    default int characteristics() {
        // FIXME: Unless the query plan is disjoint union, include SORTED. A sorted RowScanner
        // should also report IMMUTABLE, SIZED, and not CONCURRENT. Also, depending on the
        // projection, DISTINCT shouldn't be included.
        return ORDERED | DISTINCT | NONNULL | CONCURRENT;
    }
}
