/*
 *  Copyright (C) 2021 Cojen.org
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

import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.cojen.tupl.RowScanner;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class RowSpliterator<R> implements Spliterator<R> {
    public static <R> Stream<R> newStream(RowScanner<R> scanner) {
        return StreamSupport.stream(new RowSpliterator<>(scanner), false).onClose(() -> {
            try {
                scanner.close();
            } catch (Throwable e) {
                Utils.rethrow(e);
            }
        });
    }

    private final RowScanner<R> mScanner;

    private RowSpliterator(RowScanner<R> scanner) {
        mScanner = scanner;
    }

    @Override
    public boolean tryAdvance(Consumer<? super R> action) {
        try {
            R row = mScanner.row();
            if (row == null) {
                return false;
            }
            // Step to the next row before calling the action, in case an exception is
            // thrown. It would be odd if an exception is thrown after a successful accept.
            mScanner.step();
            action.accept(row);
            return true;
        } catch (Throwable e) {
            Utils.closeQuietly(mScanner);
            throw Utils.rethrow(e);
        }
    }

    @Override
    public void forEachRemaining(Consumer<? super R> action) {
        try {
            for (R row = mScanner.row(); row != null; row = mScanner.step()) {
                action.accept(row);
            }
        } catch (Throwable e) {
            Utils.closeQuietly(mScanner);
            Utils.rethrow(e);
        }
    }

    @Override
    public Spliterator<R> trySplit() {
        return null;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return ORDERED | DISTINCT | NONNULL | CONCURRENT;
    }
}
