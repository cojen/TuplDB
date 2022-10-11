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

import java.io.IOException;

import org.cojen.tupl.EntryScanner;

/**
 * A RowScanner backed by a plain EntryScanner, and rows are decoded along the way.
 *
 * @author Brian S O'Neill
 * @see RowSorter
 */
abstract class ScannerRowScanner<R> implements BaseRowScanner<R> {
    private final EntryScanner mScanner;
    private final RowDecoder<R> mDecoder;

    private R mRow;

    /**
     * @param scanner must produce at least one row
     */
    ScannerRowScanner(EntryScanner scanner, RowDecoder<R> decoder) throws IOException {
        mScanner = scanner;
        mDecoder = decoder;
        mRow = decoder.decodeRow(null, scanner.key(), scanner.value());
    }

    @Override
    public final R row() {
        return mRow;
    }

    @Override
    public final R step() throws IOException {
        return step(null);
    }

    @Override
    public final R step(R dst) throws IOException {
        EntryScanner s = mScanner;
        R row;
        if (s.step()) {
            row = mDecoder.decodeRow(dst, s.key(), s.value());
        } else {
            row = null;
        }
        mRow = row;
        return row;
    }

    @Override
    public final void close() throws IOException {
        mRow = null;
        mScanner.close();
    }

    @Override
    public final long estimateSize() {
        return Long.MAX_VALUE;
    }
}
