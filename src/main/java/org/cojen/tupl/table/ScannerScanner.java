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

package org.cojen.tupl.table;

import java.io.IOException;

import org.cojen.tupl.Entry;
import org.cojen.tupl.Scanner;

/**
 * A Scanner backed by a Scanner<Entry>, and rows are decoded along the way.
 *
 * @author Brian S O'Neill
 * @see RowSorter
 */
abstract class ScannerScanner<R> implements Scanner<R> {
    private final Scanner<Entry> mScanner;
    private final RowDecoder<R> mDecoder;

    private R mRow;

    /**
     * @param scanner must produce at least one row
     */
    ScannerScanner(Scanner<Entry> scanner, RowDecoder<R> decoder) throws IOException {
        mScanner = scanner;
        mDecoder = decoder;
        mRow = decoder.decodeRow(null, scanner.row());
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
        Scanner<Entry> s = mScanner;
        R row;
        Entry e = s.row();
        if (e != null && (e = s.step(e)) != null) {
            row = mDecoder.decodeRow(dst, e);
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
