/*
 *  Copyright 2020 Cojen.org
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

package org.cojen.tupl.views;

import java.io.IOException;

import java.util.Comparator;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.EntryScanner;

/**
 * Interface which combines EntryScanner and Cursor together.
 *
 * @author Brian S O'Neill
 * @see CursorScanner
 */
public interface ScannerCursor extends EntryScanner, Cursor {
    @Override
    public Comparator<byte[]> comparator();

    @Override
    public default boolean step() throws IOException {
        return ViewUtils.step(this);
    }

    @Override
    public void close() throws IOException;
}
