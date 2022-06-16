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

package org.cojen.tupl.views;

import java.io.IOException;

import java.util.Comparator;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Scanner;

/**
 * Simple Scanner implementation which wraps a Cursor.
 *
 * @author Brian S O'Neill
 * @see ScannerCursor
 */
public class CursorScanner implements Scanner {
    protected final Cursor mCursor;

    /**
     * @param cursor positioned at the first entry
     */
    public CursorScanner(Cursor cursor) {
        mCursor = cursor;
    }

    @Override
    public Comparator<byte[]> comparator() {
        return mCursor.comparator();
    }

    @Override
    public byte[] key() {
        return mCursor.key();
    }

    @Override
    public byte[] value() {
        return mCursor.value();
    }

    @Override
    public boolean step() throws IOException {
        return ViewUtils.step(mCursor);
    }

    @Override
    public void close() throws IOException {
        mCursor.close();
    }
}
