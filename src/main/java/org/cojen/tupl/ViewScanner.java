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

package org.cojen.tupl;

import java.io.IOException;

import java.util.Arrays;
import java.util.Comparator;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ViewScanner implements Scanner {
    protected View mView;
    protected Cursor mCursor;

    /**
     * @param cursor unpositioned cursor
     */
    ViewScanner(View view, Cursor cursor) throws IOException {
        mView = view;
        mCursor = cursor;
        cursor.first();
    }

    protected ViewScanner(Cursor cursor, View view) {
        mView = view;
        mCursor = cursor;
    }

    @Override
    public Comparator<byte[]> getComparator() {
        return mCursor.getComparator();
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
        try {
            mCursor.next();
            return mCursor.key() != null;
        } catch (UnpositionedCursorException e) {
            return false;
        }
    }

    @Override
    public boolean step(long amount) throws IOException {
        if (amount > 0) {
            try {
                mCursor.skip(amount);
            } catch (UnpositionedCursorException e) {
                return false;
            }
        } else if (amount < 0) {
            throw ViewUtils.fail(this, new IllegalArgumentException());
        }
        return mCursor.key() != null;
    }

    @Override
    public void close() throws IOException {
        mCursor.reset();
    }
}
