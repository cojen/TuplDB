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
        mCursor.next();
        return mCursor.key() != null;
    }

    @Override
    public boolean step(long amount) throws IOException {
        if (amount > 0) {
            mCursor.skip(amount);
        } else if (amount < 0) {
            throw ViewUtils.fail(this, new IllegalArgumentException());
        }
        return mCursor.key() != null;
    }

    @Override
    public Scanner trySplit() throws IOException {
        try {
            View view = mView;

            if (view.getOrdering() == Ordering.UNSPECIFIED) {
                return null;
            }

            Cursor cursor = mCursor;
            Cursor highCursor = view.newCursor(cursor.link());
            highCursor.autoload(false);
            highCursor.random(cursor.key(), null);

            byte[] highKey = highCursor.key();

            if (highKey == null || Arrays.equals(highKey, cursor.key())) {
                highCursor.reset();
                return null;
            }

            Scanner highScanner = newScanner(highCursor, new BoundedView(view, highKey, null, 0));

            if (cursor.autoload()) {
                highCursor.autoload(true);
                highCursor.load();
            }

            if (cursor instanceof BoundedCursor) {
                BoundedCursor boundedCursor = ((BoundedCursor) cursor);
                view = boundedCursor.mView;
                cursor = boundedCursor.mSource;
            } 

            BoundedView lowView = new BoundedView(view, null, highKey, BoundedView.END_EXCLUSIVE);
            BoundedCursor lowCursor = new BoundedCursor(lowView, cursor);

            mView = lowView;
            mCursor = lowCursor;

            return highScanner;
        } catch (Throwable e) {
            throw ViewUtils.fail(this, e);
        }
    }

    @Override
    public void close() {
        mCursor.reset();
    }

    protected Scanner newScanner(Cursor cursor, View view) {
        return new ViewScanner(cursor, view);
    }
}
