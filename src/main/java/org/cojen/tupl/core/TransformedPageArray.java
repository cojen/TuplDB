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

package org.cojen.tupl.core;

import java.io.IOException;

import java.util.function.Supplier;

import java.util.zip.Checksum;

import org.cojen.tupl.io.PageArray;

/**
 * Wraps a PageArray and transforms the page contents.
 *
 * @author Brian S O'Neill
 */
abstract class TransformedPageArray extends PageArray implements Compactable {
    protected final PageArray mSource;

    TransformedPageArray(PageArray source) {
        super(source.pageSize());
        mSource = source;
    }

    TransformedPageArray(int pageSize, PageArray source) {
        super(pageSize);
        mSource = source;
    }

    @Override
    public boolean compact(double target) throws IOException {
        return mSource instanceof Compactable c && c.compact(target);
    }

    @Override
    public boolean isClosed() {
        return mSource.isClosed();
    }

    static PageArray rawSource(PageArray array) {
        while (array instanceof TransformedPageArray tpa) {
            array = tpa.mSource;
        }
        return array;
    }

    static Supplier<? extends Checksum> checksumFactory(PageArray array) {
        while (true) {
            if (array instanceof ChecksumPageArray cpa) {
                return cpa.mSupplier;
            }
            if (!(array instanceof TransformedPageArray tpa)) {
                return null;
            }
            array = tpa.mSource;
        }
    }
}
