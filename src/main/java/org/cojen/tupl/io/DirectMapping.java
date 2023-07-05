/*
 *  Copyright (C) 2023 Cojen.org
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

package org.cojen.tupl.io;

import java.io.IOException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class DirectMapping extends Mapping {
    protected final DirectAccess mDirectAccess;
    protected final long mPtr;
    protected final int mSize;

    DirectMapping(long addr, int size) {
        mDirectAccess = new DirectAccess();
        mPtr = addr;
        mSize = size;
    }

    @Override
    final int size() {
        return mSize;
    }

    @Override
    final void read(int start, byte[] b, int off, int len) {
        UnsafeAccess.copy(mPtr + start, b, off, len);
    }

    @Override
    final void read(int start, long ptr, int len) {
        UnsafeAccess.copy(mPtr + start, ptr, len);
    }

    @Override
    final void write(int start, byte[] b, int off, int len) {
        UnsafeAccess.copy(b, off, mPtr + start, len);
    }

    @Override
    final void write(int start, long ptr, int len) {
        UnsafeAccess.copy(ptr, mPtr + start, len);
    }
}
