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

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class DirectMapping extends Mapping {
    // References the entire address space.
    static final MemorySegment ALL = MemorySegment.NULL.reinterpret(Long.MAX_VALUE);

    protected final long mAddr;
    protected final int mSize;

    DirectMapping(long addr, int size) {
        mAddr = addr;
        mSize = size;
    }

    @Override
    final int size() {
        return mSize;
    }

    @Override
    final void read(int start, byte[] b, int off, int len) {
        MemorySegment.copy(ALL, ValueLayout.JAVA_BYTE, mAddr + start, b, off, len);
    }

    @Override
    final void read(int start, long addr, int len) {
        MemorySegment.copy(ALL, mAddr + start, ALL, addr, len);
    }

    @Override
    final void write(int start, byte[] b, int off, int len) {
        MemorySegment.copy(b, off, ALL, ValueLayout.JAVA_BYTE, mAddr + start, len);
    }

    @Override
    final void write(int start, long addr, int len) {
        MemorySegment.copy(ALL, addr, ALL, mAddr + start, len);
    }
}
