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

package org.cojen.tupl.io;

import java.io.IOException;

import java.nio.ByteBuffer;

/**
 * 
 *
 * @author Brian S O'Neill
 */
@SuppressWarnings("restriction")
final class PosixMapping extends Mapping {
    private final DirectAccess mDirectAccess;
    private final long mAddr;
    private final int mSize;

    PosixMapping(int fd, boolean readOnly, long position, int size) throws IOException {
        mDirectAccess = new DirectAccess();
        int prot = readOnly ? 1 : (1 | 2); // PROT_READ | PROT_WRITE
        int flags = 1; // MAP_SHARED
        mAddr = PosixFileIO.mmapFd(size, prot, flags, fd, position);
        mSize = size;
    }

    @Override
    void read(int start, byte[] b, int off, int len) {
        UNSAFE.copyMemory(null, mAddr + start, b, ARRAY + off, len);
    }

    @Override
    void read(int start, ByteBuffer dst) {
        dst.put(mDirectAccess.prepare(mAddr + start, dst.remaining()));
    }

    @Override
    void write(int start, byte[] b, int off, int len) {
        UNSAFE.copyMemory(b, ARRAY + off, null, mAddr + start, len);
    }

    @Override
    void write(int start, ByteBuffer src) {
        mDirectAccess.prepare(mAddr + start, src.remaining()).put(src);
    }

    @Override
    void sync(boolean metadata) throws IOException {
        PosixFileIO.msyncAddr(mAddr, mSize);
    }

    @Override
    public void close() throws IOException {
        PosixFileIO.munmapAddr(mAddr, mSize);
    }

    private static final sun.misc.Unsafe UNSAFE = UnsafeAccess.obtain();
    private static final long ARRAY = (long) UNSAFE.arrayBaseOffset(byte[].class);
}
