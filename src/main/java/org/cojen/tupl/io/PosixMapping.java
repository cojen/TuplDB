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

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class PosixMapping extends DirectMapping {
    PosixMapping(int fd, boolean readOnly, long position, int size) throws IOException {
        super(open(fd, readOnly, position, size), size);
    }

    private static long open(int fd, boolean readOnly, long position, int size) throws IOException {
        int prot = readOnly ? 1 : (1 | 2); // PROT_READ | PROT_WRITE
        int flags = 1; // MAP_SHARED
        return PosixFileIO.mmapFd(size, prot, flags, fd, position);
    }

    @Override
    void sync(boolean metadata) throws IOException {
        PosixFileIO.msyncAddr(mAddr, mSize);
    }

    @Override
    public void close() throws IOException {
        PosixFileIO.munmapAddr(mAddr, mSize);
    }
}
