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
import java.io.File;

import java.util.EnumSet;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class WindowsMapping extends DirectMapping {
    private final int mFileHandle;
    private final int mMappingHandle;

    WindowsMapping(File file, boolean readOnly, long position, int size) throws IOException {
        this(open(file, readOnly, position, size), size);
    }

    private WindowsMapping(WindowsFileIO.MappedFile mf, int size) throws IOException{
        super(mf.addr(), size);
        mFileHandle = mf.fileHandle();
        mMappingHandle = mf.mappingHandle();
    }

    private static WindowsFileIO.MappedFile open(File file, boolean readOnly,
                                                 long position, int size)
        throws IOException
    {
        var options = EnumSet.of(OpenOption.RANDOM_ACCESS);
        if (readOnly) {
            options.add(OpenOption.READ_ONLY);
        }
        return WindowsFileIO.mapFile(file, position, size, options, true);
    }

    @Override
    void sync(boolean metadata) throws IOException {
        WindowsFileIO.flushMapping(mFileHandle, mAddr, mSize);
    }

    @Override
    public void close() throws IOException {
        WindowsFileIO.closeMappedFile(mFileHandle, mMappingHandle, mAddr);
    }
}
