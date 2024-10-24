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

import java.io.File;
import java.io.IOException;

import java.util.EnumSet;

import com.sun.jna.platform.win32.WinNT;

import org.cojen.tupl.diag.EventListener;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class WindowsMappedPageArray extends MappedPageArray {
    private final WinNT.HANDLE mFileHandle;
    private final WinNT.HANDLE mMappingHandle;

    private final boolean mNonDurable;

    private volatile boolean mEmpty;

    WindowsMappedPageArray(int pageSize, long pageCount,
                           File file, EnumSet<OpenOption> options, EventListener listener)
        throws IOException
    {
        super(pageSize, pageCount, options);

        if (file == null) {
            setMappingPtr(WindowsFileIO.valloc(pageSize * pageCount, listener));

            mFileHandle = null;
            mMappingHandle = null;
            mNonDurable = true;
            mEmpty = true;

            return;
        }

        mNonDurable = options.contains(OpenOption.NON_DURABLE);
        mEmpty = file.length() == 0;

        WindowsFileIO.MappedFile mf =
            WindowsFileIO.mapFile(file, 0, pageSize * pageCount, options, false);

        mFileHandle = mf.fileHandle();
        mMappingHandle = mf.mappingHandle();

        setMappingPtr(mf.addr());
    }

    @Override
    public long pageCount() {
        return mEmpty ? 0 : super.pageCount();
    }

    @Override
    public void truncatePageCount(long count) {
        mEmpty = count == 0;
    }

    @Override
    void doSync(long mappingPtr, boolean metadata) throws IOException {
        if (!mNonDurable) {
            WindowsFileIO.flushMapping(mFileHandle, mappingPtr, super.pageCount() * pageSize());
        }
        mEmpty = false;
    }

    @Override
    void doSyncPage(long mappingPtr, long index) throws IOException {
        if (!mNonDurable) {
            int pageSize = pageSize();
            WindowsFileIO.flushMapping(mFileHandle, mappingPtr + index * pageSize, pageSize);
        }
        mEmpty = false;
    }

    @Override
    void doClose(long mappingPtr) throws IOException {
        WindowsFileIO.closeMappedFile(mMappingHandle, mFileHandle, mappingPtr);
    }
}
