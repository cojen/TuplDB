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

/**
 * Basic {@link PageArray} implementation which accesses a file.
 *
 * @author Brian S O'Neill
 */
public class FilePageArray extends PageArray {
    final FileIO mFio;

    public FilePageArray(int pageSize, File file, EnumSet<OpenOption> options) throws IOException {
        this(pageSize, file, null, options);
    }

    public FilePageArray(int pageSize, File file, FileFactory factory,
                         EnumSet<OpenOption> options)
        throws IOException
    {
        super(pageSize);

        if (factory != null
            && options.contains(OpenOption.CREATE)
            && !options.contains(OpenOption.NON_DURABLE)
            && !options.contains(OpenOption.READ_ONLY))
        {
            factory.createFile(file);
        }

        mFio = FileIO.open(file, options);
    }

    @Override
    public boolean isReadOnly() {
        return mFio.isReadOnly();
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mFio.length() == 0;
    }

    @Override
    public long getPageCount() throws IOException {
        // Always round page count down. A partial last page effectively doesn't exist.
        return mFio.length() / mPageSize;
    }

    @Override
    public void setPageCount(long count) throws IOException {
        if (count < 0) {
            throw new IllegalArgumentException(String.valueOf(count));
        }
        if (isReadOnly()) {
            return;
        }
        mFio.setLength(count * mPageSize);
    }

    @Override
    public void readPage(long index, byte[] dst, int offset, int length) throws IOException {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        mFio.read(index * mPageSize, dst, offset, length);
    }

    @Override
    public void readPage(long index, long dstPtr, int offset, int length) throws IOException {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        mFio.read(index * mPageSize, dstPtr, offset, length);
    }

    @Override
    public void writePage(long index, byte[] src, int offset) throws IOException {
        int pageSize = mPageSize;
        mFio.write(index * pageSize, src, offset, pageSize);
    }

    @Override
    public void writePage(long index, long srcPtr, int offset) throws IOException {
        int pageSize = mPageSize;
        mFio.write(index * pageSize, srcPtr, offset, pageSize);
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        mFio.sync(metadata);
        // If mapped, now is a good time to remap if length has changed.
        mFio.remap();
    }

    @Override
    public void close(Throwable cause) throws IOException {
        Utils.close(mFio, cause);
    }
}
