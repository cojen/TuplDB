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
import java.util.Objects;

import java.util.function.Supplier;

import org.cojen.tupl.core.CheckedSupplier;

/**
 * Basic {@link PageArray} implementation which accesses a file.
 *
 * @author Brian S O'Neill
 */
public final class FilePageArray extends PageArray {
    final FileIO mFio;

    public static Supplier<PageArray> factory(int pageSize, File file,
                                              EnumSet<OpenOption> options)
    {
        return (CheckedSupplier<PageArray>) () -> new FilePageArray(pageSize, file, options);
    }

    private FilePageArray(int pageSize, File file, EnumSet<OpenOption> options) throws IOException {
        this(pageSize, FileIO.open(file, options));
    }

    public FilePageArray(int pageSize, FileIO fio) {
        super(pageSize);
        mFio = Objects.requireNonNull(fio);
    }

    @Override
    public int directPageSize() {
        int size = pageSize();
        if (mFio.isDirectIO()) {
            size = -size;
        }
        return size;
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
    public long pageCount() throws IOException {
        // Always round page count down. A partial last page effectively doesn't exist.
        return mFio.length() / mPageSize;
    }

    @Override
    public void truncatePageCount(long count) throws IOException {
        if (allowPageCountAdjust(count)) {
            mFio.truncateLength(count * mPageSize);
        }
    }

    @Override
    public void expandPageCount(long count) throws IOException {
        if (allowPageCountAdjust(count)) {
            mFio.expandLength(count * mPageSize, LengthOption.PREALLOCATE_OPTIONAL);
        }
    }

    private boolean allowPageCountAdjust(long count) {
        if (count < 0) {
            throw new IllegalArgumentException(String.valueOf(count));
        }
        return !isReadOnly();
    }

    @Override
    public void readPage(long index, long dstAddr, int offset, int length) throws IOException {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        mFio.read(index * mPageSize, dstAddr, offset, length);
    }

    @Override
    public void writePage(long index, long srcAddr, int offset) throws IOException {
        int pageSize = mPageSize;
        mFio.write(index * pageSize, srcAddr, offset, pageSize);
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        mFio.sync(metadata);
        try {
            // If mapped, now is a good time to remap if the length has changed.
            mFio.remap();
        } catch (IOException e) {
            if (mFio instanceof AbstractFileIO afio) {
                Utils.uncaught(new IOException("Remap failed: " + afio.file(), e));
            } else {
                throw e;
            }
        }
    }

    @Override
    public void close(Throwable cause) throws IOException {
        Utils.close(mFio, cause);
    }

    @Override
    public boolean isClosed() {
        return mFio.isClosed();
    }
}
