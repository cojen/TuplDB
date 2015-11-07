/*
 *  Copyright 2015 Brian S O'Neill
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.EnumSet;

import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class PosixMappedPageArray extends MappedPageArray {
    private static Posix cPosix;

    static {
        cPosix = (Posix) Native.loadLibrary(Platform.C_LIBRARY_NAME, Posix.class);
    }

    private final int mFileDescriptor;

    private volatile boolean mEmpty;

    PosixMappedPageArray(int pageSize, long pageCount,
                         File file, EnumSet<OpenOption> options)
        throws IOException
    {
        super(pageSize, pageCount, options);

        // Create file (if necessary) and get proper length.

        long fileLen;
        int fd;

        JavaFileIO fio = new JavaFileIO(file, options, 1);
        try {
            fileLen = fio.length();

            mEmpty = fileLen == 0;

            // Select O_RDONLY or O_RDWR.
            int flags = 0;
            if (!options.contains(OpenOption.READ_ONLY)) {
                flags |= 2;
            }

            fd = cPosix.open(file.getPath(), flags);

            if (fd == -1) {
                int error = Native.getLastError();
                throw toException(error);
            }
        } finally {
            fio.close();
        }

        long mappingSize = pageSize * pageCount;

        if (fileLen < mappingSize) {
            if (options.contains(OpenOption.READ_ONLY)) {
                throw new IOException("File is too short: " + fileLen + " < " + mappingSize);
            }
            // Grow the file or else accessing the mapping will seg fault.
            if (cPosix.ftruncate(fd, mappingSize) == -1) {
                int error = Native.getLastError();
                cPosix.close(fd);
                throw toException(error);
            }
        }

        int prot = 1 | 2; // PROT_READ | PROT_WRITE
        int flags = 1; // MAP_SHARED

        long ptr = cPosix.mmap(0, mappingSize, prot, flags, fd, 0);

        if (ptr == -1) {
            int error = Native.getLastError();
            cPosix.close(fd);
            throw toException(error);
        }

        mFileDescriptor = fd;

        setMappingPtr(ptr);
    }

    @Override
    public long getPageCount() {
        return mEmpty ? 0 : super.getPageCount();
    }

    @Override
    public void setPageCount(long count) {
        mEmpty = count == 0;
    }

    void doSync(long mappingPtr, boolean metadata) throws IOException {
        int flags = 4; // MS_SYNC
        if (cPosix.msync(mappingPtr, super.getPageCount() * pageSize(), flags) == -1) {
            int error = Native.getLastError();
            throw toException(error);
        }

        if (metadata && cPosix.fsync(mFileDescriptor) == -1) {
            int error = Native.getLastError();
            throw toException(error);
        }
    }

    void doSyncPage(long mappingPtr, long index) throws IOException {
        int pageSize = pageSize();
        int flags = 4; // MS_SYNC
        if (cPosix.msync(mappingPtr + index * pageSize, pageSize, flags) == -1) {
            int error = Native.getLastError();
            throw toException(error);
        }
    }

    void doClose(long mappingPtr) throws IOException {
        cPosix.munmap(mappingPtr, super.getPageCount() * pageSize());
        cPosix.close(mFileDescriptor);
    }

    private static IOException toException(int error) {
        return new IOException(cPosix.strerror_r(error, null, 0));
    }

    public static interface Posix extends Library {
        String strerror_r(int errnum, char[] buf, int buflen);

        int open(String path, int oflag);

        int ftruncate(int fd, long length);

        int fsync(int fd);

        int close(int fd);

        long mmap(long addr, long length, int prot, int flags, int fd, long offset);

        int msync(long addr, long length, int flags);

        int munmap(long addr, long length);
    }
}
