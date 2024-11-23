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

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class PosixMappedPageArray extends MappedPageArray {
    private final int mFileDescriptor;

    private volatile boolean mEmpty;

    PosixMappedPageArray(int pageSize, long pageCount,
                         File file, EnumSet<OpenOption> options, EventListener listener)
        throws IOException
    {
        super(pageSize, pageCount, options);

        int prot = 1; // PROT_READ
        if (!options.contains(OpenOption.READ_ONLY)) {
            prot |= 2; // PROT_WRITE
        }

        if (file == null) {
            int flags = 2; // MAP_PRIVATE
            flags |= PosixFileIO.OS_TYPE == PosixFileIO.OSX ? 0x1000 : 0x20; // MAP_ANONYMOUS

            long mappingSize = pageSize * pageCount;
            long addr = PosixFileIO.mmapFd(mappingSize, prot, flags, -1, 0);

            hugePages(addr, mappingSize, listener);

            setMappingAddr(addr);

            mFileDescriptor = -1;
            mEmpty = true;

            return;
        }

        // Create file (if necessary) and get proper length.

        long fileLen;
        int fd;

        if (options.contains(OpenOption.NON_DURABLE)) {
            fd = PosixFileIO.openFd(file, options);
            try {
                fileLen = PosixFileIO.lseekEndFd(fd, 0);
                PosixFileIO.lseekSetFd(fd, 0);
            } catch (IOException e) {
                try {
                    PosixFileIO.closeFd(fd);
                } catch (IOException e2) {
                    Utils.suppress(e, e2);
                }
                throw e;
            }
        } else {
            try (var fio = new JavaFileIO(file, options, 1, false)) {
                fileLen = fio.length();
                fd = PosixFileIO.openFd(file, options);
            }
        }

        mEmpty = fileLen == 0;

        long mappingSize = pageSize * pageCount;

        if (fileLen < mappingSize) {
            if (options.contains(OpenOption.READ_ONLY)) {
                throw new IOException("File is too short: " + fileLen + " < " + mappingSize);
            }
            // Grow the file or else accessing the mapping will seg fault.
            try {
                PosixFileIO.ftruncateFd(fd, mappingSize);
            } catch (IOException e) {
                try {
                    PosixFileIO.closeFd(fd);
                } catch (IOException e2) {
                    Utils.suppress(e, e2);
                }
                throw e;
            }
        }

        long addr;
        try {
            int flags = 1; // MAP_SHARED
            addr = PosixFileIO.mmapFd(mappingSize, prot, flags, fd, 0);

            if (options.contains(OpenOption.RANDOM_ACCESS)) {
                PosixFileIO.madviseAddr(addr, mappingSize, 1); // 1 = POSIX_MADV_RANDOM
            }

            /* Performance appears to be worse with this option.
            if (options.contains(OpenOption.NON_DURABLE)) {
                // Only works when /sys/kernel/mm/transparent_hugepage/shmem_enabled is set to
                // 'advise' or some other appropriate value.
                hugePages(addr, mappingSize, listener);
            }
            */
        } catch (IOException e) {
            try {
                PosixFileIO.closeFd(fd);
            } catch (IOException e2) {
                Utils.suppress(e, e2);
            }
            throw e;
        }

        mFileDescriptor = fd;

        setMappingAddr(addr);
    }

    private static void hugePages(long addr, long mappingSize, EventListener listener) {
        if (mappingSize >= (1L << 30) && PosixFileIO.OS_TYPE == PosixFileIO.LINUX) {
            try {
                PosixFileIO.madviseAddr(addr, mappingSize, 14); // 14 = MADV_HUGEPAGE
            } catch (IOException e) {
                if (listener != null) {
                    listener.notify
                        (EventType.CACHE_INIT_INFO,
                         "Unable to allocate using transparent huge pages");
                }
            }
        }
    }

    @Override
    public long pageCount() {
        return mEmpty ? 0 : super.pageCount();
    }

    @Override
    public void truncatePageCount(long count) {
        mEmpty = count == 0;
    }

    void doSync(long mappingAddr, boolean metadata) throws IOException {
        if (mFileDescriptor != -1) {
            PosixFileIO.msyncAddr(mappingAddr, super.pageCount() * pageSize());
            if (metadata) {
                PosixFileIO.fsyncFd(mFileDescriptor);
            }
        }
        mEmpty = false;
    }

    void doSyncPage(long mappingAddr, long index) throws IOException {
        int pageSize = pageSize();
        PosixFileIO.msyncAddr(mappingAddr + index * pageSize, pageSize);
        mEmpty = false;
    }

    void doClose(long mappingAddr) throws IOException {
        PosixFileIO.munmapAddr(mappingAddr, super.pageCount() * pageSize());
        if (mFileDescriptor != -1) {
            PosixFileIO.closeFd(mFileDescriptor);
        }
    }
}
