/*
 *  Copyright 2015 Cojen.org
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

import com.sun.jna.Native;
import com.sun.jna.Pointer;

import com.sun.jna.platform.win32.Kernel32;
import com.sun.jna.platform.win32.Kernel32Util;
import com.sun.jna.platform.win32.WinNT;

import com.sun.jna.win32.W32APIOptions;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class WindowsMappedPageArray extends MappedPageArray {
    private static final Kernel32Ex cKernel;

    static {
        cKernel = (Kernel32Ex) Native.loadLibrary
            ("kernel32", Kernel32Ex.class, W32APIOptions.UNICODE_OPTIONS);
    }

    private final File mFile;
    private final EnumSet<OpenOption> mOptions;

    private final WinNT.HANDLE mFileHandle;
    private final WinNT.HANDLE mMappingHandle;

    private final boolean mNonDurable;

    private volatile boolean mEmpty;

    WindowsMappedPageArray(int pageSize, long pageCount,
                           File file, EnumSet<OpenOption> options)
        throws IOException
    {
        super(pageSize, pageCount, options);

        mFile = file;
        mOptions = options;

        if (file == null) {
            long mappingPtr = cKernel.VirtualAlloc
                (0, // lpAddress
                 pageSize * pageCount,
                 0x1000 | 0x2000, // MEM_COMMIT | MEM_RESERVE
                 0x04); // PAGE_READWRITE
 
            if (mappingPtr == 0) {
                int error = cKernel.GetLastError();
                throw new IOException(Kernel32Util.formatMessage(error));
            }

            setMappingPtr(mappingPtr);

            mFileHandle = null;
            mMappingHandle = null;
            mNonDurable = true;
            mEmpty = true;

            return;
        }

        mEmpty = file.length() == 0;
        mNonDurable = options.contains(OpenOption.NON_DURABLE);

        int access = WinNT.GENERIC_READ;

        if (!options.contains(OpenOption.READ_ONLY)) {
            access |= WinNT.GENERIC_WRITE;
        }

        int create = options.contains(OpenOption.CREATE) ? WinNT.OPEN_ALWAYS : WinNT.OPEN_EXISTING;

        int flags;
        if (mNonDurable) {
            flags = WinNT.FILE_ATTRIBUTE_TEMPORARY;
        } else {
            flags = WinNT.FILE_ATTRIBUTE_NORMAL;
        }

        WinNT.HANDLE hFile = cKernel.CreateFile
            (file.getPath(),
             access,
             0, // no sharing
             null, // security attributes
             create,
             flags,
             null // template file
             );

        if (hFile == null || hFile == WinNT.INVALID_HANDLE_VALUE) {
            int error = cKernel.GetLastError();
            throw new FileNotFoundException(Kernel32Util.formatMessage(error));
        }

        long mappingSize = pageSize * pageCount;

        WinNT.HANDLE hMapping = cKernel.CreateFileMapping
            (hFile,
             null, // security attributes
             WinNT.PAGE_READWRITE,
             (int) (mappingSize >>> 32),
             (int) mappingSize,
             null // no name
             );

        if (hMapping == null || hMapping == WinNT.INVALID_HANDLE_VALUE) {
            int error = cKernel.GetLastError();
            closeHandle(hFile);
            throw toException(error);
        }

        access = options.contains(OpenOption.READ_ONLY)
            ? WinNT.SECTION_MAP_READ : WinNT.SECTION_MAP_WRITE;

        Pointer ptr = cKernel.MapViewOfFile
            (hMapping,
             access,
             0, // offset high
             0, // offset low
             mappingSize
             );

        if (ptr == null) {
            int error = cKernel.GetLastError();
            closeHandle(hMapping);
            closeHandle(hFile);
            throw toException(error);
        }

        mFileHandle = hFile;
        mMappingHandle = hMapping;

        setMappingPtr(Pointer.nativeValue(ptr));
    }

    @Override
    public long getPageCount() {
        return mEmpty ? 0 : super.getPageCount();
    }

    @Override
    public void setPageCount(long count) {
        mEmpty = count == 0;
    }

    @Override
    MappedPageArray doOpen() throws IOException {
        boolean empty = mEmpty;
        WindowsMappedPageArray pa = new WindowsMappedPageArray
            (pageSize(), super.getPageCount(), mFile, mOptions);
        pa.mEmpty = empty;
        return pa;
    }

    void doSync(long mappingPtr, boolean metadata) throws IOException {
        if (!mNonDurable) {
            if (!cKernel.FlushViewOfFile(mappingPtr, super.getPageCount() * pageSize())) {
                throw toException(cKernel.GetLastError());
            }
            fsync();
        }
        mEmpty = false;
    }

    void doSyncPage(long mappingPtr, long index) throws IOException {
        if (!mNonDurable) {
            int pageSize = pageSize();
            if (!cKernel.FlushViewOfFile(mappingPtr + index * pageSize, pageSize)) {
                throw toException(cKernel.GetLastError());
            }
            fsync();
        }
        mEmpty = false;
    }

    void doClose(long mappingPtr) throws IOException {
        if (mFileHandle == null) {
            if (!cKernel.VirtualFree(mappingPtr, 0, 0x8000)) { // MEM_RELEASE
                int error = cKernel.GetLastError();
                throw new IOException(Kernel32Util.formatMessage(error)); 
            }
        } else {
            cKernel.UnmapViewOfFile(new Pointer(mappingPtr));
            closeHandle(mMappingHandle);
            closeHandle(mFileHandle);
        }
    }

    private static IOException toException(int error) {
        return new IOException(Kernel32Util.formatMessage(error));
    }

    private static void closeHandle(WinNT.HANDLE handle) throws IOException {
        cKernel.CloseHandle(handle);
    }

    private void fsync() throws IOException {
        if (mFileHandle != null) {
            // Note: Win32 doesn't have a flush metadata flag.
            if (!cKernel.FlushFileBuffers(mFileHandle)) {
                throw toException(cKernel.GetLastError());
            }
        }
    }

    public static interface Kernel32Ex extends Kernel32 {
        // Inherited method only supports 32-bit mapping size.
        Pointer MapViewOfFile(WinNT.HANDLE hFileMappingObject,
                              int dwDesiredAccess,
                              int dwFileOffsetHigh,
                              int dwFileOffsetLow,
                              long dwNumberOfBytesToMap);

        boolean FlushViewOfFile(long baseAddress, long numberOfBytesToFlush);

        long VirtualAlloc(long lpAddress, long dwSize, int flAllocationType, int flProtect);

        boolean VirtualFree(long lpAddress, long dwSize, int dwFreeType);
    }
}
