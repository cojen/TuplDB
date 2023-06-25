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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InterruptedIOException;
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
final class WindowsFileIO extends JavaFileIO {
    private static final Kernel32Ex cKernel;

    static {
        cKernel = Native.load("kernel32", Kernel32Ex.class, W32APIOptions.UNICODE_OPTIONS);
    }

    WindowsFileIO(File file, EnumSet<OpenOption> options, int openFileCount) throws IOException {
        super(file, options, openFileCount, true);
    }

    @Override
    protected Mapping openMapping(boolean readOnly, long pos, int size) throws IOException {
        return new WindowsMapping(mFile, readOnly, pos, size);
    }

    record MappedFile(WinNT.HANDLE fileHandle, WinNT.HANDLE mappingHandle, long addr) { }

    static MappedFile mapFile(File file, long position, long length, EnumSet<OpenOption> options,
                              boolean shared)
        throws IOException
    {
        int access = WinNT.GENERIC_READ;
        boolean readOnly = options.contains(OpenOption.READ_ONLY);
        if (!readOnly) {
            access |= WinNT.GENERIC_WRITE;
        }

        int create = options.contains(OpenOption.CREATE) ? WinNT.OPEN_ALWAYS : WinNT.OPEN_EXISTING;

        int flags;
        if (options.contains(OpenOption.NON_DURABLE)) {
            flags = WinNT.FILE_ATTRIBUTE_TEMPORARY;
        } else {
            flags = WinNT.FILE_ATTRIBUTE_NORMAL;
        }

        if (options.contains(OpenOption.RANDOM_ACCESS)) {
            flags |= WinNT.FILE_FLAG_RANDOM_ACCESS;
        }

        int shareMode = shared ? (WinNT.FILE_SHARE_READ | WinNT.FILE_SHARE_WRITE) : 0;

        WinNT.HANDLE hFile = cKernel.CreateFile
            (file.getPath(),
             access,
             shareMode,
             null, // security attributes
             create,
             flags,
             null // template file
             );

        if (hFile == null || hFile == WinNT.INVALID_HANDLE_VALUE) {
            throw new FileNotFoundException(lastErrorMessage());
        }

        long maxSize = position + length;

        WinNT.HANDLE hMapping = cKernel.CreateFileMapping
            (hFile,
             null, // security attributes
             readOnly ? WinNT.PAGE_READONLY : WinNT.PAGE_READWRITE,
             (int) (maxSize >>> 32),
             (int) maxSize,
             null // no name
             );

        if (hMapping == null || hMapping == WinNT.INVALID_HANDLE_VALUE) {
            String message = lastErrorMessage();
            closeHandle(hFile);
            throw new IOException(message + " maxSize=" + maxSize +
                                  ", file.length=" + file.length());
        }

        Pointer ptr = cKernel.MapViewOfFile
            (hMapping,
             readOnly ? WinNT.SECTION_MAP_READ : WinNT.SECTION_MAP_WRITE,
             (int) (position >>> 32),
             (int) position,
             length
             );

        if (ptr == null) {
            String message = lastErrorMessage();
            closeHandle(hMapping);
            closeHandle(hFile);
            throw new IOException(message + " position=" + position + ", length=" + length +
                                  ", file.length=" + file.length());
        }

        return new MappedFile(hFile, hMapping, Pointer.nativeValue(ptr));
    }

    static void flushMapping(WinNT.HANDLE fileHandle, long addr, long length) throws IOException {
        /*
          As per the comment in Java_java_nio_MappedMemoryUtils_force0:

          FlushViewOfFile can fail with ERROR_LOCK_VIOLATION if the memory
          system is writing dirty pages to disk. As there is no way to
          synchronize the flushing then we retry a limited number of times.
        */
        for (int i=10;;) {
            if (cKernel.FlushViewOfFile(addr, length)) {
                break;
            }
            i--;
            int error = cKernel.GetLastError();
            if (i <= 0 || error != cKernel.ERROR_LOCK_VIOLATION) {
                throw new IOException(Kernel32Util.formatMessage(error));
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }

        if (fileHandle != null) {
            // Note: Win32 doesn't have a flush metadata flag -- it's implicitly true.
            if (!cKernel.FlushFileBuffers(fileHandle)) {
                throw new IOException(lastErrorMessage());
            }
        }
    }

    static void closeMappedFile(MappedFile mfile) throws IOException {
        closeMappedFile(mfile.fileHandle(), mfile.mappingHandle(), mfile.addr());
    }

    static void closeMappedFile(WinNT.HANDLE fileHandle, WinNT.HANDLE mappingHandle, long addr)
        throws IOException
    {
        if (fileHandle == null) {
            vfree(addr);
        } else {
            if (!cKernel.UnmapViewOfFile(new Pointer(addr))) {
                throw new IOException(lastErrorMessage());
            }
            closeHandle(mappingHandle);
            closeHandle(fileHandle);
        }
    }

    static long valloc(long length) throws IOException {
        long addr = cKernel.VirtualAlloc
            (0, // lpAddress
             length,
             0x1000 | 0x2000, // MEM_COMMIT | MEM_RESERVE
             0x04); // PAGE_READWRITE
 
        if (addr == 0) {
            throw new IOException(lastErrorMessage());
        }

        return addr;
    }

    static void vfree(long addr) throws IOException {
        if (!cKernel.VirtualFree(addr, 0, 0x8000)) { // MEM_RELEASE
            throw new IOException(lastErrorMessage());
        }
    }

    static String lastErrorMessage() {
        return Kernel32Util.formatMessage(cKernel.GetLastError());
    }

    static void closeHandle(WinNT.HANDLE handle) {
        cKernel.CloseHandle(handle);
    }

    public static interface Kernel32Ex extends Kernel32 {
        // The inherited method only supports 32-bit mapping size.
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
