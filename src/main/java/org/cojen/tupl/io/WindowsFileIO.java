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

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;

import java.lang.invoke.MethodHandle;

import java.util.EnumSet;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class WindowsFileIO extends JavaFileIO {
    static final int INVALID_HANDLE_VALUE = -1;

    private static final MethodHandle GetLastError;
    private static final MethodHandle FormatMessageW;
    private static final MethodHandle LocalFree;
    private static final MethodHandle CloseHandle;
    private static final MethodHandle CreateFile;
    private static final MethodHandle CreateFileMapping;
    private static final MethodHandle MapViewOfFile;
    private static final MethodHandle UnmapViewOfFile;
    private static final MethodHandle FlushViewOfFile;
    private static final MethodHandle FlushFileBuffers;
    private static final MethodHandle VirtualAlloc;
    private static final MethodHandle VirtualFree;

    static {
        System.loadLibrary("kernel32");
        Linker linker = Linker.nativeLinker();
        SymbolLookup lookup = SymbolLookup.loaderLookup();

        GetLastError = linker.downcallHandle
            (lookup.find("GetLastError").get(),
             FunctionDescriptor.of(ValueLayout.JAVA_INT));

        FormatMessageW = linker.downcallHandle
            (lookup.find("FormatMessageW").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_INT,
              ValueLayout.JAVA_INT, // dwFlags
              ValueLayout.ADDRESS,  // lpSource
              ValueLayout.JAVA_INT, // dwMessageId
              ValueLayout.JAVA_INT, // dwLanguageId,
              ValueLayout.ADDRESS,  // lpBuffer,
              ValueLayout.JAVA_INT, // nSize,
              ValueLayout.ADDRESS)  // args
             );
             
        LocalFree = linker.downcallHandle
            (lookup.find("LocalFree").get(),
             FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.ADDRESS));

        CloseHandle = linker.downcallHandle
            (lookup.find("CloseHandle").get(),
             FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.JAVA_INT));

        CreateFile = linker.downcallHandle
            (lookup.find("CreateFileW").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_INT,
              ValueLayout.ADDRESS,  // lpFileName
              ValueLayout.JAVA_INT, // dwDesiredAccess
              ValueLayout.JAVA_INT, // dwShareMode
              ValueLayout.ADDRESS,  // lpSecurityAttributes
              ValueLayout.JAVA_INT, // dwCreationDisposition
              ValueLayout.JAVA_INT, // dwFlagsAndAttributes
              ValueLayout.JAVA_INT) // hTemplateFile
             );

        CreateFileMapping = linker.downcallHandle
            (lookup.find("CreateFileMappingW").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_INT,
              ValueLayout.JAVA_INT, // hFile
              ValueLayout.ADDRESS,  // lpFileMappingAttributes
              ValueLayout.JAVA_INT, // flProtect
              ValueLayout.JAVA_INT, // dwMaximumSizeHigh
              ValueLayout.JAVA_INT, // dwMaximumSizeLow
              ValueLayout.ADDRESS)  // lpName
             );

        MapViewOfFile = linker.downcallHandle
            (lookup.find("MapViewOfFile").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_LONG,
              ValueLayout.JAVA_INT,  // hFileMappingObject
              ValueLayout.JAVA_INT,  // dwDesiredAccess
              ValueLayout.JAVA_INT,  // dwFileOffsetHigh
              ValueLayout.JAVA_INT,  // dwFileOffsetLow
              ValueLayout.JAVA_LONG) // dwNumberOfBytesToMap
             );

        UnmapViewOfFile = linker.downcallHandle
            (lookup.find("UnmapViewOfFile").get(),
             FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.JAVA_LONG));

        FlushViewOfFile = linker.downcallHandle
            (lookup.find("FlushViewOfFile").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_BOOLEAN,
              ValueLayout.JAVA_LONG, // lpBaseAddress
              ValueLayout.JAVA_LONG) // dwNumberOfBytesToFlush
             );

        FlushFileBuffers = linker.downcallHandle
            (lookup.find("FlushFileBuffers").get(),
             FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.JAVA_INT));

        VirtualAlloc = linker.downcallHandle
            (lookup.find("VirtualAlloc").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_LONG,
              ValueLayout.JAVA_LONG, // lpAddress
              ValueLayout.JAVA_LONG, // dwSize
              ValueLayout.JAVA_INT,  // flAllocationType
              ValueLayout.JAVA_INT)  // flProtect
             );

        VirtualFree = linker.downcallHandle
            (lookup.find("VirtualFree").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_BOOLEAN,
              ValueLayout.JAVA_LONG, // lpAddress
              ValueLayout.JAVA_LONG, // dwSize
              ValueLayout.JAVA_INT)  // dwFreeType
             );

        // Invoke this early in case additional classes need to be loaded. The error is
        // clobbered when the JVM makes additional system calls.
        lastErrorId();
    }

    WindowsFileIO(File file, EnumSet<OpenOption> options, int openFileCount) throws IOException {
        super(file, options, openFileCount, true);
    }

    @Override
    protected Mapping openMapping(boolean readOnly, long pos, int size) throws IOException {
        return new WindowsMapping(mFile, readOnly, pos, size);
    }

    record MappedFile(int fileHandle, int mappingHandle, long addr) { }

    static MappedFile mapFile(File file, long position, long length, EnumSet<OpenOption> options,
                              boolean shared)
        throws IOException
    {
        int access = 0x80000000; // GENERIC_READ;
        boolean readOnly = options.contains(OpenOption.READ_ONLY);
        if (!readOnly) {
            access |= 0x40000000; // GENERIC_WRITE;
        }

        int create = options.contains(OpenOption.CREATE) ? 4 /*OPEN_ALWAYS*/ : 3; // OPEN_EXISTING

        int flags;
        if (options.contains(OpenOption.NON_DURABLE)) {
            flags = 0x00000100; // FILE_ATTRIBUTE_TEMPORARY;
        } else {
            flags = 0x00000080; // FILE_ATTRIBUTE_NORMAL;
        }

        if (options.contains(OpenOption.RANDOM_ACCESS)) {
            flags |= 0x10000000; // FILE_FLAG_RANDOM_ACCESS;
        }

        int shareMode = shared ? 3 /*FILE_SHARE_READ | FILE_SHARE_WRITE*/ : 0;

        int hFile = createFile
            (file,
             access,
             shareMode,
             MemorySegment.NULL, // security attributes
             create,
             flags,
             0 // template file
             );

        long maxSize = position + length;

        int hMapping;
        try {
            hMapping = (int) CreateFileMapping.invokeExact
                (hFile,
                 MemorySegment.NULL, // security attributes
                 readOnly ? 2 /*PAGE_READONLY*/ : 4, // PAGE_READWRITE
                 (int) (maxSize >>> 32),
                 (int) maxSize,
                 MemorySegment.NULL // no name
                 );
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }

        if (hMapping == 0 || hMapping == INVALID_HANDLE_VALUE) {
            String message = lastErrorMessage();
            closeHandle(hFile);
            throw new IOException(message + " maxSize=" + maxSize +
                                  ", file.length=" + file.length());
        }

        long addr;
        try {
            addr = (long) MapViewOfFile.invokeExact
                (hMapping,
                 readOnly ? 4 /*SECTION_MAP_READ*/ : 2, // WinNT.SECTION_MAP_WRITE
                 (int) (position >>> 32),
                 (int) position,
                 length
                 );
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }

        if (addr == 0) {
            String message = lastErrorMessage();
            closeHandle(hMapping);
            closeHandle(hFile);
            throw new IOException(message + " position=" + position + ", length=" + length +
                                  ", file.length=" + file.length());
        }

        return new MappedFile(hFile, hMapping, addr);
    }

    static void flushMapping(int fileHandle, long addr, long length) throws IOException {
        /*
          As per the comment in Java_java_nio_MappedMemoryUtils_force0:

          FlushViewOfFile can fail with ERROR_LOCK_VIOLATION if the memory
          system is writing dirty pages to disk. As there is no way to
          synchronize the flushing then we retry a limited number of times.
        */
        for (int i=10;;) {
            boolean result;
            try {
                result = (boolean) FlushViewOfFile.invokeExact(addr, length);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
            if (result) {
                break;
            }
            i--;
            int errorId = lastErrorId();
            if (i <= 0 || errorId != 33) { // ERROR_LOCK_VIOLATION
                throw new IOException(errorMessage(errorId));
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }

        if (fileHandle != INVALID_HANDLE_VALUE) {
            // Note: Win32 doesn't have a flush metadata flag -- it's implicitly true.
            boolean result;
            try {
                result = (boolean) FlushFileBuffers.invokeExact(fileHandle);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
            if (!result) {
                throw new IOException(lastErrorMessage());
            }
        }
    }

    static void closeMappedFile(MappedFile mfile) throws IOException {
        closeMappedFile(mfile.fileHandle(), mfile.mappingHandle(), mfile.addr());
    }

    static void closeMappedFile(int fileHandle, int mappingHandle, long addr)
        throws IOException
    {
        if (fileHandle == INVALID_HANDLE_VALUE) {
            vfree(addr);
        } else {
            unmapViewOfFile(addr);
            closeHandle(mappingHandle);
            closeHandle(fileHandle);
        }
    }

    static String lastErrorMessage() {
        return errorMessage(lastErrorId());
    }

    static int lastErrorId() {
        try {
            return (int) GetLastError.invokeExact();
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    static String errorMessage(int errorId) {
        try (Arena a = Arena.ofConfined()) {
            int dwFlags = 0x00000100 // FORMAT_MESSAGE_ALLOCATE_BUFFER
                | 0x00001000  // FORMAT_MESSAGE_FROM_SYSTEM
                | 0x00000200; // FORMAT_MESSAGE_IGNORE_INSERTS

            MemorySegment lpSource = MemorySegment.NULL;
            int dwLanguageId = 0;
            MemorySegment lpBuffer = a.allocate(ValueLayout.ADDRESS);
            MemorySegment args = MemorySegment.NULL;

            int result = (int) FormatMessageW.invokeExact
                (dwFlags, lpSource, errorId, dwLanguageId, lpBuffer, 0, args);

            if (result == 0) {
                return "Error " + errorId;
            }

            MemorySegment ptr = lpBuffer.get(ValueLayout.ADDRESS, 0);

            try {
                var chars = new char[result];
                MemorySegment.copy(ptr.reinterpret(result * 2), ValueLayout.JAVA_CHAR, 0,
                                   chars, 0, chars.length);
                return new String(chars).trim();
            } finally {
                var x = (MemorySegment) LocalFree.invokeExact(ptr);
            }
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    static void closeHandle(int handle) {
        boolean result;
        try {
            result = (boolean) CloseHandle.invokeExact(handle);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    static int createFile(File file, int dwDesiredAccess, int dwShareMode,
                          MemorySegment lpSecurityAttributes,
                          int dwCreationDisposition, int dwFlagsAndAttributes, int hTemplateFile)
        throws IOException
    {
        int hFile;
        try (Arena a = Arena.ofConfined()) {
            char[] path = file.getAbsolutePath().toCharArray();
            MemorySegment lpFileName = a.allocate(8 + path.length * 2 + 2);
            lpFileName.set(ValueLayout.JAVA_CHAR, 0, '\\');
            lpFileName.set(ValueLayout.JAVA_CHAR, 2, '\\');
            lpFileName.set(ValueLayout.JAVA_CHAR, 4, '?');
            lpFileName.set(ValueLayout.JAVA_CHAR, 6, '\\');
            MemorySegment.copy(path, 0, lpFileName, ValueLayout.JAVA_CHAR, 8, path.length);

            hFile = (int) CreateFile.invokeExact
                (lpFileName, dwDesiredAccess, dwShareMode, lpSecurityAttributes,
                 dwCreationDisposition, dwFlagsAndAttributes, hTemplateFile);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }

        if (hFile == INVALID_HANDLE_VALUE) {
            throw new FileNotFoundException(lastErrorMessage());
        }

        return hFile;
    }

    static long mapViewOfFile(int hFileMappingObject,
                              int dwDesiredAccess,
                              int dwFileOffsetHigh,
                              int dwFileOffsetLow,
                              long dwNumberOfBytesToMap)
        throws IOException
    {
        long addr;
        try {
            addr = (long) MapViewOfFile.invokeExact
                (hFileMappingObject, dwDesiredAccess,
                 dwFileOffsetHigh, dwFileOffsetLow, dwNumberOfBytesToMap);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }

        if (addr == 0) {
            throw new IOException(lastErrorMessage());
        }

        return addr;
    }

    static void unmapViewOfFile(long addr) throws IOException {
        boolean result;
        try {
            result = (boolean) UnmapViewOfFile.invokeExact(addr);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
        if (!result) {
            throw new IOException(lastErrorMessage());
        }
    }

    static long valloc(long length) throws IOException {
        /* FIXME: Don't use JNA.
        // Try to allocate large pages.
        if (length >= (1L << 30) && requestSeLockMemoryPrivilege()) {
            // Round up if necessary.
            long largePageSize = cKernel.GetLargePageMinimum();
            long largeLength = ((length + largePageSize - 1) / largePageSize) * largePageSize;

            long addr = cKernel.VirtualAlloc
                (0, // lpAddress
                 largeLength,
                 0x1000 | 0x2000 | 0x20000000, // MEM_COMMIT | MEM_RESERVE | MEM_LARGE_PAGES
                 0x04); // PAGE_READWRITE

            if (addr != 0) {
                return addr;
            }
        }
        */

        long addr;
        try {
            addr = (long) VirtualAlloc.invokeExact
                (0L, // lpAddress
                 length,
                 0x1000 | 0x2000, // MEM_COMMIT | MEM_RESERVE
                 0x04); // PAGE_READWRITE
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }

        if (addr == 0) {
            throw new IOException(lastErrorMessage());
        }

        return addr;
    }

    /* FIXME: Don't use JNA.
    private static boolean requestSeLockMemoryPrivilege() {
        WinNT.HANDLE process = cKernel.GetCurrentProcess();

        try {
            var api = Advapi32.INSTANCE;
            int access = cKernel.TOKEN_ADJUST_PRIVILEGES;
            var tokenRef = new WinNT.HANDLEByReference();
            if (!api.OpenProcessToken(process, access, tokenRef)) {
                return false;
            }

            WinNT.HANDLE token = tokenRef.getValue();

            try {
                var luid = new WinNT.LUID();
                if (!api.LookupPrivilegeValue(null, "SeLockMemoryPrivilege", luid)) {
                    return false;
                }

                var tp = new WinNT.TOKEN_PRIVILEGES(1);
                tp.Privileges[0] = new WinNT.LUID_AND_ATTRIBUTES
                    (luid, new WinDef.DWORD(WinNT.SE_PRIVILEGE_ENABLED));

                if (!api.AdjustTokenPrivileges(token, false, tp, tp.size(), null, null)) {
                    return false;
                }

                return cKernel.GetLastError() == cKernel.ERROR_SUCCESS;
            } finally {
                closeHandle(token);
            }
        } finally {
            closeHandle(process);
        }
    }
    */

    static void vfree(long addr) throws IOException {
        boolean result;
        try {
            result = (boolean) VirtualFree.invokeExact(addr, 0L, 0x8000); // MEM_RELEASE
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
        if (!result) {
            throw new IOException(lastErrorMessage());
        }
    }
}
