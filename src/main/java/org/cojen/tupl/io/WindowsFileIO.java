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
import java.lang.foreign.StructLayout;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;

import java.util.EnumSet;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

import org.cojen.tupl.util.LocalPool;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class WindowsFileIO extends JavaFileIO {
    static final int INVALID_HANDLE_VALUE = -1;

    private static final int MAX_POOL_SIZE = -4; // 4 * number of available processors

    private static final LocalPool<MemorySegment> errorPool;
    private static final VarHandle errorHandle;

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

        Linker.Option captureError = Linker.Option.captureCallState("GetLastError");
        StructLayout errorLayout = Linker.Option.captureStateLayout();
        errorPool = new LocalPool<>(() -> Arena.ofAuto().allocate(errorLayout), MAX_POOL_SIZE);
        errorHandle = errorLayout.varHandle(StructLayout.PathElement.groupElement("GetLastError"));

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
              ValueLayout.ADDRESS,   // lpFileName
              ValueLayout.JAVA_INT,  // dwDesiredAccess
              ValueLayout.JAVA_INT,  // dwShareMode
              ValueLayout.ADDRESS,   // lpSecurityAttributes
              ValueLayout.JAVA_INT,  // dwCreationDisposition
              ValueLayout.JAVA_INT,  // dwFlagsAndAttributes
              ValueLayout.JAVA_INT), // hTemplateFile
             captureError
             );

        CreateFileMapping = linker.downcallHandle
            (lookup.find("CreateFileMappingW").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_INT,
              ValueLayout.JAVA_INT,  // hFile
              ValueLayout.ADDRESS,   // lpFileMappingAttributes
              ValueLayout.JAVA_INT,  // flProtect
              ValueLayout.JAVA_INT,  // dwMaximumSizeHigh
              ValueLayout.JAVA_INT,  // dwMaximumSizeLow
              ValueLayout.ADDRESS),  // lpName
             captureError
             );

        MapViewOfFile = linker.downcallHandle
            (lookup.find("MapViewOfFile").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_LONG,
              ValueLayout.JAVA_INT,   // hFileMappingObject
              ValueLayout.JAVA_INT,   // dwDesiredAccess
              ValueLayout.JAVA_INT,   // dwFileOffsetHigh
              ValueLayout.JAVA_INT,   // dwFileOffsetLow
              ValueLayout.JAVA_LONG), // dwNumberOfBytesToMap
             captureError
             );

        UnmapViewOfFile = linker.downcallHandle
            (lookup.find("UnmapViewOfFile").get(),
             FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.JAVA_LONG),
             captureError
             );

        FlushViewOfFile = linker.downcallHandle
            (lookup.find("FlushViewOfFile").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_BOOLEAN,
              ValueLayout.JAVA_LONG,  // lpBaseAddress
              ValueLayout.JAVA_LONG), // dwNumberOfBytesToFlush
             captureError
             );

        FlushFileBuffers = linker.downcallHandle
            (lookup.find("FlushFileBuffers").get(),
             FunctionDescriptor.of(ValueLayout.JAVA_BOOLEAN, ValueLayout.JAVA_INT),
             captureError
             );

        VirtualAlloc = linker.downcallHandle
            (lookup.find("VirtualAlloc").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_LONG,
              ValueLayout.JAVA_LONG, // lpAddress
              ValueLayout.JAVA_LONG, // dwSize
              ValueLayout.JAVA_INT,  // flAllocationType
              ValueLayout.JAVA_INT), // flProtect
             captureError
             );

        VirtualFree = linker.downcallHandle
            (lookup.find("VirtualFree").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_BOOLEAN,
              ValueLayout.JAVA_LONG,  // lpAddress
              ValueLayout.JAVA_LONG,  // dwSize
              ValueLayout.JAVA_INT),  // dwFreeType
             captureError
             );
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

        LocalPool.Entry<MemorySegment> ee = errorPool.access();
        try {
            int hMapping = (int) CreateFileMapping.invokeExact
                (ee.get(),
                 hFile,
                 MemorySegment.NULL, // security attributes
                 readOnly ? 2 /*PAGE_READONLY*/ : 4, // PAGE_READWRITE
                 (int) (maxSize >>> 32),
                 (int) maxSize,
                 MemorySegment.NULL // no name
                 );

            if (hMapping == 0 || hMapping == INVALID_HANDLE_VALUE) {
                String message = errorMessage(ee);
                closeHandle(hFile);
                throw new IOException(message + " maxSize=" + maxSize +
                                      ", file.length=" + file.length());
            }

            long addr = (long) MapViewOfFile.invokeExact
                (ee.get(),
                 hMapping,
                 readOnly ? 4 /*SECTION_MAP_READ*/ : 2, // WinNT.SECTION_MAP_WRITE
                 (int) (position >>> 32),
                 (int) position,
                 length
                 );

            if (addr == 0) {
                String message = errorMessage(ee);
                closeHandle(hMapping);
                closeHandle(hFile);
                throw new IOException(message + " position=" + position + ", length=" + length +
                                      ", file.length=" + file.length());
            }

            return new MappedFile(hFile, hMapping, addr);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        } finally {
            ee.release();
        }
    }

    static void flushMapping(int fileHandle, long addr, long length) throws IOException {
        LocalPool.Entry<MemorySegment> ee = errorPool.access();
        try {
            /*
              As per the comment in Java_java_nio_MappedMemoryUtils_force0:

              FlushViewOfFile can fail with ERROR_LOCK_VIOLATION if the memory
              system is writing dirty pages to disk. As there is no way to
              synchronize the flushing then we retry a limited number of times.
            */
            for (int i=10;;) {
                if ((boolean) FlushViewOfFile.invokeExact(ee.get(), addr, length)) {
                    break;
                }
                i--;
                int errorId = errorId(ee);
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
                if (!((boolean) FlushFileBuffers.invokeExact(ee.get(), fileHandle))) {
                    throw new IOException(errorMessage(errorId(ee)));
                }
            }
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        } finally {
            ee.release();
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

    static int errorId(LocalPool.Entry<MemorySegment> ee) {
        return (int) errorHandle.get(ee.get(), 0L);
    }

    static String errorMessage(LocalPool.Entry<MemorySegment> ee) {
        return errorMessage(errorId(ee));
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

            MemorySegment addr = lpBuffer.get(ValueLayout.ADDRESS, 0);

            try {
                var chars = new char[result];
                MemorySegment.copy(addr.reinterpret(result * 2L), ValueLayout.JAVA_CHAR, 0,
                                   chars, 0, chars.length);
                return new String(chars).trim();
            } finally {
                var x = (MemorySegment) LocalFree.invokeExact(addr);
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
        LocalPool.Entry<MemorySegment> ee = errorPool.access();
        try (Arena a = Arena.ofConfined()) {
            char[] path = file.getAbsolutePath().toCharArray();
            MemorySegment lpFileName = a.allocate(8 + path.length * 2L + 2);
            lpFileName.set(ValueLayout.JAVA_CHAR, 0, '\\');
            lpFileName.set(ValueLayout.JAVA_CHAR, 2, '\\');
            lpFileName.set(ValueLayout.JAVA_CHAR, 4, '?');
            lpFileName.set(ValueLayout.JAVA_CHAR, 6, '\\');
            MemorySegment.copy(path, 0, lpFileName, ValueLayout.JAVA_CHAR, 8, path.length);

            int hFile = (int) CreateFile.invokeExact
                (ee.get(), lpFileName, dwDesiredAccess, dwShareMode, lpSecurityAttributes,
                 dwCreationDisposition, dwFlagsAndAttributes, hTemplateFile);

            if (hFile == INVALID_HANDLE_VALUE) {
                throw new FileNotFoundException(errorMessage(ee));
            }

            return hFile;
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        } finally {
            ee.release();
        }
    }

    static long mapViewOfFile(int hFileMappingObject,
                              int dwDesiredAccess,
                              int dwFileOffsetHigh,
                              int dwFileOffsetLow,
                              long dwNumberOfBytesToMap)
        throws IOException
    {
        LocalPool.Entry<MemorySegment> ee = errorPool.access();
        try {
            long addr = (long) MapViewOfFile.invokeExact
                (ee.get(), hFileMappingObject, dwDesiredAccess,
                 dwFileOffsetHigh, dwFileOffsetLow, dwNumberOfBytesToMap);
            if (addr == 0) {
                throw new IOException(errorMessage(errorId(ee)));
            }
            return addr;
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        } finally {
            ee.release();
        }
    }

    static void unmapViewOfFile(long addr) throws IOException {
        LocalPool.Entry<MemorySegment> ee = errorPool.access();
        try {
            if (!((boolean) UnmapViewOfFile.invokeExact(ee.get(), addr))) {
                throw new IOException(errorMessage(ee));
            }
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        } finally {
            ee.release();
        }
    }

    static long valloc(long length, EventListener listener) throws IOException {
        long addr;

        // Try to allocate large pages.
        if (length >= (1L << 30) && (addr = LargePages.tryVallocLarge(length, listener)) != 0) {
            return addr;
        }

        LocalPool.Entry<MemorySegment> ee = errorPool.access();
        try {
            // MEM_COMMIT | MEM_RESERVE
            int flags = 0x1000 | 0x2000;

            addr = (long) VirtualAlloc.invokeExact
                (ee.get(), 0L /*lpAddress*/, length, flags, 0x04 /*PAGE_READWRITE*/);

            if (addr == 0) {
                throw new IOException(errorMessage(ee));
            }

            return addr;
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        } finally {
            ee.release();
        }
    }

    static void vfree(long addr) throws IOException {
        LocalPool.Entry<MemorySegment> ee = errorPool.access();
        try {
            if (!((boolean) VirtualFree.invokeExact(ee.get(), addr, 0L, 0x8000))) { // MEM_RELEASE
                throw new IOException(errorMessage(ee));
            }
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        } finally {
            ee.release();
        }
    }

    private static class LargePages {
        private static final MethodHandle GetCurrentProcess;
        private static final MethodHandle OpenProcessToken;
        private static final MethodHandle LookupPrivilegeValue;
        private static final MethodHandle AdjustTokenPrivileges;
        private static final MethodHandle GetLargePageMinimum;

        static {
            System.loadLibrary("advapi32");
            Linker linker = Linker.nativeLinker();
            SymbolLookup lookup = SymbolLookup.loaderLookup();

            GetCurrentProcess = linker.downcallHandle
                (lookup.find("GetCurrentProcess").get(),
                 FunctionDescriptor.of(ValueLayout.JAVA_LONG));

            OpenProcessToken = linker.downcallHandle
                (lookup.find("OpenProcessToken").get(),
                 FunctionDescriptor.of
                 (ValueLayout.JAVA_BOOLEAN,
                  ValueLayout.JAVA_LONG, // ProcessHandle
                  ValueLayout.JAVA_INT,  // DesiredAccess
                  ValueLayout.ADDRESS)   // TokenHandle
                 );

            LookupPrivilegeValue = linker.downcallHandle
                (lookup.find("LookupPrivilegeValueA").get(),
                 FunctionDescriptor.of
                 (ValueLayout.JAVA_BOOLEAN,
                  ValueLayout.ADDRESS, // lpSystemName
                  ValueLayout.ADDRESS, // lpName
                  ValueLayout.ADDRESS) // lpLuid 
                 );

            AdjustTokenPrivileges = linker.downcallHandle
                (lookup.find("AdjustTokenPrivileges").get(),
                 FunctionDescriptor.of
                 (ValueLayout.JAVA_BOOLEAN,
                  ValueLayout.JAVA_INT,     // TokenHandle
                  ValueLayout.JAVA_BOOLEAN, // DisableAllPrivileges
                  ValueLayout.ADDRESS,      // NewState
                  ValueLayout.JAVA_INT,     // BufferLength
                  ValueLayout.ADDRESS,      // PreviousState
                  ValueLayout.ADDRESS),     // ReturnLength
                 Linker.Option.captureCallState("GetLastError")
                 );

            GetLargePageMinimum = linker.downcallHandle
                (lookup.find("GetLargePageMinimum").get(),
                 FunctionDescriptor.of(ValueLayout.JAVA_LONG));
        }

        /**
         * @return 0 if failed
         */
        static long tryVallocLarge(long length, EventListener listener) throws IOException {
            if (!requestSeLockMemoryPrivilege()) {
                if (listener != null) {
                    listener.notify(EventType.CACHE_INIT_INFO,
                                    "Unable to lock pages in memory for supporting large pages");
                    
                }
            } else {
                // Round up the length if necessary.
                {
                    long largePageSize;
                    try {
                        largePageSize = ((long) GetLargePageMinimum.invokeExact());
                    } catch (Throwable e) {
                        throw Utils.rethrow(e);
                    }

                    length = ((length + largePageSize - 1) / largePageSize) * largePageSize;
                }

                LocalPool.Entry<MemorySegment> ee = errorPool.access();
                try {
                    // MEM_COMMIT | MEM_RESERVE | MEM_LARGE_PAGES
                    int flags = 0x1000 | 0x2000 | 0x20000000;

                    long addr = (long) VirtualAlloc.invokeExact
                        (ee.get(), 0L /*lpAddress*/, length, flags, 0x04 /*PAGE_READWRITE*/);

                    if (addr != 0) {
                        return addr;
                    }

                    if (listener != null) {
                        listener.notify(EventType.CACHE_INIT_INFO,
                                        "Unable to allocate using large pages: " +
                                        errorMessage(ee));
                    }
                } catch (Throwable e) {
                    throw Utils.rethrow(e);
                } finally {
                    ee.release();
                }
            }

            return 0;
        }

        private static boolean requestSeLockMemoryPrivilege() {
            try (Arena a = Arena.ofConfined()) {
                long process = (long) GetCurrentProcess.invokeExact();
                int access = 0x0020; // TOKEN_ADJUST_PRIVILEGES
                MemorySegment tokenRef = a.allocate(ValueLayout.JAVA_LONG);

                if (!((boolean) OpenProcessToken.invokeExact(process, access, tokenRef))) {
                    return false;
                }

                int token = tokenRef.get(ValueLayout.JAVA_INT, 0);

                try {
                    MemorySegment name = a.allocateFrom("SeLockMemoryPrivilege");
                    MemorySegment luidRef = a.allocate(ValueLayout.JAVA_LONG);

                    if (!((boolean) LookupPrivilegeValue
                          .invokeExact(MemorySegment.NULL, name, luidRef)))
                    {
                        return false;
                    }

                    long luid = luidRef.get(ValueLayout.JAVA_LONG, 0);

                    MemorySegment tp = a.allocate(4 + 8 + 4);
                    tp.set(ValueLayout.JAVA_INT, 0, 1);               // PrivilegeCount
                    tp.set(ValueLayout.JAVA_LONG_UNALIGNED, 4, luid); // Luid
                    tp.set(ValueLayout.JAVA_INT, 12, 0x02);           // SE_PRIVILEGE_ENABLED

                    LocalPool.Entry<MemorySegment> ee = errorPool.access();
                    try {
                        if (!((boolean) AdjustTokenPrivileges.invokeExact
                              (ee.get(), token, false, tp, (int) tp.byteSize(),
                               MemorySegment.NULL, MemorySegment.NULL)))
                        {
                            return false;
                        }

                        return errorId(ee) == 0;
                    } finally {
                        ee.release();
                    }
                } finally {
                    closeHandle(token);
                }
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
        }
    }
}
