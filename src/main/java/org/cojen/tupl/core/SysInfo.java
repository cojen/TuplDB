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

package org.cojen.tupl.core;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;

import java.lang.invoke.MethodHandle;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SysInfo {
    private static final int PAGE_SIZE;

    static {
        int pageSize = 4096; // fallback

        Linker linker = Linker.nativeLinker();

        try {
            if (System.getProperty("os.name").startsWith("Windows")) {
                System.loadLibrary("kernel32");
                SymbolLookup lookup = SymbolLookup.loaderLookup();


                MethodHandle GetSystemInfo = linker.downcallHandle
                    (lookup.find("GetSystemInfo").get(),
                     FunctionDescriptor.ofVoid(ValueLayout.ADDRESS));

                var systemInfoLayout = MemoryLayout.structLayout
                    (ValueLayout.JAVA_INT, // dwOemId union {wProcessorArchitecture, wReserved}
                     ValueLayout.JAVA_INT, // dwPageSize
                     ValueLayout.ADDRESS,  // lpMinimumApplicationAddress
                     ValueLayout.ADDRESS,  // lpMaximumApplicationAddress
                     ValueLayout.JAVA_INT, // dwActiveProcessorMask
                     ValueLayout.JAVA_INT, // dwNumberOfProcessors
                     ValueLayout.JAVA_INT, // dwProcessorType
                     ValueLayout.JAVA_INT, // dwAllocationGranularity
                     ValueLayout.JAVA_INT, // wProcessorLevel
                     ValueLayout.JAVA_INT  // wProcessorRevision
                     );

                try (Arena a = Arena.ofConfined()) {
                    MemorySegment systemInfo = a.allocate(systemInfoLayout);
                    GetSystemInfo.invokeExact(systemInfo);
                    pageSize = systemInfo.getAtIndex(ValueLayout.JAVA_INT, 1);
                }
            } else {
                SymbolLookup lookup = linker.defaultLookup();

                MethodHandle getpagesize = linker.downcallHandle
                    (lookup.find("getpagesize").get(),
                     FunctionDescriptor.of(ValueLayout.JAVA_INT));

                pageSize = (int) getpagesize.invokeExact();
            }
        } catch (Throwable e) {
            Utils.uncaught(e);
        }

        PAGE_SIZE = pageSize;
    }

    public static int pageSize() {
        return PAGE_SIZE;
    }
}
