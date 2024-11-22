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

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;

import java.lang.invoke.MethodHandle;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class DirectMemory {
    // References the entire address space.
    static final MemorySegment ALL;

    private static final MethodHandle malloc, free;

    static {
        ALL = MemorySegment.NULL.reinterpret(Long.MAX_VALUE);

        Linker linker = Linker.nativeLinker();
        SymbolLookup lookup = linker.defaultLookup();

        malloc = linker.downcallHandle
            (lookup.find("malloc").get(),
             FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));

        free = linker.downcallHandle
            (lookup.find("free").get(),
             FunctionDescriptor.ofVoid(ValueLayout.JAVA_LONG));
    }

    /**
     * Allocate native memory.
     */
    public static long malloc(long size) {
        long addr;
        try {
            addr = (long) malloc.invokeExact(size);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
        if (addr == 0) {
            throw new OutOfMemoryError();
        }
        return addr;
    }

    /**
     * Allocate native memory.
     */
    public static long malloc(long size, boolean aligned) {
        return aligned ? Valloc.valloc(size) : malloc(size);
    }

    /**
     * Allocate native memory, zero filled.
     */
    public static long calloc(long size, boolean aligned) {
        long addr = malloc(size, aligned);
        MemorySegment.ofAddress(addr).reinterpret(size).fill((byte) 0);
        return addr;
    }

    /**
     * Free allocated native memory.
     */
    public static void free(long addr) {
        try {
            free.invokeExact(addr);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    static class Valloc {
        private static final MethodHandle valloc;

        static {
            Linker linker = Linker.nativeLinker();
            SymbolLookup lookup = linker.defaultLookup();

            valloc = linker.downcallHandle
                (lookup.find("valloc").get(),
                 FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));
        }

        // TODO: Define a variant that works on Windows. Call WindowsFileIO.valloc, but must
        // also call WindowsFileIO.vfree.
        static long valloc(long size) {
            long addr;
            try {
                addr = (long) valloc.invokeExact(size);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
            if (addr == 0) {
                throw new OutOfMemoryError();
            }
            return addr;
        }
    }
}
