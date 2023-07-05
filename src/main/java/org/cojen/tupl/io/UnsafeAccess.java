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

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;

import java.lang.invoke.MethodHandle;

import java.lang.reflect.Field;

import java.nio.ByteOrder;

/**
 * Utility for accessing the unsupported Unsafe class.
 *
 * @author Brian S O'Neill
 */
@SuppressWarnings("rawtypes")
public class UnsafeAccess {
    private static final sun.misc.Unsafe UNSAFE;
    private static final Throwable UNSUPPORTED;
    private static final long ARRAY_OFFSET;

    static {
        sun.misc.Unsafe unsafe = null;
        Throwable unsupported = null;

        if (ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN) {
            try {
                Field theUnsafe = sun.misc.Unsafe.class.getDeclaredField("theUnsafe");
                theUnsafe.setAccessible(true);
                unsafe = (sun.misc.Unsafe) theUnsafe.get(null);
            } catch (Throwable e) {
                unsupported = e;
            }
        }

        UNSAFE = unsafe;
        UNSUPPORTED = unsupported;
        ARRAY_OFFSET = unsafe == null ? 0 : unsafe.arrayBaseOffset(byte[].class);
    }

    private UnsafeAccess() {
    }

    /**
     * @return null if not supported
     */
    public static sun.misc.Unsafe tryObtain() {
        return UNSAFE;
    }

    /**
     * @throws UnsupportedOperationException if not supported
     */
    public static sun.misc.Unsafe obtain() throws UnsupportedOperationException {
        if (UNSAFE == null) {
            throw new UnsupportedOperationException(UNSUPPORTED);
        }
        return UNSAFE;
    }

    /**
     * Allocate native memory.
     */
    public static long alloc(int size) {
        return UNSAFE.allocateMemory(size);
    }

    /**
     * Allocate native memory.
     */
    public static long alloc(int size, boolean aligned) {
        return aligned ? Foreign.valloc(size) : UNSAFE.allocateMemory(size);
    }

    /**
     * Allocate native memory, zero filled.
     */
    public static long calloc(int size, boolean aligned) {
        long addr = alloc(size, aligned);
        UNSAFE.setMemory(addr, size, (byte) 0);
        return addr;
    }

    /**
     * Free allocated native memory.
     */
    public static void free(long addr) {
        UNSAFE.freeMemory(addr);
    }

    static class Foreign {
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
            try {
                return (long) valloc.invokeExact(size);
            } catch (Throwable e) {
                throw Utils.rethrow(e);
            }
        }
    }
}
