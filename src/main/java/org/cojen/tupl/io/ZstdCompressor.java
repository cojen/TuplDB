/*
 *  Copyright (C) 2025 Cojen.org
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

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;

import java.lang.invoke.MethodHandle;

import java.nio.charset.StandardCharsets;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
final class ZstdCompressor implements PageCompressor {
    private static final MethodHandle getErrorName;
    private static final MethodHandle compressBound, createCCtx, freeCCtx, compressCCtx;
    private static final MethodHandle createDCtx, freeDCtx, decompressDCtx;

    static {
        SymbolLookup lookup;
        load: {
            for (String name : new String[] {"zstd", "libzstd"}) {
                try {
                    lookup = SymbolLookup.libraryLookup(name, Arena.global());
                    break load;
                } catch (IllegalArgumentException e) {
                    // try another
                }
            }

            throw new UnsatisfiedLinkError("Could not load libzstd");
        }

        Linker linker = Linker.nativeLinker();

        getErrorName = linker.downcallHandle
            (lookup.find("ZSTD_getErrorName").get(),
             FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));

        compressBound = linker.downcallHandle
            (lookup.find("ZSTD_compressBound").get(),
             FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.JAVA_LONG));

        createCCtx = linker.downcallHandle
            (lookup.find("ZSTD_createCCtx").get(),
             FunctionDescriptor.of(ValueLayout.ADDRESS));

        freeCCtx = linker.downcallHandle
            (lookup.find("ZSTD_freeCCtx").get(),
             FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

        compressCCtx = linker.downcallHandle
            (lookup.find("ZSTD_compressCCtx").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_LONG,  // result
              ValueLayout.ADDRESS,    // cctx
              ValueLayout.ADDRESS,    // dst
              ValueLayout.JAVA_LONG,  // dstCapacity
              ValueLayout.ADDRESS,    // src
              ValueLayout.JAVA_LONG,  // srcSize
              ValueLayout.JAVA_INT),  // compressionLevel
             Linker.Option.critical(true)
             );

        createDCtx = linker.downcallHandle
            (lookup.find("ZSTD_createDCtx").get(),
             FunctionDescriptor.of(ValueLayout.ADDRESS));

        freeDCtx = linker.downcallHandle
            (lookup.find("ZSTD_freeDCtx").get(),
             FunctionDescriptor.of(ValueLayout.JAVA_LONG, ValueLayout.ADDRESS));

        decompressDCtx = linker.downcallHandle
            (lookup.find("ZSTD_decompressDCtx").get(),
             FunctionDescriptor.of
             (ValueLayout.JAVA_LONG,  // result
              ValueLayout.ADDRESS,    // dctx
              ValueLayout.ADDRESS,    // dst
              ValueLayout.JAVA_LONG,  // dstCapacity
              ValueLayout.ADDRESS,    // src
              ValueLayout.JAVA_LONG), // srcSize
             Linker.Option.critical(true)
             );
    }

    private final int mLevel;

    private MemorySegment mCompressContext, mDecompressContext;

    private byte[] mCompressedBytes;

    ZstdCompressor(int level) {
        mLevel = level;

        try {
            mCompressContext = (MemorySegment) createCCtx.invokeExact();
            mDecompressContext = (MemorySegment) createDCtx.invokeExact();
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private static String errorMessage(long code) throws Throwable {
        var msg = (MemorySegment) getErrorName.invokeExact(code);
        try {
            return msg.reinterpret(1000).getString(0, StandardCharsets.UTF_8) +
                " (code " + Math.abs(code) + ')';
        } catch (IndexOutOfBoundsException e) {
            return "Error " + Math.abs(code);
        }
    }

    @Override
    public int compress(long srcAddr, int srcOff, int srcLen) {
        byte[] dstBytes = mCompressedBytes;
        if (dstBytes == null) {
            mCompressedBytes = dstBytes = new byte[compressBound(srcLen)];
        }

        try {
            while (true) {
                var result = (long) compressCCtx.invokeExact
                    (mCompressContext,
                     MemorySegment.ofArray(dstBytes),
                     (long) dstBytes.length,
                     MemorySegment.ofAddress(srcAddr + srcOff),
                     (long) srcLen,
                     mLevel);

                if (result < 0) {
                    if (result == -70) { // Destination buffer is too small
                        int newLen = compressBound(srcLen);
                        if (newLen > dstBytes.length) {
                            mCompressedBytes = dstBytes = new byte[newLen];
                            continue;
                        }
                    }
                    throw new IllegalStateException(errorMessage(result));
                }

                return (int) result;
            }
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private static int compressBound(int srcLen) {
        try {
            var size = (long) compressBound.invokeExact((long) srcLen);
            return (int) Math.min(size, Integer.MAX_VALUE);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    @Override
    public byte[] compressedBytes() {
        return mCompressedBytes;
    }

    @Override
    public void decompress(byte[] src, int srcOff, int srcLen,
                           long dstAddr, int dstOff, int dstLen)
    {
        try {
            MemorySegment srcSegment = MemorySegment.ofArray(src);

            if (srcOff != 0) {
                srcSegment = srcSegment.asSlice(srcOff);
            }

            var result = (long) decompressDCtx.invokeExact
                (mDecompressContext,
                 MemorySegment.ofAddress(dstAddr + dstOff),
                 (long) dstLen,
                 srcSegment,
                 (long) srcLen);

            if (result < 0) {
                throw new IllegalStateException(errorMessage(result));
            }
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    @Override
    public void close() {
        try {
            MemorySegment ctx = mCompressContext;
            if (ctx != null) {
                mCompressContext = null;
                var result = (long) freeCCtx.invokeExact(ctx);
            }
        } catch (Throwable e) {
            // ignore
        }

        try {
            MemorySegment ctx = mDecompressContext;
            if (ctx != null) {
                mDecompressContext = null;
                var result = (long) freeDCtx.invokeExact(ctx);
            }
        } catch (Throwable e) {
            // ignore
        }
    }
}
