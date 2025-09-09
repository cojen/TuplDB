/*
 *  Copyright 2020 Cojen.org
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

import java.io.Closeable;
import java.io.IOException;

import java.util.function.Supplier;

import org.cojen.tupl.core.CheckedSupplier;

/**
 * Compresses and decompresses pages. Instances aren't expected to be thread-safe.
 *
 * @author Brian S O'Neill
 */
public interface PageCompressor extends Closeable {
    /**
     * Returns a supplier of new ZLIB compressors (deflate).
     */
    public static Supplier<PageCompressor> zlib() {
        return (CheckedSupplier<PageCompressor>) ZlibCompressor::new;
    }

    /**
     * Returns a supplier of new ZLIB compressors (deflate).
     *
     * @param level compression level [0..9]
     */
    public static Supplier<PageCompressor> zlib(int level) {
        return (CheckedSupplier<PageCompressor>) () -> new ZlibCompressor(level);
    }

    /**
     * Returns a supplier of new LZ4 compressors.
     */
    public static Supplier<PageCompressor> lz4() {
        return (CheckedSupplier<PageCompressor>) LZ4Compressor::new;
    }

    /**
     * Returns a supplier of new Zstandard compressors. The native zstd library isn't provided
     * by this class, and so it must be installed separately.
     */
    public static Supplier<PageCompressor> zstd() {
        return zstd(0); // 0 selects the default level
    }

    /**
     * Returns a supplier of new Zstandard compressors. The native zstd library isn't provided
     * by this class, and so it must be installed separately.
     *
     * @param level compression level [1..22]
     */
     public static Supplier<PageCompressor> zstd(int level) {
        return (CheckedSupplier<PageCompressor>) () -> new ZstdCompressor(level);
    }

    /**
     * Compress to a byte array from a raw memory address.
     *
     * @return the compressed size
     */
    public int compress(long srcAddr, int srcOff, int srcLen) throws IOException;

    /**
     * Target of the compress method. The array is recycled, although it can be replaced.
     */
    public byte[] compressedBytes();

    /**
     * Decompress to a raw memory address.
     *
     * @param dstLen original size of uncompressed page
     */
    public void decompress(byte[] src, int srcOff, int srcLen, long dstAddr, int dstOff, int dstLen)
        throws IOException;

    public void close();
}
