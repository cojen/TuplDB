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

/**
 * Compresses and decompresses pages. Instances aren't expected to be thread-safe.
 *
 * @author Brian S O'Neill
 */
public interface PageCompressor extends Closeable {
    /**
     * Returns a new ZLIB compressor (deflate).
     */
    public static PageCompressor zlib() {
        return new ZlibCompressor();
    }

    /**
     * Returns a new ZLIB compressor (deflate).
     *
     * @param level compression level [0..9]
     */
    public static PageCompressor zlib(int level) {
        return new ZlibCompressor(level);
    }

    /*
    public static PageCompressor lz4() {
        return new LZ4Compressor();
    }
    */

    /**
     * Compress to a byte array.
     *
     * @return the compressed size
     */
    public int compress(byte[] src, int srcOff, int srcLen);

    /**
     * Compress to a byte array from a raw memory pointer.
     *
     * @return the compressed size
     */
    public int compress(long srcPtr, int srcOff, int srcLen);

    /**
     * Target of the compress method. The array is recycled, although it can be replaced.
     */
    public byte[] compressedBytes();

    /**
     * Decompress to a byte array.
     *
     * @param dstLen original size of uncompressed page
     */
    public void decompress(byte[] src, int srcOff, byte[] dst, int dstOff, int dstLen);

    /**
     * Decompress to a raw memory pointer.
     *
     * @param dstLen original size of uncompressed page
     */
    public void decompress(byte[] src, int srcOff, long dstPtr, int dstOff, int dstLen);

    public void close();
}
