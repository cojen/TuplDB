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

import java.lang.foreign.MemorySegment;

import java.nio.ByteBuffer;

import net.jpountz.lz4.LZ4Factory;
import net.jpountz.lz4.LZ4Exception;
import net.jpountz.lz4.LZ4FastDecompressor;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class LZ4Compressor implements PageCompressor {
    private final net.jpountz.lz4.LZ4Compressor mCompressor;
    private final LZ4FastDecompressor mDecompressor;

    private byte[] mCompressedBytes;

    LZ4Compressor() {
        var factory = LZ4Factory.fastestInstance();
        mCompressor = factory.fastCompressor();
        mDecompressor = factory.fastDecompressor();
    }

    @Override
    public int compress(long srcAddr, int srcOff, int srcLen) {
        byte[] dst = mCompressedBytes;
        if (dst == null) {
            mCompressedBytes = dst = new byte[srcLen / 16];
        }

        while (true) {
            ByteBuffer bb = MemorySegment.ofAddress
                (srcAddr + srcOff).reinterpret(srcLen).asByteBuffer();
            ByteBuffer wrapped = ByteBuffer.wrap(dst, 0, dst.length);
            try {
                mCompressor.compress(bb, wrapped);
                return wrapped.position();
            } catch (LZ4Exception e) {
                dst = expandCapacity(srcLen);
            }
        }
    }

    private byte[] expandCapacity(int srcLen) {
        int newLen = mCompressedBytes.length << 1;
        newLen = Math.min(newLen, mCompressor.maxCompressedLength(srcLen));
        return mCompressedBytes = new byte[newLen];
    }

    @Override
    public byte[] compressedBytes() {
        return mCompressedBytes;
    }

    @Override
    public void decompress(byte[] src, int srcOff, int srcLen,
                           long dstAddr, int dstOff, int dstLen)
    {
        ByteBuffer bb = MemorySegment.ofAddress(dstAddr + dstOff)
            .reinterpret(dstLen).asByteBuffer();

        mDecompressor.decompress(ByteBuffer.wrap(src, srcOff, srcLen), bb);
    }

    @Override
    public void close() {
    }
}
