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

import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ZlibCompressor implements PageCompressor {
    private final Deflater mDeflater;
    private final Inflater mInflater;

    private byte[] mCompressedBytes;

    ZlibCompressor() {
        this(Deflater.DEFAULT_COMPRESSION);
    }

    ZlibCompressor(int level) {
        mDeflater = new Deflater(level);
        mInflater = new Inflater();
    }

    @Override
    public int compress(long srcAddr, int srcOff, int srcLen) {
        byte[] dst = mCompressedBytes;
        if (dst == null) {
            mCompressedBytes = dst = new byte[srcLen / 16];
        }

        ByteBuffer bb = MemorySegment.ofAddress(srcAddr + srcOff)
            .reinterpret(srcLen).asByteBuffer();

        try {
            mDeflater.setInput(bb);
            mDeflater.finish();
            int dstOff = 0;
            int len = 0;
            while (true) {
                len += mDeflater.deflate(dst, dstOff, dst.length - dstOff);
                if (mDeflater.finished()) {
                    return len;
                }
                byte[] newDst = new byte[dst.length << 1];
                System.arraycopy(dst, 0, newDst, 0, len);
                mCompressedBytes = dst = newDst;
                dstOff = len;
            }
        } finally {
            mDeflater.reset();
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
        ByteBuffer bb = MemorySegment.ofAddress(dstAddr + dstOff)
            .reinterpret(dstLen).asByteBuffer();

        try {
            mInflater.setInput(src, srcOff, srcLen);
            mInflater.inflate(bb);
        } catch (DataFormatException e) {
            throw Utils.rethrow(e);
        } finally {
            mInflater.reset();
        }
    }

    @Override
    public void close() {
        mDeflater.end();
        mInflater.end();
    }
}
