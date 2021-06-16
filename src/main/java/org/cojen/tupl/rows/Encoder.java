/*
 *  Copyright 2021 Cojen.org
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

package org.cojen.tupl.rows;

import java.util.Arrays;

import static org.cojen.tupl.rows.RowUtils.*;

/**
 * Simple encoder into a growable byte array.
 *
 * @author Brian S O'Neill
 */
class Encoder {
    private byte[] mBuffer;
    private int mLength;

    Encoder(int capacity) {
        mBuffer = new byte[capacity];
    }

    public void reset(int amt) {
        mLength = 0;
        ensureCapacity(amt);
        mLength = amt;
    }

    public void writeByte(int v) {
        ensureCapacity(1);
        mBuffer[mLength++] = (byte) v;
    }

    public void writeIntLE(int v) {
        ensureCapacity(4);
        encodeIntLE(mBuffer, mLength, v);
        mLength += 4;
    }

    public void writeLongLE(long v) {
        ensureCapacity(8);
        encodeLongLE(mBuffer, mLength, v);
        mLength += 8;
    }

    public void writePrefixPF(int value) {
        int prefixLen = lengthPrefixPF(value);
        ensureCapacity(prefixLen);
        encodePrefixPF(mBuffer, mLength, value);
        mLength += prefixLen;
    }

    /**
     * Write a non-null string with a length prefix.
     */
    public void writeStringUTF(String str) {
        int prefixLen = lengthPrefixPF(str.length());
        int strLen = lengthStringUTF(str);
        ensureCapacity(prefixLen + strLen);
        encodePrefixPF(mBuffer, mLength, str.length());
        encodeStringUTF(mBuffer, mLength + prefixLen, str);
        mLength += prefixLen + strLen;
    }

    public byte[] toByteArray() {
        return Arrays.copyOf(mBuffer, mLength);
    }

    private void ensureCapacity(int amt) {
        if (mLength + amt > mBuffer.length) {
            mBuffer = Arrays.copyOf(mBuffer, Math.max(mLength + amt, mLength << 1));
        }
    }
}
