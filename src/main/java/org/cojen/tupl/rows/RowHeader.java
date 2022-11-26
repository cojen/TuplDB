/*
 *  Copyright (C) 2022 Cojen.org
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

package org.cojen.tupl.rows;

import java.io.DataInput;
import java.io.IOException;

import java.util.Arrays;

/**
 * Object which is written when remotely serializing rows.
 *
 * @author Brian S O'Neill
 * @see RowWriter
 */
public final class RowHeader {
    static RowHeader make(RowGen rowGen) {
        return make(rowGen.keyCodecs(), rowGen.valueCodecs());
    }

    static RowHeader make(ColumnCodec[] keyCodecs, ColumnCodec[] valueCodecs) {
        int numColumns = keyCodecs.length + valueCodecs.length;
        var columnNames = new String[numColumns];
        var columnTypes = new int[numColumns];
        var columnFlags = new int[numColumns];

        int num = 0;

        for (ColumnCodec codec : keyCodecs) {
            ColumnInfo info = codec.mInfo;
            columnNames[num] = info.name;
            columnTypes[num] = info.typeCode;
            columnFlags[num] = codec.codecFlags();
            num++;
        }

        for (ColumnCodec codec : valueCodecs) {
            ColumnInfo info = codec.mInfo;
            columnNames[num] = info.name;
            columnTypes[num] = info.typeCode;
            columnFlags[num] = codec.codecFlags();
            num++;
        }

        int hash = keyCodecs.length;
        hash = hash * 31 + Arrays.hashCode(columnNames);
        hash = hash * 31 + Arrays.hashCode(columnTypes);
        hash = hash * 31 + Arrays.hashCode(columnFlags);

        return new RowHeader(hash, keyCodecs.length, columnNames, columnTypes, columnFlags);
    }

    final int numKeys;
    final String[] columnNames;
    final int[] columnTypes;
    final int[] columnFlags;

    private final int mHashCode;

    private RowHeader(int hashCode,
                      int numKeys, String[] columnNames, int[] columnTypes, int[] columnFlags)
    {
        mHashCode = hashCode;
        this.numKeys = numKeys;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.columnFlags = columnFlags;
    }

    public byte[] encode() {
        int numColumns = columnNames.length;

        int length = (4 + 4 + 4 + 4) + numColumns * (2 + 4 + 4);

        for (int i=0; i<numColumns; i++) {
            length += RowUtils.lengthStringUTF(columnNames[i]);
        }

        byte[] bytes = new byte[length];

        RowUtils.encodeIntBE(bytes, 0, length - 4);
        RowUtils.encodeIntBE(bytes, 4, mHashCode);
        RowUtils.encodeIntBE(bytes, 8, numKeys);
        RowUtils.encodeIntBE(bytes, 12, columnNames.length);

        int offset = 16;

        for (int i=0; i<numColumns; i++) {
            int start = offset + 2;
            offset = RowUtils.encodeStringUTF(bytes, start, columnNames[i]);
            int strlen = offset - start;
            if (strlen > 65535) {
                throw new IllegalStateException();
            }
            RowUtils.encodeShortBE(bytes, start - 2, strlen);

            RowUtils.encodeIntBE(bytes, offset, columnTypes[i]);
            offset += 4;

            RowUtils.encodeIntBE(bytes, offset, columnFlags[i]);
            offset += 4;
        }

        return bytes;
    }

    /**
     * @param bytes the original encoded bytes excluding the length field
     */
    public static RowHeader decode(byte[] bytes) {
        int hash = RowUtils.decodeIntBE(bytes, 0);
        int numKeys = RowUtils.decodeIntBE(bytes, 4);
        int numColumns = RowUtils.decodeIntBE(bytes, 8);

        int offset = 12;

        String[] columnNames = new String[numColumns];
        int[] columnTypes = new int[numColumns];
        int[] columnFlags = new int[numColumns];

        for (int i=0; i<numColumns; i++) {
            int strlen = RowUtils.decodeShortBE(bytes, offset);
            offset += 2;
            columnNames[i] = RowUtils.decodeStringUTF(bytes, offset, strlen);
            offset += strlen;

            columnTypes[i] = RowUtils.decodeIntBE(bytes, offset);
            offset += 4;

            columnFlags[i] = RowUtils.decodeIntBE(bytes, offset);
            offset += 4;
        }

        if (offset != bytes.length) {
            throw new IllegalStateException();
        }

        return new RowHeader(hash, numKeys, columnNames, columnTypes, columnFlags);
    }

    /**
     * @return the encoded bytes excluding the length field.
     */
    public static byte[] readFrom(DataInput in) throws IOException {
        int length = in.readInt();
        byte[] header = new byte[length];
        in.readFully(header);
        return header;
    }

    @Override
    public int hashCode() {
        return mHashCode;
    }

    @Override
    public boolean equals(Object obj) {
        return this == obj || obj instanceof RowHeader other
            && numKeys == other.numKeys
            && Arrays.equals(columnNames, other.columnNames)
            && Arrays.equals(columnTypes, other.columnTypes)
            && Arrays.equals(columnFlags, other.columnFlags);
    }
}
