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

import java.io.DataOutput;
import java.io.IOException;

import java.util.HashMap;

import org.cojen.tupl.Scanner;

/**
 * Defines a context for remotely serializing rows.
 *
 * @author Brian S O'Neill
 */
public final class RowWriter<R> implements RowConsumer<R> {
    // FIXME:  RowSorter needs to call beginBatch.

    private final DataOutput mOut;

    // The active header is initially null, which implies a row header with no columns.
    private byte[] mActiveHeader;

    // Note: Header keys can rely on identity comparison because encoded headers are typically
    // cached by generated code. Full canonicalization would help further, but it's not
    // necessary. In the worst case, a duplicate header can be written, but this isn't harmful.
    private HashMap<byte[], Integer> mAllHeaders;

    private RowEvaluator mEvaluator;

    RowWriter(DataOutput out) {
        mOut = out;
    }

    @Override
    public void beginBatch(Scanner scanner, RowEvaluator<R> evaluator) throws IOException {
        if (mEvaluator == null) {
            mOut.writeLong(scanner.estimateSize());
            mOut.writeInt(scanner.characteristics());
        }
        mEvaluator = evaluator;
    }

    @Override
    public void accept(byte[] key, byte[] value) throws IOException {
        mEvaluator.writeRow(this, key, value);
    }

    /**
     * Header prefix byte:
     *
     *      0: scan terminator
     *      1: new header (is followed by a RowHeader; first id is 0 and goes up by one each time)
     *      2: same header as before
     * 3..254: refer to an existing header (id is 0..251)
     *    255: refer to an existing header (is followed by an int id)
     *
     * @see RowHeader
     */
    public void writeHeader(byte[] header) throws IOException {
        if (header == mActiveHeader) {
            // same header as before
            mOut.writeByte(2);
        } else {
            doWriteHeader(header);
        }
    }

    private void doWriteHeader(byte[] header) throws IOException {
        doWrite: {
            if (mActiveHeader != null) {
                if (mAllHeaders == null) {
                    mAllHeaders = new HashMap<>();
                    mAllHeaders.put(mActiveHeader, 0);
                } else {
                    int newId = mAllHeaders.size();
                    Integer existingId = mAllHeaders.putIfAbsent(header, newId);
                    if (existingId != null) {
                        // refer to an existing header
                        int id = existingId;
                        if (id <= 251) {
                            mOut.writeByte(id + 3);
                        } else {
                            mOut.writeByte(255);
                            mOut.writeInt(id);
                        }
                        break doWrite;
                    }
                }
            }

            // new header
            mOut.writeByte(1);
            mOut.write(header);
        }

        mActiveHeader = header;
    }

    public void writeRowLength(int length) throws IOException {
        if (length <= 32767) {
            mOut.writeShort(length);
        } else {
            mOut.writeInt(length | (1 << 31));
        }
    }

    /**
     * @param keyLength must not be zero
     */
    public void writeRowAndKeyLength(int rowLength, int keyLength) throws IOException {
        if (keyLength <= 128) {
            writeRowLength(rowLength + 1);
            mOut.writeByte(keyLength - 1);
        } else {
            writeRowLength(rowLength + 4);
            mOut.writeInt(keyLength | (1 << 31));
        }
    }

    public void writeBytes(byte[] bytes) throws IOException {
        mOut.write(bytes);
    }

    public void writeBytes(byte[] bytes, int offset) throws IOException {
        mOut.write(bytes, offset, bytes.length - offset);
    }
}
