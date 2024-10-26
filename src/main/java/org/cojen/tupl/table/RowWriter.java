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

package org.cojen.tupl.table;

import java.io.IOException;

import java.util.HashMap;

import java.util.Spliterator;

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.Scanner;

/**
 * Defines a context for remotely serializing rows. Note that RowConsumer is implemented, which
 * provides the means for a Scanner to write rows into a RowWriter. The RowWriter is passed to
 * the Scanner as if it's a row.
 *
 * @author Brian S O'Neill
 * @see RowReader
 * @see WriteRowMaker
 */
public sealed class RowWriter<R> implements RowConsumer<R> {
    protected final Pipe mOut;

    // The active header is initially null, which implies a row header with no columns.
    private byte[] mActiveHeader;

    // Note: Header keys can rely on identity comparison because encoded headers are typically
    // cached by generated code. Full canonicalization would help further, but it's not
    // necessary. In the worst case, a duplicate header can be written, but this isn't harmful.
    private HashMap<byte[], Integer> mAllHeaders;

    private RowEvaluator mEvaluator;

    private boolean mWrittenCharacteristics;

    RowWriter(Pipe out) {
        mOut = out;
    }

    /**
     * Must be called by QueryLaunchers which don't issue batches.
     *
     * @param characteristics Spliterator characteristics
     * @param size only applicable when SIZED characteristic is set
     * @throws IllegalStateException if characteristics have already been written
     */
    public final void writeCharacteristics(int characteristics, long size) throws IOException {
        if (mWrittenCharacteristics) {
            throw new IllegalStateException();
        }
        mOut.writeInt(characteristics);
        if ((characteristics & Spliterator.SIZED) != 0) {
            mOut.writeLong(size);
        }
        mWrittenCharacteristics = true;
    }

    @Override
    public final void beginBatch(Scanner scanner, RowEvaluator<R> evaluator) throws IOException {
        mEvaluator = evaluator;

        if (!mWrittenCharacteristics) {
            int characteristics = scanner.characteristics();
            mOut.writeInt(characteristics);
            if ((characteristics & Spliterator.SIZED) != 0) {
                mOut.writeLong(scanner.estimateSize());
            }
            mWrittenCharacteristics = true;
        }
    }

    @Override
    public final void accept(byte[] key, byte[] value) throws IOException {
        mEvaluator.writeRow(this, key, value);
    }

    /**
     * Writes a prefix possibly followed by the header. Each prefix begins with a format byte:
     *
     *      0: scan terminator
     *      1: same header as before
     *      2: new header (is followed by a RowHeader; first id is 0 and goes up by one each time)
     *      3: exception terminator (is followed by a serialized Throwable)
     *      4: reserved
     * 5..254: refer to an existing header (id is 0..249)
     *    255: refer to an existing header (is followed by an int id)
     *
     * Only formats 1, 2, and 5+ are actually written by this method. The writeTerminator
     * method writes format 0, and writeTerminalException writes format 3.
     *
     * @param header must be constant since it will be used as an identity cache key
     * @see RowHeader
     */
    public final void writeHeader(byte[] header) throws IOException {
        if (header == mActiveHeader) {
            // same header as before
            mOut.writeByte(1);
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
                        if (id <= 249) {
                            mOut.writeByte(id + 5);
                        } else {
                            mOut.writeByte(255);
                            mOut.writeInt(id);
                        }
                        break doWrite;
                    }
                }
            }

            // new header
            mOut.writeByte(2);
            mOut.write(header);
        }

        mActiveHeader = header;
    }

    public final void writeTerminator() throws IOException {
        if (!mWrittenCharacteristics) {
            writeCharacteristics(Spliterator.SIZED, 0);
        }
        mOut.writeByte(0);
    }

    /**
     * Should be called instead of writing a header, and nothing more can be written.
     */
    public final void writeTerminalException(Throwable e) throws IOException {
        if (!mWrittenCharacteristics) {
            writeCharacteristics(Spliterator.SIZED, 0);
        }
        mOut.writeByte(3);
        mOut.writeObject(e);
    }

    public final void writeRowLength(int length) throws IOException {
        if (length <= 32767) {
            mOut.writeShort(length);
        } else {
            mOut.writeInt(length | (1 << 31));
        }
    }

    public final void writeRowAndKeyLength(int rowLength, int keyLength) throws IOException {
        if (keyLength <= 127) {
            writeRowLength(rowLength + 1);
            mOut.writeByte(keyLength);
        } else {
            writeRowLength(rowLength + 4);
            mOut.writeInt(keyLength | (1 << 31));
        }
    }

    public final void writeBytes(byte[] bytes) throws IOException {
        mOut.write(bytes);
    }

    public final void writeBytes(byte[] bytes, int offset) throws IOException {
        mOut.write(bytes, offset, bytes.length - offset);
    }

    public final void writeBytes(byte[] bytes, int offset, int length) throws IOException {
        mOut.write(bytes, offset, length);
    }

    /**
     * A special subclass which allows the {@link WriteRow} implementation to stash a generated
     * encoder instance. As such, ForEncoder instances cannot be shared by multiple threads.
     */
    public static final class ForEncoder<R> extends RowWriter<R> {
        public Pipe.Encoder<R> encoder;

        ForEncoder(Pipe out) {
            super(out);
        }

        /**
         * Note: The encoder field must be set.
         */
        public void writeRowEncode(R row, int length) throws IOException {
            writeRowLength(length);
            mOut.writeEncode(row, length, encoder);
        }
    }
}
