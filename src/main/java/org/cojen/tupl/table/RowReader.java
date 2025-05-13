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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.io.IOException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Spliterator;

import org.cojen.dirmi.Pipe;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Scanner;

import org.cojen.tupl.table.codec.ColumnCodec;

/**
 * Used for reading remotely serialized rows.
 *
 * @author Brian S O'Neill
 * @see RowWriter
 */
public class RowReader<R> implements Scanner<R> {
    private static final WeakCache<CacheKey, Decoder, Object> cDecoders;

    static {
        cDecoders = new WeakCache<>() {
            @Override
            @SuppressWarnings("unchecked")
            protected Decoder newValue(CacheKey key, Object unused) {
                return makeDecoder(key.mRowType, key.mHeader);
            }
        };
    }

    private final Class<R> mRowType;
    private Pipe mIn;
    private final long mSize;
    private final int mCharacteristics;

    private Decoder<R> mDecoder;
    private R mRow;

    private byte[] mRowData;

    private Decoder[] mDecoders;
    private int mNumDecoders;

    /**
     * @throws IOException or an unchecked exception from processing the query
     */
    public RowReader(Class<R> rowType, Pipe in, R row) throws IOException {
        mRowType = rowType;
        mIn = in;
        try {
            int characteristics = in.readInt();
            mSize = (characteristics & Spliterator.SIZED) == 0 ? Long.MAX_VALUE : in.readLong();
            mCharacteristics = characteristics;

            doStep(row);
        } catch (Throwable e) {
            throw RowUtils.fail(this, e);
        }
    }

    @Override
    public final long estimateSize() {
        return mSize;
    }

    @Override
    public final int characteristics() {
        return mCharacteristics;
    }

    @Override
    public final R row() {
        return mRow;
    }

    /**
     * @throws IOException or an unchecked exception from processing the query
     */
    @Override
    public final R step(R row) throws IOException {
        try {
            return doStep(row);
        } catch (Throwable e) {
            throw RowUtils.fail(this, e);
        }
    }

    @Override
    public final void close() throws IOException {
        close(false);
    }

    private void close(boolean finished) throws IOException {
        Pipe in = mIn;
        if (in != null) {
            mIn = null;
            mDecoder = null;
            mRow = null;
            mRowData = null;
            mDecoders = null;
            if (finished) {
                in.recycle();
            } else {
                in.close();
            }
        }
    }

    /**
     * @throws IOException or an unchecked exception from processing the query
     */
    @SuppressWarnings("unchecked")
    private R doStep(R row) throws IOException {
        Pipe in = mIn;
        if (in == null) {
            return null;
        }

        // See RowWriter.writeHeader for prefix format.
        int prefix = in.readUnsignedByte();

        if (prefix != 1) {
            switch (prefix) {
            case 0:
                close(true);
                return null;
            case 2:
                var key = new CacheKey(mRowType, RowHeader.readFrom(in));
                var decoder = (Decoder<R>) cDecoders.obtain(key, null);

                if (mDecoder != null) {
                    if (mDecoders == null) {
                        mDecoders = new Decoder[] {mDecoder, decoder};
                        mNumDecoders = 2;
                    } else {
                        int decoderId = mNumDecoders;
                        if (decoderId >= mDecoders.length) {
                            int newLen = Math.min(mDecoders.length << 1, Integer.MAX_VALUE);
                            mDecoders = Arrays.copyOf(mDecoders, newLen);
                        }
                        mDecoders[decoderId] = decoder;
                        mNumDecoders = decoderId + 1;
                    }
                }

                mDecoder = decoder;
                break;
            case 3:
                var e = (Throwable) in.readThrowable();
                try {
                    close(true);
                } catch (Throwable e2) {
                    RowUtils.suppress(e, e2);
                }
                throw RowUtils.rethrow(e);
            default:
                int decoderId = prefix < 255 ? (prefix - 5) : in.readInt();
                if (mDecoders != null) {
                    mDecoder = mDecoders[decoderId];
                } else if (decoderId != 0) {
                    throw new IllegalStateException();
                }
                break;
            }
        }

        return mRow = mDecoder.decodeRow(this, row);
    }

    /**
     * @see RowWriter#writeRowLength
     */
    public final int readRowLength() throws IOException {
        int len = mIn.readShort();
        return len >= 0 ? len : readIntRowLength(len);
    }

    private int readIntRowLength(int len) throws IOException {
        return ((len << 16) | mIn.readUnsignedShort()) & ~(1 << 31);
    }

    /**
     * Reads and returns the requested bytes in an array which might be larger than necessary.
     */
    public final byte[] readRowBytes(int length) throws IOException {
        byte[] rowData = mRowData;
        if (rowData == null || rowData.length < length) {
            mRowData = rowData = new byte[length];
        }
        mIn.readFully(rowData, 0, length);
        return rowData;
    }

    private static final class CacheKey {
        final Class mRowType;
        final byte[] mHeader;

        CacheKey(Class rowType, byte[] header) {
            mRowType = rowType;
            mHeader = header;
        }

        @Override
        public int hashCode() {
            return mRowType.hashCode() ^ RowUtils.decodeIntBE(mHeader, 0);
        }

        @Override
        public boolean equals(Object obj) {
            return this == obj || obj instanceof CacheKey other
                && mRowType == other.mRowType && Arrays.equals(mHeader, other.mHeader);
        }
    }

    public static interface Decoder<R> {
        /**
         * @param row can pass null to construct a new instance
         * @return non-null row
         */
        R decodeRow(RowReader reader, R row) throws IOException;
    }

    @SuppressWarnings("unchecked")
    private static <R> Decoder<R> makeDecoder(Class<R> rowType, byte[] header) {
        RowInfo rowInfo = RowInfo.find(rowType);
        Class<? extends R> rowClass = RowMaker.find(rowType);

        RowHeader rh = RowHeader.decode(header);
        ColumnCodec[] codecs = ColumnCodec.make(rh);

        ClassMaker cm = rowInfo.rowGen().beginClassMaker(RowReader.class, rowType, "reader");
        cm.public_().final_().implement(Decoder.class);

        // Keep a singleton instance, in order for a weakly cached reference to the decoder to
        // stick around until the class is unloaded.
        cm.addField(Decoder.class, "_").private_().static_();

        MethodMaker mm = cm.addConstructor().private_();
        mm.invokeSuperConstructor();
        mm.field("_").set(mm.this_());

        mm = cm.addMethod(Object.class, "decodeRow", RowReader.class, Object.class).public_();

        var rowVar = CodeUtils.castOrNew(mm.param(1), rowClass);

        var readerVar = mm.param(0);
        Variable valueEndVar = readerVar.invoke("readRowLength");
        Variable dataVar = readerVar.invoke("readRowBytes", valueEndVar);

        var offsetVar = mm.var(int.class);
        Variable keyEndVar;

        if (rh.numValues() == 0 || rh.numKeys == 0 || !codecs[rh.numKeys - 1].isLast()) {
            offsetVar.set(0);
            keyEndVar = null;
        } else {
            // Decode the full key length.
            // See WriteRowMaker.makeWriteRow and RowWriter.writeRowAndKeyLength.
            keyEndVar = dataVar.aget(0).cast(int.class).and(0xff);
            Label bigKey = mm.label();
            keyEndVar.ifGe(128, bigKey);
            keyEndVar.inc(1); // +1 to skip the length field
            offsetVar.set(1);
            Label cont = mm.label().goto_();
            bigKey.here();
            var rowUtils = mm.var(RowUtils.class);
            keyEndVar.set(rowUtils.invoke("decodeIntBE", dataVar, 0).and(~(1 << 31)).add(4));
            offsetVar.set(4);
            cont.here();
        }

        codecs = ColumnCodec.bind(codecs, mm);

        var decodedColumns = new HashMap<String, ColumnInfo>();

        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            String name = codec.info.name;
            ColumnInfo colInfo = rowInfo.allColumns.get(name);

            Variable endVar = valueEndVar;

            if (keyEndVar != null && i == rh.numKeys - 1) {
                endVar = keyEndVar;
                keyEndVar = null;
            }

            if (colInfo == null) {
                codec.decodeSkip(dataVar, offsetVar, endVar);
            } else {
                Converter.decodeExact(mm, name, dataVar, offsetVar, endVar,
                                      codec, colInfo, rowVar.field(name));
                decodedColumns.put(name, colInfo);
            }
        }

        TableMaker.markClean(rowVar, rowInfo.rowGen(), decodedColumns);

        mm.return_(rowVar);

        try {
            MethodHandles.Lookup lookup = cm.finishHidden();
            MethodHandle mh = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class));
            return (Decoder<R>) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }
}
