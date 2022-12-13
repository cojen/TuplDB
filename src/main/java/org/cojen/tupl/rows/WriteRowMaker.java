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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.ref.WeakReference;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.DatabaseException;

/**
 * Makes a call site for implementing the RowEvaluator.writeRow method.
 *
 * @author Brian S O'Neill
 */
public class WriteRowMaker {
    /**
     * Returns a SwitchCallSite instance suitable for writing rows from evolvable tables. All
     * row columns are written.
     *
     * MethodType is void (int schemaVersion, RowWriter writer, byte[] key, byte[] value)
     */
    public static SwitchCallSite indyWriteRow(MethodHandles.Lookup lookup,
                                              String name, MethodType mt,
                                              WeakReference<RowStore> storeRef,
                                              Class<?> rowType, long tableId)
    {
        return new SwitchCallSite(lookup, mt, schemaVersion -> {
            MethodMaker mm = MethodMaker.begin
                (lookup, null, "writeRow", RowWriter.class, byte[].class, byte[].class);

            RowStore store = storeRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
            } else {
                RowInfo rowInfo;
                try {
                    rowInfo = store.rowInfo(rowType, tableId, schemaVersion);
                } catch (Exception e) {
                    return new ExceptionCallSite.Failed
                        (MethodType.methodType
                         (void.class, RowWriter.class, byte[].class, byte[].class), mm, e);
                }

                RowGen rowGen = rowInfo.rowGen();

                var writerVar = mm.param(0);
                var keyVar = mm.param(1);
                var valueVar = mm.param(2);

                var headerVar = mm.var(byte[].class).setExact(RowHeader.make(rowGen).encode());
                writerVar.invoke("writeHeader", headerVar);

                ColumnCodec[] keyCodecs = rowGen.keyCodecs();

                var rowLengthVar = keyVar.alength().add(valueVar.alength());

                if (schemaVersion > 0) {
                    rowLengthVar.inc(schemaVersion < 128 ? -1 : -4);
                }

                if (!keyCodecs[keyCodecs.length - 1].isLast()) {
                    writerVar.invoke("writeRowLength", rowLengthVar);
                } else {
                    // No length prefix is written for the last column of the key, but one
                    // should be when key and value are written together into the stream.
                    // Rather than retrofit the encoded column, provide the full key length up
                    // front. Decoding of the last column finishes when the offset reaches the
                    // end of the full key.
                    writerVar.invoke("writeRowAndKeyLength", rowLengthVar, keyVar.alength());
                }

                writerVar.invoke("writeBytes", keyVar);

                if (schemaVersion > 0) {
                    writerVar.invoke("writeBytes", valueVar, schemaVersion < 128 ? 1 : 4);
                }
            }

            return mm.finish();
        });
    }

    /**
     * Returns a MethodHandle suitable for partially writing rows from evolvable tables. A
     * subset of row columns is written, based on the given projection specification.
     *
     * MethodType is void (int schemaVersion, RowWriter writer, byte[] key, byte[] value)
     *
     * @param projectionSpec must not be null
     */
    public static MethodHandle makeWritePartialHandle(WeakReference<RowStore> storeRef,
                                                      Class<?> rowType, long tableId,
                                                      byte[] projectionSpec)
    {
        // Because no special access is required, the local lookup is sufficient.
        var lookup = MethodHandles.lookup();

        MethodType mt = MethodType.methodType
            (void.class, int.class, RowWriter.class, byte[].class, byte[].class);

        return new SwitchCallSite(lookup, mt, schemaVersion -> {
            MethodMaker mm = MethodMaker.begin
                (lookup, null, "writeRow", RowWriter.class, byte[].class, byte[].class);

            RowStore store = storeRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
                return mm.finish();
            }

            RowInfo rowInfo;
            try {
                rowInfo = store.rowInfo(rowType, tableId, schemaVersion);
            } catch (Exception e) {
                return new ExceptionCallSite.Failed
                    (MethodType.methodType
                     (void.class, RowWriter.class, byte[].class, byte[].class), mm, e);
            }

            RowGen rowGen = rowInfo.rowGen();

            var writerVar = mm.param(0);
            var keyVar = mm.param(1);
            var valueVar = mm.param(2);

            // The header describes only the projected columns.
            BitSet projSet = DecodePartialMaker.decodeSpec(projectionSpec)[0];
            var headerVar = mm.var(byte[].class).setExact(RowHeader.make(rowGen, projSet).encode());
            writerVar.invoke("writeHeader", headerVar);

            ColumnCodec[] keyCodecs = rowGen.keyCodecs();
            ColumnCodec[] valueCodecs = rowGen.valueCodecs();

            Variable keyLengthVar;
            List<Range> keyRanges;

            if (DecodePartialMaker.allRequested(projSet, 0, keyCodecs.length)) {
                // All key columns are projected.
                keyLengthVar = keyVar.alength();
                keyRanges = List.of(new Range(0, keyLengthVar));
            } else {
                keyRanges = prepareRanges(projSet, 0, keyCodecs, keyVar, 0);
                keyLengthVar = mm.var(int.class).set(0);
                incLength(keyLengthVar, keyRanges);
            }

            int valueStart = schemaVersion == 0 ? 0 : (schemaVersion < 128 ? 1 : 4);

            Variable rowLengthVar;
            List<Range> valueRanges;

            if (DecodePartialMaker.allRequested
                (projSet, keyCodecs.length, keyCodecs.length + valueCodecs.length))
            {
                // All value columns are projected.
                Variable valueLengthVar = valueVar.alength().sub(valueStart);
                rowLengthVar = keyLengthVar.add(valueLengthVar);
                valueRanges = List.of(new Range(valueStart, valueLengthVar));
            } else {
                valueRanges = prepareRanges
                    (projSet, keyCodecs.length, valueCodecs, valueVar, valueStart);
                rowLengthVar = keyLengthVar.get();
                incLength(rowLengthVar, valueRanges);
            }

            if (!projSet.get(keyCodecs.length - 1) || !keyCodecs[keyCodecs.length - 1].isLast()) {
                writerVar.invoke("writeRowLength", rowLengthVar);
            } else {
                // No length prefix is written for the last column of the key, but one should
                // be when key and value are written together into the stream. See the comment
                // in the indyWriteRow method.
                writerVar.invoke("writeRowAndKeyLength", rowLengthVar, keyLengthVar);
            }

            writeRanges(writerVar, keyVar, keyRanges);
            writeRanges(writerVar, valueVar, valueRanges);

            return mm.finish();
        }).dynamicInvoker();
    }

    private static List<Range> prepareRanges(BitSet projSet, int projOffset, ColumnCodec[] codecs,
                                             Variable bytesVar, int bytesStart)
    {
        codecs = ColumnCodec.bind(codecs, bytesVar.methodMaker());

        int numColumns = 0;

        for (int i=0; i<codecs.length; i++) {
            if (projSet.get(projOffset + i)) {
                numColumns++;
            }
        }

        var ranges = new ArrayList<Range>();

        MethodMaker mm = bytesVar.methodMaker();
        Variable offsetVar = mm.var(int.class).set(bytesStart);
        boolean gap = true;

        for (int i = 0; numColumns > 0 && i < codecs.length; i++) {
            ColumnCodec codec = codecs[i];

            if (!projSet.get(projOffset + i)) {
                codec.decodeSkip(bytesVar, offsetVar, null);
                gap = true;
                continue;
            }

            Object start = i == 0 ? bytesStart : offsetVar.get();
            Variable length;

            if (i < codecs.length - 1) {
                codec.decodeSkip(bytesVar, offsetVar, null);
                length = offsetVar.sub(start);
            } else {
                length = bytesVar.alength().sub(start);
            }

            if (gap) {
                ranges.add(new Range(start, length));
            } else {
                ranges.get(ranges.size() - 1).length.inc(length);
            }

            numColumns--;
            gap = false;
        }

        return ranges;
    }

    private static void incLength(Variable lengthVar, List<Range> ranges) {
        for (Range range : ranges) {
            lengthVar.inc(range.length);
        }
    }

    private static void writeRanges(Variable writerVar, Variable bytesVar, List<Range> ranges) {
        for (Range range : ranges) {
            writerVar.invoke("writeBytes", bytesVar, range.start, range.length);
        }
    }

    private static class Range {
        Object start; // Variable or constant.
        Variable length;

        Range(Object start, Variable length) {
            this.start = start;
            this.length = length;
        }
    }
}
