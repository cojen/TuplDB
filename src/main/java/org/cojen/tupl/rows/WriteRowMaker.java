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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.ref.WeakReference;

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
     * Returns a SwitchCallSite instance suitable for writing rows from evolvable tables.
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
                (lookup, null, "case", RowWriter.class, byte[].class, byte[].class);

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

                var rowLenVar = keyVar.alength().add(valueVar.alength());

                if (schemaVersion > 0) {
                    rowLenVar.inc(schemaVersion < 128 ? -1 : -4);
                }

                if (!keyCodecs[keyCodecs.length - 1].isLast()) {
                    writerVar.invoke("writeRowLength", rowLenVar);
                } else {
                    // No length prefix is written for the last column of the key, but one
                    // should be when key and value are written together into the stream.
                    // Rather than retrofit the encoded column, provide the full key length up
                    // front. Decoding of the last column finishes when the offset reaches the
                    // end of the full key.
                    writerVar.invoke("writeRowAndKeyLength", rowLenVar, keyVar.alength());
                }

                writerVar.invoke("writeBytes", keyVar);

                if (schemaVersion > 0) {
                    writerVar.invoke("writeBytes", valueVar, schemaVersion < 128 ? 1 : 4);
                }
            }

            return mm.finish();
        });
    }
}
