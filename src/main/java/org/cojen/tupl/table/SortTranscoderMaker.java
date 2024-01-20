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

import java.lang.ref.WeakReference;

import org.cojen.tupl.DatabaseException;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;

/**
 * Makes totally ordered Transcoders suitable for feeding entries into a Sorter.
 *
 * @author Brian S O'Neill
 */
public class SortTranscoderMaker {
    /**
     * @param rowInfo source info; can be a primary RowInfo or a SecondaryInfo
     * @param tableId source index; ignored when 0 or source is a secondary index
     * @param sortedInfo see SortDecoderMaker.findSortedInfo
     */
    static Transcoder makeTranscoder(RowStore rs, Class<?> rowType, RowInfo rowInfo,
                                     long tableId, SecondaryInfo sortedInfo)
    {
        ClassMaker cm = rowInfo.rowGen().beginClassMaker
            (SortTranscoderMaker.class, rowType, null).implement(Transcoder.class).final_();

        // Keep a singleton instance, in order for a weakly cached reference to the Transcoder
        // to stick around until the class is unloaded.
        cm.addField(Transcoder.class, "_").private_().static_();

        {
            MethodMaker mm = cm.addConstructor().private_();
            mm.invokeSuperConstructor();
            mm.field("_").set(mm.this_());
        }

        MethodMaker mm = cm.addMethod
            (null, "transcode", byte[].class, byte[].class, byte[][].class, int.class);
        mm.public_();

        if (tableId == 0 || rowInfo instanceof SecondaryInfo) {
            // Unevolvable values don't support multiple schemas, so no need to use indy.
            transcode(mm, rowInfo, sortedInfo, 0);
        } else {
            var keyVar = mm.param(0);
            var valueVar = mm.param(1);
            var kvPairsVar = mm.param(2);
            var offsetVar = mm.param(3);

            // A secondary index descriptor is suitable for reconstructing the target info.
            byte[] targetDesc = RowStore.secondaryDescriptor(sortedInfo, false);

            var indy = mm.var(SortTranscoderMaker.class).indy
                ("indyTranscode", rs.ref(), rowType, tableId, targetDesc);

            var schemaVersion = mm.var(RowUtils.class).invoke("decodeSchemaVersion", valueVar);

            indy.invoke(null, "transcode", null, schemaVersion,
                        keyVar, valueVar, kvPairsVar, offsetVar);
        }

        try {
            MethodHandles.Lookup lookup = cm.finishHidden();
            MethodHandle mh = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class));
            return (Transcoder) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    /**
     * @param sourceOffset start offset of the source binary value; pass -1 to auto skip schema
     * version
     */
    private static <R> void transcode(MethodMaker mm,
                                      RowInfo info, RowInfo targetInfo, int sourceOffset)
    {
        var tm = new TransformMaker<R>(null, info, null);

        tm.addKeyTarget(targetInfo, 0, true);

        if (!targetInfo.valueColumns.isEmpty()) {
            tm.addValueTarget(targetInfo, 0, true);
        }

        var keyVar = mm.param(0);
        var valueVar = mm.param(1);
        var kvPairsVar = mm.param(2);
        var offsetVar = mm.param(3);

        tm.begin(mm, null, keyVar, valueVar, sourceOffset);

        kvPairsVar.aset(offsetVar, tm.encode(0));
        offsetVar.inc(1);

        if (!targetInfo.valueColumns.isEmpty()) {
            kvPairsVar.aset(offsetVar, tm.encode(1));
        } else {
            kvPairsVar.aset(offsetVar, mm.var(RowUtils.class).field("EMPTY_BYTES"));
        }
    }

    /**
     * Bootstrap method for the transcode method when a schema version must be decoded.
     *
     * MethodType is:
     *
     *     void (int schemaVersion,
     *           byte[] srcKey, byte[] srcValue, byte[][] kvPairsVar, int offset)
     */
    public static SwitchCallSite indyTranscode
        (MethodHandles.Lookup lookup, String name, MethodType mt,
         WeakReference<RowStore> storeRef, Class<?> rowType, long indexId, byte[] targetDesc)
    {
        return new SwitchCallSite(lookup, mt, schemaVersion -> {
            // Drop the schemaVersion parameter.
            var mtx = mt.dropParameterTypes(0, 1);

            RowStore store = storeRef.get();
            if (store == null) {
                var mm = MethodMaker.begin(lookup, "transcode", mtx);
                mm.new_(DatabaseException.class, "Closed").throw_();
                return mm.finish();
            }

            RowInfo sourceInfo;
            try {
                sourceInfo = store.rowInfo(rowType, indexId, schemaVersion);
            } catch (Exception e) {
                var mm = MethodMaker.begin(lookup, "transcode", mtx);
                return new ExceptionCallSite.Failed(mtx, mm, e);
            }

            MethodMaker mm = MethodMaker.begin(lookup, "transcode", mtx);

            RowInfo targetInfo = RowStore.secondaryRowInfo(sourceInfo, targetDesc);
            int sourceOffset = RowUtils.lengthPrefixPF(schemaVersion);

            transcode(mm, sourceInfo, targetInfo, sourceOffset);

            return mm.finish();
        });
    }
}
