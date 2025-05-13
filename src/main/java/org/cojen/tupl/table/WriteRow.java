/*
 *  Copyright (C) 2024 Cojen.org
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

import java.util.Arrays;
import java.util.BitSet;
import java.util.Map;

import org.cojen.dirmi.Pipe;

import org.cojen.maker.Bootstrap;
import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.codec.ColumnCodec;

/**
 * Writes a resolved row object to a RowWriter. Use WriteRowMaker for writing rows which are
 * binary encoded.
 *
 * @author Brian S. O'Neill
 */
public abstract class WriteRow<R> {
    private static final WeakCache<Class<?>, WriteRow<?>, Object> cCache = new WeakCache<>() {
        @Override
        protected WriteRow<?> newValue(Class<?> rowType, Object unused) {
            return makeWriteRow(rowType);
        }
    };

    /**
     * Returns a new or cached WriteRow instance which works against the given row type.
     */
    @SuppressWarnings("unchecked")
    public static <R> WriteRow<R> find(Class<R> rowType) {
        return (WriteRow<R>) cCache.obtain(rowType, null);
    }

    public abstract void writeRow(RowWriter.ForEncoder<R> writer, R row);

    /**
     * Returns an object suitable for writing resolved row objects. Unset columns aren't
     * written, and dirty columns will be read back clean by the RowReader.
     */
    @SuppressWarnings("unchecked")
    private static <R> WriteRow<R> makeWriteRow(Class<R> rowType) {
        Class<?> rowClass = RowMaker.find(rowType);

        // The generated class needs access to the package-private state fields.
        ClassMaker cm = RowGen.anotherClassMaker
            (WriteRow.class, RowInfo.find(rowType).name, rowClass, "writeRow");
        
        cm.public_().final_().extend(WriteRow.class);

        // Keep a singleton instance, in order for a weakly cached reference to the WriteRow
        // object to stick around until the class is unloaded.
        cm.addField(WriteRow.class, "_").private_().static_();

        MethodMaker mm = cm.addConstructor().private_();
        mm.invokeSuperConstructor();
        mm.field("_").set(mm.this_());

        mm = cm.addMethod(null, "writeRow", RowWriter.ForEncoder.class, Object.class).public_();

        var writerVar = mm.param(0);
        var rowVar = mm.param(1).cast(rowClass);

        RowGen rowGen = RowInfo.find(rowType).rowGen();

        if (rowGen.info.allColumns.isEmpty()) {
            var headerVar = mm.var(byte[].class).setExact(RowHeader.make(rowGen).encode(true));
            writerVar.invoke("writeHeader", headerVar);
            writerVar.invoke("writeRowLength", 0);
        } else {
            Bootstrap indy = mm.var(WriteRow.class).indy("indyWriteRow", rowType, null);
            var stateField = rowVar.field(rowGen.stateFields()[0]);
            indy.invoke(null, "writeRow", null, stateField, writerVar, rowVar);
        }

        MethodHandles.Lookup lookup = cm.finishHidden();

        try {
            Class<?> clazz = lookup.lookupClass();
            MethodHandle ctor = lookup.findConstructor(clazz, MethodType.methodType(void.class));
            return (WriteRow<R>) ctor.invoke();
        } catch (Throwable e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Used by the makeWriteRow method.
     *
     * MethodType is void (int state, RowWriter.ForEncoder writer, Row row)
     *
     * @param prevStates can be null to indicate an empty array
     */
    public static SwitchCallSite indyWriteRow(MethodHandles.Lookup lookup,
                                              String name, MethodType mt,
                                              Class<?> rowType, int[] prevStates)
    {
        return new SwitchCallSite(lookup, mt, state -> {
            Class<?> rowClass = RowMaker.find(rowType);

            MethodMaker mm = MethodMaker.begin
                (lookup, null, "writeRow", RowWriter.ForEncoder.class, rowClass);

            var writerVar = mm.param(0);
            var rowVar = mm.param(1).cast(rowClass);

            // Note that the "plain" RowInfo is used, in order to encode all columns using the
            // simpler "value" format instead of the "key" format.
            RowInfo originalRowInfo = RowInfo.find(rowType);
            RowInfo rowInfo = originalRowInfo.plain();
            RowGen rowGen = rowInfo.rowGen();
            String[] stateFields = rowGen.stateFields();

            int depth = prevStates == null ? 1 : (prevStates.length + 1);

            if (depth < stateFields.length) {
                // The row contains more state fields to be examined, so switch again to
                // capture another.

                int[] seenStates;
                if (prevStates == null || prevStates.length == 0) {
                    seenStates = new int[] {state};
                } else {
                    seenStates = Arrays.copyOfRange(prevStates, 0, prevStates.length + 1);
                    seenStates[seenStates.length - 1] = state;
                }

                Bootstrap indy = mm.var(WriteRow.class).indy("indyWriteRow", rowType, seenStates);
                var stateField = rowVar.field(stateFields[depth]);
                indy.invoke(null, "writeRow", null, stateField, writerVar, rowVar);

                return mm.finish();
            }

            ClassMaker cm = mm.classMaker();

            assert rowInfo.keyColumns.isEmpty();
            ColumnCodec[] codecs = rowGen.valueCodecs();

            // Examine the field states and determine which columns are to be encoded.
            var projSet = new BitSet(codecs.length);
            Map<String, Integer> columnNumbers = originalRowInfo.rowGen().columnNumbers();
            for (int i=0; i<codecs.length; i++) {
                int num = columnNumbers.get(codecs[i].info.name);
                int fieldNum = RowGen.stateFieldNum(num);
                int colState = (fieldNum + 1 == stateFields.length) ? state : prevStates[fieldNum];
                if ((colState & RowGen.stateFieldMask(num)) != 0) {
                    projSet.set(i);
                }
            }

            RowHeader rh = RowHeader.make(rowGen, projSet);
            var headerVar = mm.var(byte[].class).setExact(rh.encode(true));
            writerVar.invoke("writeHeader", headerVar);

            // Bind the codecs which will be encoded.
            codecs = codecs.clone();
            for (int i=0; i<codecs.length; i++) {
                if (projSet.get(i)) {
                    codecs[i] = codecs[i].bind(mm);
                }
            }

            // Determine the minimum encoding size and prepare the encoders.
            int minSize = 0;
            boolean hasPrepared = false;
            for (int i=0; i<codecs.length; i++) {
                if (projSet.get(i)) {
                    ColumnCodec codec = codecs[i];
                    minSize += codec.minSize();
                    hasPrepared |= codec.encodePrepare();
                }
            }

            // Generate code which determines the additional runtime length.
            Variable totalVar = mm.var(int.class).set(minSize);
            for (int i=0; i<codecs.length; i++) {
                if (projSet.get(i)) {
                    ColumnCodec codec = codecs[i];
                    var srcVar = rowVar.field(codec.info.name);
                    totalVar = codec.encodeSize(srcVar, totalVar);
                }
            }

            // The actual encoding is performed by an Encoder which is passed to the
            // Pipe.writeEncode method. This class can implement the Encoder interface directly
            // rather than creating another one.
            cm.implement(Pipe.Encoder.class);
            cm.addConstructor().private_();

            var encoderField = writerVar.field("encoder");

            final Variable encoderVar;

            if (hasPrepared) {
                // Extra fields will need to be defined and filled in before it can be used.
                encoderVar = mm.var(cm);
                var currentEncoderVar = encoderField.get();
                Label validInstance = mm.label();
                currentEncoderVar.instanceOf(cm).ifTrue(validInstance);
                encoderVar.set(mm.new_(cm));
                encoderField.set(encoderVar);
                Label ready = mm.label().goto_();
                validInstance.here();
                encoderVar.set(currentEncoderVar.cast(cm));
                ready.here();
            } else {
                // Use a singleton instance.
                cm.addField(Pipe.Encoder.class, "_").private_().static_().final_();
                MethodMaker clinit = cm.addClinit();
                clinit.field("_").set(clinit.new_(cm));
                encoderVar = mm.field("_");
                encoderField.set(encoderVar);
            }

            {
                MethodMaker encode = cm.addMethod
                    (int.class, "encode", Object.class, int.class, byte[].class, int.class)
                    .public_().override();

                // Rebind the codecs to the encode method.

                var encodeCodecs = codecs.clone();
                var numFields = new int[1];

                for (int i=0; i<codecs.length; i++) {
                    if (projSet.get(i)) {
                        ColumnCodec codec = codecs[i];
                        ColumnCodec encodeCodec = codec.bind(encode);
                        encodeCodecs[i] = encodeCodec;

                        if (!hasPrepared) {
                            // Nothing to transfer.
                            continue;
                        }

                        codec.encodeTransfer(encodeCodec, (Variable from) -> {
                            String fieldName = "f" + (numFields[0]++);
                            cm.addField(from, fieldName).private_();
                            encoderVar.field(fieldName).set(from);
                            return encode.field(fieldName);
                        });
                    }
                }

                // Now perform the encoding.

                var encodeRowVar = encode.param(0).cast(rowClass);
                //var lengthVar = encode.param(1);
                var bufferVar = encode.param(2);
                var offsetVar = encode.param(3);

                for (int i=0; i<encodeCodecs.length; i++) {
                    if (projSet.get(i)) {
                        ColumnCodec codec = encodeCodecs[i];
                        var srcVar = encodeRowVar.field(codec.info.name);
                        codec.encode(srcVar, bufferVar, offsetVar);
                    }
                }

                encode.return_(offsetVar);
            }

            writerVar.invoke("writeRowEncode", rowVar, totalVar);

            return mm.finish();
        });
    }
}
