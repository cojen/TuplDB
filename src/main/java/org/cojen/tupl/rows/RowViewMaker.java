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

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.MutableCallSite;

import java.lang.ref.WeakReference;

import java.util.Collection;
import java.util.Map;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.CorruptDatabaseException;
import org.cojen.tupl.Cursor;
import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.RowScanner;
import org.cojen.tupl.RowUpdater;
import org.cojen.tupl.RowView;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.Updater;
import org.cojen.tupl.View;

/**
 * Makes RowView classes that extend AbstractRowView. RowIndex methods are supported when
 * constructed with a View source which is an Index.
 *
 * @author Brian S O'Neill
 */
class RowViewMaker {
    private final RowStore mStore;
    private final WeakReference<RowStore> mStoreRef;
    private final Class<?> mRowType;
    private final RowGen mRowGen;
    private final RowInfo mRowInfo;
    private final Class<?> mRowClass;
    private final ClassMaker mClassMaker;

    /**
     * @param store generated class is pinned to this specific instance
     */
    RowViewMaker(RowStore store, Class<?> type, RowGen gen) {
        mStore = store;
        mStoreRef = new WeakReference<RowStore>(mStore);
        mRowType = type;
        mRowGen = gen;
        mRowInfo = gen.info;
        mRowClass = RowMaker.find(type);
        mClassMaker = gen.beginClassMaker("View").extend(AbstractRowView.class).final_();
    }

    @SuppressWarnings("unchecked")
    public Class<? extends AbstractRowView> finish() {
        {
            MethodMaker mm = mClassMaker.addConstructor(View.class).public_();
            mm.invokeSuperConstructor(mm.param(0));
        }

        // Add the simple rowType method.
        mClassMaker.addMethod(Class.class, "rowType").public_().return_(mRowType);

        // Add the newView(View source) method (defined by AbstractRowView).
        {
            MethodMaker mm = mClassMaker.addMethod
                (RowView.class, "newView", View.class).protected_();
            mm.return_(mm.new_(mClassMaker, mm.param(0)));
        }

        // Add the newRow method and its bridge.
        {
            MethodMaker mm = mClassMaker.addMethod(mRowType, "newRow").public_();
            mm.return_(mm.new_(mRowClass));
            mm = mClassMaker.addMethod(Object.class, "newRow").public_().bridge();
            mm.return_(mm.this_().invoke(mRowType, "newRow", null));
        }

        // Add the reset method.
        addResetMethod();

        // Add private methods which check that required columns are set.
        {
            addCheckSet("checkPrimaryKeySet", mRowInfo.keyColumns);

            //addCheckSet("checkValues", mRowInfo.valueColumns);

            addCheckSet("checkAllSet", mRowInfo.allColumns);
            addRequireSet("requireAllSet", mRowInfo.allColumns);

            int i = 0;
            for (ColumnSet altKey : mRowInfo.alternateKeys) {
                addCheckSet("checkAltKeySet$" + i, altKey.keyColumns);
                i++;
            }

            addCheckDirty("checkValuesDirty", mRowInfo.valueColumns);
        }

        // Add encode/decode methods.

        ColumnCodec[] keyCodecs = mRowGen.keyCodecs();
        addEncodeColumns("encodePrimaryKey", keyCodecs);
        addDecodeColumns("decodePrimaryKey", keyCodecs);

        addDynamicEncodeValueColumns();
        addDynamicDecodeValueColumns();

        // Add the public load/store methods, etc.

        addByKeyMethod("load");
        addByKeyMethod("exists");
        addByKeyMethod("delete");

        addStoreMethod("store", null);
        addStoreMethod("exchange", mRowType);
        addStoreMethod("insert", boolean.class);
        addStoreMethod("replace", boolean.class);

        addDoUpdateMethod();
        addUpdateMethod("update", false);
        addUpdateMethod("merge", true);

        // FIXME: define update, merge, and remove methods that accept a match row

        // Add scanner and updater support.
        addScannerMethod(RowScanner.class);
        addScannerMethod(RowUpdater.class);

        return (Class) mClassMaker.finish();
    }

    /**
     * Defines a static method which accepts a row and returns boolean. When it returns true,
     * all of the given columns are set.
     *
     * @param name method name
     */
    private void addCheckSet(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod
            (boolean.class, name, mRowClass).static_().private_();

        if (columns.isEmpty()) {
            mm.return_(true);
            return;
        }

        if (columns.size() == 1) {
            int num = mRowGen.columnNumbers().get(columns.values().iterator().next().name);
            Label cont = mm.label();
            stateField(mm.param(0), num).and(RowGen.stateFieldMask(num)).ifNe(0, cont);
            mm.return_(false);
            cont.here();
            mm.return_(true);
            return;
        }

        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            var baseColumns = step == 0 ? mRowInfo.keyColumns : mRowInfo.valueColumns;

            for (ColumnInfo info : baseColumns.values()) {
                if (columns.containsKey(info.name)) {
                    mask |= RowGen.stateFieldMask(num);
                }

                if (isMaskReady(++num, mask)) {
                    // Convert all states of value 0b01 (clean) into value 0b11 (dirty). All
                    // other states stay the same.
                    var state = stateField(mm.param(0), num - 1).get();
                    state = state.or(state.and(0x5555_5555).shl(1));

                    // Flip all column state bits. If final result is non-zero, then some
                    // columns were unset.
                    state = state.xor(mask);
                    mask = maskRemainder(num, mask);
                    if (mask != 0xffff_ffff) {
                        state = state.and(mask);
                    }

                    Label cont = mm.label();
                    state.ifEq(0, cont);
                    mm.return_(false);
                    cont.here();
                    mask = 0;
                }
            }
        }

        mm.return_(true);
    }

    /**
     * Defines a static method which accepts a row and returns boolean. When it returns true,
     * all of the given columns are dirty.
     *
     * @param name method name
     */
    private void addCheckDirty(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod
            (boolean.class, name, mRowClass).static_().private_();

        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            var baseColumns = step == 0 ? mRowInfo.keyColumns : mRowInfo.valueColumns;

            for (ColumnInfo info : baseColumns.values()) {
                if (columns.containsKey(info.name)) {
                    mask |= RowGen.stateFieldMask(num);
                }

                if (isMaskReady(++num, mask)) {
                    Label cont = mm.label();
                    stateField(mm.param(0), num - 1).and(mask).ifEq(mask, cont);
                    mm.return_(false);
                    cont.here();
                    mask = 0;
                }
            }
        }

        mm.return_(true);
    }

    /**
     * Called when building state field masks for columns, when iterating them in order.
     *
     * @param num column number pre-incremented to the next one
     * @param mask current group; must be non-zero to have any effect
     */
    private boolean isMaskReady(int num, int mask) {
        return mask != 0 && ((num & 0b1111) == 0 || num >= mRowInfo.allColumns.size());
    }

    /**
     * When building a mask for the highest state field, sets the high unused bits on the
     * mask. This can eliminate an unnecessary 'and' operation.
     *
     * @param num column number pre-incremented to the next one
     * @param mask current group
     * @return updated mask
     */
    private int maskRemainder(int num, int mask) {
        if (num >= mRowInfo.allColumns.size()) {
            int shift = (num & 0b1111) << 1;
            if (shift < 32) {
                mask |= 0xffff_ffff << shift;
            }
        }
        return mask;
    }

    /**
     * Defines a static method which accepts a row and always throws a detailed exception
     * describing the required columns which aren't set. A check method should have been
     * invoked first.
     *
     * @param name method name
     */
    private void addRequireSet(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod(null, name, mRowClass).static_().private_();

        String initMessage = "Some required columns are unset";

        if (columns.isEmpty()) {
            mm.new_(IllegalStateException.class, initMessage).throw_();
            return;
        }

        int initLength = initMessage.length() + 2;
        var bob = mm.new_(StringBuilder.class, initLength << 1)
            .invoke("append", initMessage).invoke("append", ": ");

        boolean first = true;
        for (ColumnInfo info : columns.values()) {
            int num = mRowGen.columnNumbers().get(info.name);
            Label isSet = mm.label();
            stateField(mm.param(0), num).and(RowGen.stateFieldMask(num)).ifNe(0, isSet);
            if (!first) {
                Label sep = mm.label();
                bob.invoke("length").ifEq(initLength, sep);
                bob.invoke("append", ", ");
                sep.here();
            }
            bob.invoke("append", info.name);
            isSet.here();
            first = false;
        }

        mm.new_(IllegalStateException.class, bob.invoke("toString")).throw_();
    }

    /**
     * @return null if no field is defined for the column (probably SchemaVersionColumnCodec)
     */
    private static Field findField(Variable row, ColumnCodec codec) {
        ColumnInfo info = codec.mInfo;
        return info == null ? null : row.field(info.name);
    }

    /**
     * Defines a static method which returns a new composite byte[] key or value. Caller must
     * check that the columns are set.
     *
     * @param name method name
     */
    private void addEncodeColumns(String name, ColumnCodec[] codecs) {
        MethodMaker mm = mClassMaker.addMethod(byte[].class, name, mRowClass).static_().private_();
        addEncodeColumns(mm, ColumnCodec.bind(codecs, mm));
    }

    /**
     * @param mm param(0): Row object, return: byte[]
     * @param codecs must be bound to the MethodMaker
     */
    private static void addEncodeColumns(MethodMaker mm, ColumnCodec[] codecs) {
        if (codecs.length == 0) {
            mm.return_(mm.var(RowUtils.class).field("EMPTY_BYTES"));
            return;
        }

        // Determine the minimum byte array size and prepare the encoders.
        int minSize = 0;
        for (ColumnCodec codec : codecs) {
            minSize += codec.minSize();
            codec.encodePrepare();
        }

        // Generate code which determines the additional runtime length.
        Variable totalVar = null;
        for (ColumnCodec codec : codecs) {
            Field srcVar = findField(mm.param(0), codec);
            totalVar = codec.encodeSize(srcVar, totalVar);
        }

        // Generate code which allocates the destination byte array.
        Variable dstVar;
        if (totalVar == null) {
            dstVar = mm.new_(byte[].class, minSize);
        } else {
            if (minSize != 0) {
                totalVar.inc(minSize);
            }
            dstVar = mm.new_(byte[].class, totalVar);
        }

        // Generate code which fills in the byte array.
        Variable offsetVar = null;
        int fixedOffset = 0;
        for (ColumnCodec codec : codecs) {
            Field srcVar = findField(mm.param(0), codec);
            offsetVar = codec.encode(srcVar, dstVar, offsetVar, fixedOffset);
            if (offsetVar == null) {
                fixedOffset += codec.minSize();
            }
        }

        mm.return_(dstVar);
    }

    /**
     * Method isn't implemented until needed, delaying acquisition/creation of the current
     * schema version. This allows replicas to decode existing rows even when the class
     * definition has changed, but encoding will still fail.
     */
    private void addDynamicEncodeValueColumns() {
        MethodMaker mm = mClassMaker.addMethod
            (byte[].class, "encodeValues", mRowClass).static_().private_();
        var indy = mm.var(RowViewMaker.class).indy("indyEncodeValueColumns", mRowType, mStoreRef);
        mm.return_(indy.invoke(byte[].class, "_", null, mm.param(0)));
    }

    static CallSite indyEncodeValueColumns(MethodHandles.Lookup lookup, String name, MethodType mt,
                                           Class<?> rowType, WeakReference<RowStore> storeRef)
    {
        return indyEncode(lookup, name, mt, rowType, storeRef, (mm, info, schemaVersion) -> {
            ColumnCodec[] codecs = info.rowGen().valueCodecs();
            addEncodeColumns(mm, ColumnCodec.bind(schemaVersion, codecs, mm));
        });
    }

    @FunctionalInterface
    static interface EncodeFinisher {
        void finish(MethodMaker mm, RowInfo info, int schemaVersion);
    }

    /**
     * Does the work to obtain the current schema version, handling any exceptions. The given
     * finisher completes the definition of the encode method when no exception was thrown when
     * trying to obtain the schema version. If an exception was thrown, the finisher might be
     * called at a later time.
     */
    private static CallSite indyEncode(MethodHandles.Lookup lookup, String name, MethodType mt,
                                       Class<?> rowType, WeakReference<RowStore> storeRef,
                                       EncodeFinisher finisher)
    {
        return ExceptionCallSite.make(() -> {
            MethodMaker mm = MethodMaker.begin(lookup, name, mt);
            RowStore store = storeRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
            } else {
                RowInfo info = RowInfo.find(rowType);
                int schemaVersion;
                try {
                    schemaVersion = store.schemaVersion(info);
                } catch (Exception e) {
                    return new ExceptionCallSite.Failed(mt, mm, e);
                }
                finisher.finish(mm, info, schemaVersion);
            }
            return mm.finish();
        });
    }

    /**
     * Defines a static method which decodes columns from a composite byte[] parameter.
     *
     * @param name method name
     */
    private void addDecodeColumns(String name, ColumnCodec[] codecs) {
        MethodMaker mm = mClassMaker.addMethod(null, name, mRowClass, byte[].class).static_();
        addDecodeColumns(mm, mRowInfo, codecs, 0);
    }

    /**
     * @param mm param(0): Row object, param(1): byte[], return: void
     * @param fixedOffset must be after the schema version (when applicable)
     */
    private static void addDecodeColumns(MethodMaker mm, RowInfo dstRowInfo,
                                         ColumnCodec[] srcCodecs, int fixedOffset)
    {
        srcCodecs = ColumnCodec.bind(srcCodecs, mm);

        Variable srcVar = mm.param(1);
        Variable offsetVar = null;

        for (ColumnCodec srcCodec : srcCodecs) {
            String name = srcCodec.mInfo.name;
            ColumnInfo dstInfo = dstRowInfo.allColumns.get(name);

            if (dstInfo == null) {
                offsetVar = srcCodec.decodeSkip(srcVar, offsetVar, fixedOffset, null);
            } else {
                var rowVar = mm.param(0);
                Field dstVar = rowVar.field(name);
                if (dstInfo.type.isAssignableFrom(srcCodec.mInfo.type)) {
                    offsetVar = srcCodec.decode(dstVar, srcVar, offsetVar, fixedOffset, null);
                } else {
                    // Decode into a temp variable and then perform a best-effort conversion.
                    var tempVar = mm.var(srcCodec.mInfo.type);
                    offsetVar = srcCodec.decode(tempVar, srcVar, offsetVar, fixedOffset, null);
                    Converter.convert(mm, srcCodec.mInfo, tempVar, dstInfo, dstVar);
                }
            }

            if (offsetVar == null) {
                fixedOffset += srcCodec.minSize();
            }
        }
    }

    private void addDynamicDecodeValueColumns() {
        MethodMaker mm = mClassMaker.addMethod
            (null, "decodeValues", mRowClass, byte[].class).static_();

        var data = mm.param(1);
        var schemaVersion = decodeSchemaVersion(mm, data);

        var indy = mm.var(RowViewMaker.class).indy("indyDecodeValueColumns", mRowType, mStoreRef);
        indy.invoke(null, "_", null, schemaVersion, mm.param(0), data);
    }

    static CallSite indyDecodeValueColumns(MethodHandles.Lookup lookup, String name, MethodType mt,
                                           Class<?> rowType, WeakReference<RowStore> storeRef)
    {
        Class<?> rowClass = mt.parameterType(1);

        return new SwitchCallSite(lookup, mt, schemaVersion -> {
            MethodMaker mm = MethodMaker.begin(lookup, null, "_", rowClass, byte[].class);

            RowStore store = storeRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
            } else {
                RowInfo dstRowInfo = RowInfo.find(rowType);

                RowInfo srcRowInfo;
                try {
                    srcRowInfo = store.rowInfo(rowType, schemaVersion);
                    if (srcRowInfo == null) {
                        throw new CorruptDatabaseException
                            ("Schema version not found: " + schemaVersion);
                    }
                } catch (Exception e) {
                    return new ExceptionCallSite.Failed
                        (MethodType.methodType(void.class, rowClass, byte[].class), mm, e);
                }

                ColumnCodec[] srcCodecs = srcRowInfo.rowGen().valueCodecs();
                int fixedOffset = schemaVersion < 128 ? 1 : 4;

                addDecodeColumns(mm, dstRowInfo, srcCodecs, fixedOffset);

                if (dstRowInfo != srcRowInfo) {
                    // Assign defaults for any missing columns.
                    for (Map.Entry<String, ColumnInfo> e : dstRowInfo.valueColumns.entrySet()) {
                        String fieldName = e.getKey();
                        if (!srcRowInfo.valueColumns.containsKey(fieldName)) {
                            Converter.setDefault(e.getValue(), mm.param(0).field(fieldName));
                        }
                    }
                }

                markAllClean(mm.param(0), dstRowInfo);
            }

            return mm.finish();
        });
    }

    /**
     * Decodes the first 1 to 4 bytes of the given byte array into a schema version int variable.
     */
    private static Variable decodeSchemaVersion(MethodMaker mm, Variable bytes) {
        var schemaVersion = mm.var(int.class);
        schemaVersion.set(bytes.aget(0));
        Label cont = mm.label();
        schemaVersion.ifGe(0, cont);
        schemaVersion.set(mm.var(RowUtils.class).invoke("decodeIntBE", bytes, 0).and(~(1 << 31)));
        cont.here();
        return schemaVersion;
    }

    /**
     * @param variant "load", "exists", or "delete"
     */
    private void addByKeyMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (boolean.class, variant, Transaction.class, Object.class).public_();

        Variable txnVar = mm.param(0);
        Variable rowVar = mm.param(1).cast(mRowClass);

        Label ready = mm.label();
        mm.invoke("checkPrimaryKeySet", rowVar).ifTrue(ready);

        if (mRowInfo.alternateKeys.isEmpty()) {
            mm.new_(IllegalStateException.class, "Primary key isn't fully specified").throw_();
        } else {
            // FIXME: check alternate keys too, and load using a join
            mm.new_(IllegalStateException.class,
                    "Primary or alternate key isn't fully specified").throw_();
        }

        ready.here();

        var keyVar = mm.invoke("encodePrimaryKey", rowVar);
        var valueVar = mm.field("mSource").invoke(variant, txnVar, keyVar);

        if (variant != "load") {
            mm.return_(valueVar);
        } else {
            Label notNull = mm.label();
            valueVar.ifNe(null, notNull);
            resetValueColumns(rowVar);
            mm.return_(false);
            notNull.here();
            mm.invoke("decodeValues", rowVar, valueVar);
            mm.return_(true);
        }
    }

    /**
     * @param variant "store", "exchange", "insert", or "replace"
     */
    private void addStoreMethod(String variant, Class returnType) {
        MethodMaker mm = mClassMaker.addMethod
            (returnType, variant, Transaction.class, Object.class).public_();

        Variable txnVar = mm.param(0);
        Variable rowVar = mm.param(1).cast(mRowClass);

        Label ready = mm.label();
        mm.invoke("checkAllSet", rowVar).ifTrue(ready);
        mm.invoke("requireAllSet", rowVar);
        ready.here();

        var keyVar = mm.invoke("encodePrimaryKey", rowVar);
        var valueVar = mm.invoke("encodeValues", rowVar);

        Variable resultVar = mm.field("mSource").invoke(variant, txnVar, keyVar, valueVar);

        if (returnType == null) {
            // This case is expected only for the "store" variant.
            markAllClean(rowVar);
            return;
        }

        if (variant != "exchange") {
            // This case is expected for the "insert" and "replace" variants.
            Label failed = mm.label();
            resultVar.ifFalse(failed);
            markAllClean(rowVar);
            failed.here();
            mm.return_(resultVar);
            return;
        }

        // The rest is for implementing the "exchange" variant.

        markAllClean(rowVar);
        Label found = mm.label();
        resultVar.ifNe(null, found);
        mm.return_(null);
        found.here();

        var copyVar = mm.new_(mRowClass);
        copyFields(mm, rowVar, copyVar, mRowInfo.keyColumns.values());
        mm.invoke("decodeValues", copyVar, resultVar);
        mm.return_(copyVar);

        // Now implement the bridge method.
        mm = mClassMaker.addMethod
            (Object.class, variant, Transaction.class, Object.class).public_().bridge();
        mm.return_(mm.this_().invoke(returnType, variant, null, mm.param(0), mm.param(1)));
    }

    private static void copyFields(MethodMaker mm, Variable src, Variable dst,
                                   Collection<ColumnInfo> infos)
    {
        for (ColumnInfo info : infos) {
            Variable srcField = src.field(info.name);

            if (info.isArray()) {
                srcField = srcField.get();
                Label isNull = null;
                if (info.isNullable()) {
                    isNull = mm.label();
                    srcField.ifEq(null, isNull);
                }
                srcField.set(srcField.invoke("clone").cast(info.type));
                if (isNull != null) {
                    isNull.here();
                }
            }

            dst.field(info.name).set(srcField);
        }
    }

    /**
     * Adds a method which does most of the work for the update and merge methods. The
     * transaction parameter must not be null, which is committed when changes are made.
     *
     *     boolean doUpdate(Transaction txn, ActualRow row, boolean merge);
     */
    private void addDoUpdateMethod() {
        MethodMaker mm = mClassMaker.addMethod
            (boolean.class, "doUpdate", Transaction.class, mRowClass, boolean.class).private_();

        Variable txnVar = mm.param(0);
        Variable rowVar = mm.param(1);
        Variable mergeVar = mm.param(2);

        Label ready = mm.label();
        mm.invoke("checkPrimaryKeySet", rowVar).ifTrue(ready);

        if (mRowInfo.alternateKeys.isEmpty()) {
            mm.new_(IllegalStateException.class, "Primary key isn't fully specified").throw_();
        } else {
            // FIXME: check alternate keys too, and load using a join
            mm.new_(IllegalStateException.class,
                    "Primary or alternate key isn't fully specified").throw_();
        }

        ready.here();

        var keyVar = mm.invoke("encodePrimaryKey", rowVar);
        var cursorVar = mm.field("mSource").invoke("newCursor", txnVar);
        Label tryStart = mm.label().here();

        // If all value columns are dirty, replace the whole row and commit.
        {
            Label cont = mm.label();
            mm.invoke("checkValuesDirty", rowVar).ifFalse(cont);

            cursorVar.invoke("autoload", false);
            cursorVar.invoke("find", keyVar);
            Label replace = mm.label();
            cursorVar.invoke("value").ifNe(null, replace);
            mm.return_(false);
            replace.here();
            cursorVar.invoke("commit", mm.invoke("encodeValues", rowVar));
            mm.return_(true);

            cont.here();
        }

        cursorVar.invoke("find", keyVar);

        Label hasValue = mm.label();
        cursorVar.invoke("value").ifNe(null, hasValue);
        mm.return_(false);
        hasValue.here();

        // The bulk of the method isn't implemented until needed, delaying acquisition/creation
        // of the current schema version.

        var indy = mm.var(RowViewMaker.class).indy("indyDoUpdate", mRowType, mStoreRef);
        indy.invoke(null, "_", null, mm.this_(), rowVar, mergeVar, cursorVar);
        Label tryEnd = mm.label().here();
        mm.return_(true);

        mm.finally_(tryStart, () -> cursorVar.invoke("reset"));
    }

    static CallSite indyDoUpdate(MethodHandles.Lookup lookup, String name, MethodType mt,
                                 Class<?> rowType, WeakReference<RowStore> storeRef)
    {
        return indyEncode(lookup, name, mt, rowType, storeRef, RowViewMaker::finishIndyDoUpdate);
    }

    private static void finishIndyDoUpdate(MethodMaker mm, RowInfo rowInfo, int schemaVersion) {
        // All these variables were provided by the indy call in addDoUpdateMethod.
        Variable viewVar = mm.param(0);
        Variable rowVar = mm.param(1);
        Variable mergeVar = mm.param(2);
        Variable cursorVar = mm.param(3);

        Variable valueVar = cursorVar.invoke("value");
        Variable decodeVersion = decodeSchemaVersion(mm, valueVar);

        Label sameVersion = mm.label();
        decodeVersion.ifEq(schemaVersion, sameVersion);

        // If different schema versions, decode and re-encode a new value, and then go to the
        // next step. The simplest way to perform this conversion is to create a new temporary
        // row object, decode the value into it, and then create a new value from it.
        {
            var tempRowVar = mm.new_(rowVar);
            viewVar.invoke("decodeValues", tempRowVar, valueVar);
            valueVar.set(viewVar.invoke("encodeValues", tempRowVar));
        }

        sameVersion.here();

        RowGen rowGen = rowInfo.rowGen();
        ColumnCodec[] codecs = ColumnCodec.bind(rowGen.valueCodecs(), mm);
        Map<String, Integer> columnNumbers = rowGen.columnNumbers();

        // Identify the offsets to all the columns in the original value, and calculate the
        // size of the new value.

        String stateFieldName = null;
        Variable stateField = null;

        var columnOffsets = new int[codecs.length];   // start offsets for columns, if fixed
        var columnVars = new Variable[codecs.length]; // start offsets when not fixed

        Variable offsetVar = null;
        int fixedOffset = schemaVersion < 128 ? 1 : 4;
        var newSizeVar = mm.var(int.class).set(fixedOffset); // need room for schemaVersion

        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            codec.encodePrepare();

            if (offsetVar == null) {
                columnOffsets[i] = fixedOffset;
            } else {
                columnVars[i] = offsetVar.get();
            }
            offsetVar = codec.decodeSkip(valueVar, offsetVar, fixedOffset, null);
            if (offsetVar == null) {
                fixedOffset += codec.minSize();
            }

            ColumnInfo info = codec.mInfo;
            int num = columnNumbers.get(info.name);

            String sfName = rowGen.stateField(num);
            if (!sfName.equals(stateFieldName)) {
                stateFieldName = sfName;
                stateField = rowVar.field(stateFieldName).get();
            }

            Label cont = mm.label();

            int sfMask = rowGen.stateFieldMask(num);
            Label isDirty = mm.label();
            stateField.and(sfMask).ifEq(sfMask, isDirty);

            // Add in the size of existing column, which won't be updated.
            {
                codec.encodeSkip();
                if (offsetVar == null) {
                    newSizeVar.inc(fixedOffset - columnOffsets[i]);
                } else {
                    var startVar = columnVars[i];
                    newSizeVar.inc(offsetVar.sub(startVar == null ? columnOffsets[i] : startVar));
                }
                mm.goto_(cont);
            }

            // Add in the size of the dirty column, which needs to be encoded.
            isDirty.here();
            newSizeVar.inc(codec.minSize());
            codec.encodeSize(rowVar.field(info.name), newSizeVar);

            cont.here();
        }

        // Encode the new byte[] value...

        var newValueVar = mm.new_(byte[].class, newSizeVar);

        var srcOffsetVar = mm.var(int.class).set(0);
        var dstOffsetVar = mm.var(int.class).set(0);
        var spanLengthVar = mm.var(int.class).set(schemaVersion < 128 ? 1 : 4);
        var sysVar = mm.var(System.class);

        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            ColumnInfo info = codec.mInfo;
            int num = columnNumbers.get(info.name);

            Variable columnLenVar;
            {
                var startVar = columnVars[i];
                Variable endVar;
                if (i + 1 < codecs.length) {
                    endVar = columnVars[i + 1];
                    if (endVar == null) {
                        endVar = mm.var(int.class).set(columnOffsets[i + 1]);
                    }
                } else {
                    endVar = valueVar.alength();
                }
                columnLenVar = endVar.sub(startVar == null ? columnOffsets[i] : startVar);
            }

            int sfMask = rowGen.stateFieldMask(num);
            Label isDirty = mm.label();
            stateField.and(sfMask).ifEq(sfMask, isDirty);

            // Increase the copy span length.
            Label cont = mm.label();
            spanLengthVar.inc(columnLenVar);
            mm.goto_(cont);

            isDirty.here();

            // Copy the current span and prepare for the next span.
            {
                Label noSpan = mm.label();
                spanLengthVar.ifEq(0, noSpan);
                sysVar.invoke("arraycopy", valueVar, srcOffsetVar,
                              newValueVar, dstOffsetVar, spanLengthVar);
                srcOffsetVar.inc(spanLengthVar);
                dstOffsetVar.inc(spanLengthVar);
                spanLengthVar.set(0);
                noSpan.here();
            }

            // Encode the dirty column, and skip over the original column value.
            codec.encode(rowVar.field(info.name), newValueVar, dstOffsetVar, 0);
            srcOffsetVar.inc(columnLenVar);

            cont.here();
        }

        // Copy any remaining span.
        {
            Label noSpan = mm.label();
            spanLengthVar.ifEq(0, noSpan);
            sysVar.invoke("arraycopy", valueVar, srcOffsetVar,
                          newValueVar, dstOffsetVar, spanLengthVar);
            noSpan.here();
        }

        cursorVar.invoke("commit", newValueVar);

        Label doMerge = mm.label();
        mergeVar.ifTrue(doMerge);
        markAllUndirty(rowVar, rowInfo);
        mm.return_();

        doMerge.here();

        // Decode all the original column values that weren't updated into the row.

        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            ColumnInfo info = codec.mInfo;
            int num = columnNumbers.get(info.name);

            int sfMask = rowGen.stateFieldMask(num);
            Label cont = mm.label();
            stateField.and(sfMask).ifEq(sfMask, cont);

            offsetVar = columnVars[i];
            if (offsetVar == null) {
                fixedOffset = columnOffsets[i];
            }

            codec.decode(rowVar.field(info.name), valueVar, offsetVar, fixedOffset, null);

            cont.here();
        }

        markAllClean(rowVar, rowInfo);
    }

    /**
     * Delegates to the doUpdate method.
     */
    private void addUpdateMethod(String variant, boolean merge) {
        MethodMaker mm = mClassMaker.addMethod
            (boolean.class, variant, Transaction.class, Object.class).public_();
        Variable txnVar = mm.param(0);
        Variable rowVar = mm.param(1).cast(mRowClass);
        txnVar.set(mm.invoke("enterTransaction", txnVar));
        Label tryStart = mm.label().here();
        mm.return_(mm.invoke("doUpdate", txnVar, rowVar, merge));
        mm.finally_(tryStart, () -> txnVar.invoke("exit"));
    }

    private void addResetMethod() {
        MethodMaker mm = mClassMaker.addMethod(null, "reset", Object.class).public_();
        Variable rowVar = mm.param(0).cast(mRowClass);

        // Clear the column fields that refer to objects.
        for (ColumnInfo info : mRowInfo.allColumns.values()) {
            Field field = rowVar.field(info.name);
            if (!info.type.isPrimitive()) {
                field.set(null);
            }
        }

        // Clear the column state fields.
        for (String name : mRowGen.stateFields()) {
            rowVar.field(name).set(0);
        }
    }

    private void resetValueColumns(Variable rowVar) {
        // Clear the value column fields that refer to objects.
        for (ColumnInfo info : mRowInfo.valueColumns.values()) {
            Field field = rowVar.field(info.name);
            if (!info.type.isPrimitive()) {
                field.set(null);
            }
        }

        // Clear the value column state fields. Skip the key columns, which are numbered first.
        int num = mRowInfo.keyColumns.size();
        int mask = 0;
        for (ColumnInfo info : mRowInfo.valueColumns.values()) {
            mask |= RowGen.stateFieldMask(num);
            if (isMaskReady(++num, mask)) {
                mask = maskRemainder(num, mask);
                Field field = stateField(rowVar, num - 1);
                mask = ~mask;
                if (mask == 0) {
                    field.set(mask);
                } else {
                    field.set(field.and(mask));
                    mask = 0;
                }
            }
        }
    }

    private void markAllClean(Variable rowVar) {
        markAllClean(rowVar, mRowInfo);
    }

    private static void markAllClean(Variable rowVar, RowInfo info) {
        markAll(rowVar, info, 0x5555_5555);
    }

    private static void markAll(Variable rowVar, RowInfo info, int mask) {
        int i = 0;
        String[] stateFields = info.rowGen().stateFields();
        for (; i < stateFields.length - 1; i++) {
            rowVar.field(stateFields[i]).set(mask);
        }
        mask >>>= (32 - ((info.allColumns.size() & 0b1111) << 1));
        rowVar.field(stateFields[i]).set(mask);
    }

    /**
     * Remaining states are UNSET or CLEAN.
     */
    private static void markAllUndirty(Variable rowVar, RowInfo info) {
        int mask = 0x5555_5555;
        int i = 0;
        String[] stateFields = info.rowGen().stateFields();
        for (; i < stateFields.length - 1; i++) {
            var field = rowVar.field(stateFields[i]);
            field.set(field.and(mask));
        }
        mask >>>= (32 - ((info.allColumns.size() & 0b1111) << 1));
        var field = rowVar.field(stateFields[i]);
        field.set(field.and(mask));
    }

    private Field stateField(Variable rowVar, int columnNum) {
        return rowVar.field(mRowGen.stateField(columnNum));
    }

    /**
     * @param rowVariant RowScanner or RowUpdater
     */
    private void addScannerMethod(Class<?> rowVariant) {
        String methodName = "new" + rowVariant.getSimpleName().substring(3);
        MethodMaker mm = mClassMaker.addMethod(rowVariant, methodName, Cursor.class).public_();
        var indy = mm.var(RowViewMaker.class).indy("indyDefineScanner", mRowType, mRowClass);
        mm.return_(indy.invoke(rowVariant, "_", null, mm.param(0)));
    }

    /**
     * Defines the scanner/updater class and returns a call to the constructor.
     */
    static CallSite indyDefineScanner(MethodHandles.Lookup lookup, String name, MethodType mt,
                                      Class rowType, Class rowClass)
    {
        Class<?> variant = mt.returnType(); // RowScanner or RowUpdater

        RowInfo rowInfo = RowInfo.find(rowType);

        String variantName = variant.getSimpleName();
        ClassMaker cm = RowGen.beginClassMaker
            (lookup, rowInfo, variantName.substring(3))
            .extend(RowViewMaker.class.getPackageName() + ".Abstract" + variantName);

        cm.addField(rowClass, "row").private_();

        {
            MethodMaker mm = cm.addMethod(Object.class, "row").public_();
            mm.return_(mm.field("row"));
        }

        {
            MethodMaker mm = cm.addConstructor(Cursor.class);
            mm.invokeSuperConstructor(mm.param(0));
            mm.invoke("init");
        }

        {
            MethodMaker mm = cm.addMethod
                (Object.class, "decodeRow",
                 LockResult.class, byte[].class, byte[].class).protected_();
            var viewVar = mm.var(lookup.lookupClass());
            var rowVar = mm.new_(rowClass);
            viewVar.invoke("decodePrimaryKey", rowVar, mm.param(1));
            viewVar.invoke("decodeValues", rowVar, mm.param(2));
            markAllClean(rowVar, rowInfo);
            mm.field("row").set(rowVar);
            mm.return_(rowVar);
        }

        {
            MethodMaker mm = cm.addMethod
                (boolean.class, "decodeRow",
                 LockResult.class, byte[].class, byte[].class, Object.class).protected_();
            var viewVar = mm.var(lookup.lookupClass());
            var rowVar = mm.param(3).cast(rowClass);
            viewVar.invoke("decodePrimaryKey", rowVar, mm.param(1));
            viewVar.invoke("decodeValues", rowVar, mm.param(2));
            markAllClean(rowVar, rowInfo);
            mm.field("row").set(rowVar);
            mm.return_(true);
        }

        {
            MethodMaker mm = cm.addMethod(null, "clearRow").protected_();
            mm.field("row").set(null);
        }

        if (variant == RowUpdater.class) {
            // FIXME: define update and delete methods
        }

        var clazz = cm.finish();

        try {
            var mh = lookup.findConstructor(clazz, mt.changeReturnType(void.class));
            return new ConstantCallSite(mh.asType(mt));
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }
}
