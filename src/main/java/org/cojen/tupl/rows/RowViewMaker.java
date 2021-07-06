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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.ref.WeakReference;

import java.util.Collection;
import java.util.Map;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.CorruptDatabaseException;
import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import org.cojen.tupl.filter.RowFilter;

/**
 * Makes RowView classes that extend AbstractRowView.
 *
 * @author Brian S O'Neill
 */
public class RowViewMaker {
    private final WeakReference<RowStore> mStoreRef;
    private final Class<?> mRowType;
    private final RowGen mRowGen;
    private final RowInfo mRowInfo;
    private final Class<?> mRowClass;
    private final long mIndexId;
    private final ClassMaker mClassMaker;

    /**
     * @param store generated class is pinned to this specific instance
     */
    RowViewMaker(RowStore store, Class<?> type, RowGen gen, long indexId) {
        mStoreRef = new WeakReference<>(store);
        mRowType = type;
        mRowGen = gen;
        mRowInfo = gen.info;
        mRowClass = RowMaker.find(type);
        mIndexId = indexId;
        mClassMaker = gen.beginClassMaker(getClass(), type, "View")
            .extend(AbstractRowView.class).final_().public_();
    }

    /**
     * @return a constructor which accepts a View and returns a AbstractRowView implementation
     */
    MethodHandle finish() {
        {
            MethodMaker mm = mClassMaker.addConstructor(View.class);
            mm.invokeSuperConstructor(mm.param(0));
        }

        // Add the simple rowType method.
        mClassMaker.addMethod(Class.class, "rowType").public_().return_(mRowType);

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

            //addCheckSet("checkValue", mRowInfo.valueColumns);

            addCheckSet("checkAllSet", mRowInfo.allColumns);
            addRequireSet("requireAllSet", mRowInfo.allColumns);

            int i = 0;
            for (ColumnSet altKey : mRowInfo.alternateKeys) {
                addCheckSet("checkAltKeySet$" + i, altKey.keyColumns);
                i++;
            }

            if (!mRowInfo.valueColumns.isEmpty()) {
                addCheckAllDirty("checkValueAllDirty", mRowInfo.valueColumns);
            }

            addCheckAnyDirty("checkPrimaryKeyAnyDirty", mRowInfo.keyColumns);
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

        addUnfilteredMethod();

        addFilteredFactoryMethod();

        try {
            var lookup = mClassMaker.finishLookup();
            return lookup.findConstructor(lookup.lookupClass(),
                                          MethodType.methodType(void.class, View.class));
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    /**
     * Defines a static method which accepts a row and returns boolean. When it returns true,
     * all of the given columns are set.
     *
     * @param name method name
     */
    private void addCheckSet(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, name, mRowClass).static_();

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
    private void addCheckAllDirty(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, name, mRowClass).static_();

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
     * Defines a static method which accepts a row and returns boolean. When it returns true,
     * at least one of the given columns are dirty.
     *
     * @param name method name
     */
    private void addCheckAnyDirty(String name, Map<String, ColumnInfo> columns) {
        MethodMaker mm = mClassMaker.addMethod(boolean.class, name, mRowClass).static_();

        int num = 0, mask = 0;

        for (int step = 0; step < 2; step++) {
            // Key columns are numbered before value columns. Add checks in two steps.
            var baseColumns = step == 0 ? mRowInfo.keyColumns : mRowInfo.valueColumns;

            for (ColumnInfo info : baseColumns.values()) {
                if (columns.containsKey(info.name)) {
                    mask |= RowGen.stateFieldMask(num, 0b10);
                }

                if (isMaskReady(++num, mask)) {
                    Label cont = mm.label();
                    stateField(mm.param(0), num - 1).and(mask).ifEq(0, cont);
                    mm.return_(true);
                    cont.here();
                    mask = 0;
                }
            }
        }

        mm.return_(false);
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
            if (shift != 0) {
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
        MethodMaker mm = mClassMaker.addMethod(null, name, mRowClass).static_();

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
        MethodMaker mm = mClassMaker.addMethod(byte[].class, name, mRowClass).static_();
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
        var offsetVar = mm.var(int.class).set(0);
        for (ColumnCodec codec : codecs) {
            codec.encode(findField(mm.param(0), codec), dstVar, offsetVar);
        }

        mm.return_(dstVar);
    }

    /**
     * Method isn't implemented until needed, delaying acquisition/creation of the current
     * schema version. This allows replicas to decode existing rows even when the class
     * definition has changed, but encoding will still fail.
     */
    private void addDynamicEncodeValueColumns() {
        MethodMaker mm = mClassMaker.addMethod(byte[].class, "encodeValue", mRowClass).static_();
        var indy = mm.var(RowViewMaker.class).indy
            ("indyEncodeValueColumns", mStoreRef, mRowType, mIndexId);
        mm.return_(indy.invoke(byte[].class, "encodeValue", null, mm.param(0)));
    }

    public static CallSite indyEncodeValueColumns
        (MethodHandles.Lookup lookup, String name, MethodType mt,
         WeakReference<RowStore> storeRef, Class<?> rowType, long indexId)
    {
        return doIndyEncode
            (lookup, name, mt, storeRef, rowType, indexId, (mm, info, schemaVersion) -> {
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
    private static CallSite doIndyEncode(MethodHandles.Lookup lookup, String name, MethodType mt,
                                         WeakReference<RowStore> storeRef,
                                         Class<?> rowType, long indexId,
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
                    schemaVersion = store.schemaVersion(info, indexId);
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
        MethodMaker mm = mClassMaker.addMethod(null, name, mRowClass, byte[].class)
            .static_().public_();
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
        Variable offsetVar = mm.var(int.class).set(fixedOffset);

        for (ColumnCodec srcCodec : srcCodecs) {
            String name = srcCodec.mInfo.name;
            ColumnInfo dstInfo = dstRowInfo.allColumns.get(name);

            if (dstInfo == null) {
                srcCodec.decodeSkip(srcVar, offsetVar, null);
            } else {
                var rowVar = mm.param(0);
                Field dstVar = rowVar.field(name);
                Converter.decode(mm, srcVar, offsetVar, null, srcCodec, dstInfo, dstVar);
            }
        }
    }

    private void addDynamicDecodeValueColumns() {
        // First define a method which generates the SwitchCallSite.
        {
            MethodMaker mm = mClassMaker.addMethod
                (SwitchCallSite.class, "decodeValueSwitchCallSite").static_();
            var condy = mm.var(RowViewMaker.class).condy
                ("condyDecodeValueColumns",  mStoreRef, mRowType, mRowClass, mIndexId);
            mm.return_(condy.invoke(SwitchCallSite.class, "_"));
        }

        // Also define a method to obtain a MethodHandle which decodes for a given schema
        // version. This must be defined here to ensure that the correct lookup is used. It
        // must always refer to this view class.
        {
            MethodMaker mm = mClassMaker.addMethod
                (MethodHandle.class, "decodeValueHandle", int.class).static_();
            var lookup = mm.var(MethodHandles.class).invoke("lookup");
            var mh = mm.invoke("decodeValueSwitchCallSite").invoke("getCase", lookup, mm.param(0));
            mm.return_(mh);
        }

        MethodMaker mm = mClassMaker.addMethod
            (null, "decodeValue", mRowClass, byte[].class).static_().public_();

        var data = mm.param(1);
        var schemaVersion = decodeSchemaVersion(mm, data);

        var indy = mm.var(RowViewMaker.class).indy("indyDecodeValueColumns");
        indy.invoke(null, "decodeValue", null, schemaVersion, mm.param(0), data);
    }

    /**
     * Returns a SwitchCallSite instance suitable for decoding all value columns. By defining
     * it via a "condy" method, the SwitchCallSite instance can be shared by other methods. In
     * particular, filter subclasses are generated against specific schema versions, and so
     * they need direct access to just one of the cases. This avoids a redundant version check.
     *
     * MethodType is: void (int schemaVersion, RowClass row, byte[] data)
     */
    public static SwitchCallSite condyDecodeValueColumns
        (MethodHandles.Lookup lookup, String name, Class<?> type,
         WeakReference<RowStore> storeRef, Class<?> rowType, Class<?> rowClass, long indexId)
    {
        MethodType mt = MethodType.methodType(void.class, int.class, rowClass, byte[].class);

        return new SwitchCallSite(lookup, mt, schemaVersion -> {
            MethodMaker mm = MethodMaker.begin(lookup, null, "case", rowClass, byte[].class);

            RowStore store = storeRef.get();
            if (store == null) {
                mm.new_(DatabaseException.class, "Closed").throw_();
            } else {
                RowInfo dstRowInfo = RowInfo.find(rowType);

                if (schemaVersion == 0) {
                    // No columns to decode, so assign defaults.
                    for (Map.Entry<String, ColumnInfo> e : dstRowInfo.valueColumns.entrySet()) {
                        Converter.setDefault(e.getValue(), mm.param(0).field(e.getKey()));
                    }
                } else {
                    RowInfo srcRowInfo;
                    try {
                        srcRowInfo = store.rowInfo(rowType, indexId, schemaVersion);
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
                }

                markAllClean(mm.param(0), dstRowInfo);
            }

            return mm.finish();
        });
    }

    /**
     * This just returns the SwitchCallSite generated by condyDecodeValueColumns.
     */
    public static SwitchCallSite indyDecodeValueColumns(MethodHandles.Lookup lookup,
                                                        String name, MethodType mt)
        throws Throwable
    {
        MethodHandle mh = lookup.findStatic(lookup.lookupClass(), "decodeValueSwitchCallSite",
                                            MethodType.methodType(SwitchCallSite.class));
        return (SwitchCallSite) mh.invokeExact();
    }

    /**
     * Decodes the first 1 to 4 bytes of the given byte array into a schema version int
     * variable. When the given byte array is empty, the schema version is zero.
     */
    static Variable decodeSchemaVersion(MethodMaker mm, Variable bytes) {
        var schemaVersion = mm.var(int.class);
        Label notEmpty = mm.label();
        bytes.alength().ifNe(0, notEmpty);
        schemaVersion.set(0);
        Label cont = mm.label();
        mm.goto_(cont);
        notEmpty.here();
        schemaVersion.set(bytes.aget(0));
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
            mm.invoke("decodeValue", rowVar, valueVar);
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
        var valueVar = mm.invoke("encodeValue", rowVar);

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
        mm.invoke("decodeValue", copyVar, resultVar);
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
            (boolean.class, "doUpdate", Transaction.class, mRowClass, boolean.class);

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
            Label cont;
            if (mRowInfo.valueColumns.isEmpty()) {
                // If the checkValueAllDirty method was defined, it would always return true.
                cont = null;
            } else {
                cont = mm.label();
                mm.invoke("checkValueAllDirty", rowVar).ifFalse(cont);
            }

            cursorVar.invoke("autoload", false);
            cursorVar.invoke("find", keyVar);
            Label replace = mm.label();
            cursorVar.invoke("value").ifNe(null, replace);
            mm.return_(false);
            replace.here();
            cursorVar.invoke("commit", mm.invoke("encodeValue", rowVar));
            mm.return_(true);

            if (cont == null) {
                return;
            }

            cont.here();
        }

        cursorVar.invoke("find", keyVar);

        Label hasValue = mm.label();
        cursorVar.invoke("value").ifNe(null, hasValue);
        mm.return_(false);
        hasValue.here();

        // The bulk of the method isn't implemented until needed, delaying acquisition/creation
        // of the current schema version.

        var indy = mm.var(RowViewMaker.class).indy("indyDoUpdate", mStoreRef, mRowType, mIndexId);
        indy.invoke(null, "doUpdate", null, mm.this_(), rowVar, mergeVar, cursorVar);
        mm.return_(true);

        mm.finally_(tryStart, () -> cursorVar.invoke("reset"));
    }

    public static CallSite indyDoUpdate(MethodHandles.Lookup lookup, String name, MethodType mt,
                                        WeakReference<RowStore> storeRef,
                                        Class<?> rowType, long indexId)
    {
        return doIndyEncode(lookup, name, mt, storeRef, rowType, indexId,
                            RowViewMaker::finishIndyDoUpdate);
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
            viewVar.invoke("decodeValue", tempRowVar, valueVar);
            valueVar.set(viewVar.invoke("encodeValue", tempRowVar));
        }

        sameVersion.here();

        RowGen rowGen = rowInfo.rowGen();
        ColumnCodec[] codecs = ColumnCodec.bind(rowGen.valueCodecs(), mm);
        Map<String, Integer> columnNumbers = rowGen.columnNumbers();

        // Identify the offsets to all the columns in the original value, and calculate the
        // size of the new value.

        String stateFieldName = null;
        Variable stateField = null;

        var columnVars = new Variable[codecs.length];

        int fixedOffset = schemaVersion < 128 ? 1 : 4;
        Variable offsetVar = mm.var(int.class).set(fixedOffset);
        var newSizeVar = mm.var(int.class).set(fixedOffset); // need room for schemaVersion

        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            codec.encodePrepare();

            columnVars[i] = offsetVar.get();
            codec.decodeSkip(valueVar, offsetVar, null);

            ColumnInfo info = codec.mInfo;
            int num = columnNumbers.get(info.name);

            String sfName = rowGen.stateField(num);
            if (!sfName.equals(stateFieldName)) {
                stateFieldName = sfName;
                stateField = rowVar.field(stateFieldName).get();
            }

            Label cont = mm.label();

            int sfMask = RowGen.stateFieldMask(num);
            Label isDirty = mm.label();
            stateField.and(sfMask).ifEq(sfMask, isDirty);

            // Add in the size of existing column, which won't be updated.
            {
                codec.encodeSkip();
                newSizeVar.inc(offsetVar.sub(columnVars[i]));
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
                Variable endVar;
                if (i + 1 < codecs.length) {
                    endVar = columnVars[i + 1];
                } else {
                    endVar = valueVar.alength();
                }
                columnLenVar = endVar.sub(columnVars[i]);
            }

            int sfMask = RowGen.stateFieldMask(num);
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
            codec.encode(rowVar.field(info.name), newValueVar, dstOffsetVar);
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

            int sfMask = RowGen.stateFieldMask(num);
            Label cont = mm.label();
            stateField.and(sfMask).ifEq(sfMask, cont);

            codec.decode(rowVar.field(info.name), valueVar, columnVars[i], null);

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

    static void markAllClean(Variable rowVar, RowInfo info) {
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
     * Defines a method which returns a singleton RowDecoderEncoder instance.
     */
    private void addUnfilteredMethod() {
        MethodMaker mm = mClassMaker.addMethod(RowDecoderEncoder.class, "unfiltered").protected_();
        var condy = mm.var(RowViewMaker.class).condy("condyDefineUnfiltered", mRowType, mRowClass);
        mm.return_(condy.invoke(RowDecoderEncoder.class, "unfiltered"));
    }

    public static Object condyDefineUnfiltered(MethodHandles.Lookup lookup, String name, Class type,
                                               Class rowType, Class rowClass)
        throws Throwable
    {
        RowInfo rowInfo = RowInfo.find(rowType);

        ClassMaker cm = RowGen.beginClassMaker
            (RowViewMaker.class, rowType, rowInfo, null, "Unfiltered")
            .implement(RowDecoderEncoder.class).public_();

        // Subclassed by filter implementations.
        cm.addConstructor().protected_();

        {
            // Defined by RowDecoderEncoder.
            MethodMaker mm = cm.addMethod
                (Object.class, "decodeRow", byte[].class, byte[].class, Object.class).public_();
            var viewVar = mm.var(lookup.lookupClass());
            var rowVar = mm.param(2).cast(rowClass);
            Label hasRow = mm.label();
            rowVar.ifNe(null, hasRow);
            rowVar.set(mm.new_(rowClass));
            hasRow.here();
            viewVar.invoke("decodePrimaryKey", rowVar, mm.param(0));
            viewVar.invoke("decodeValue", rowVar, mm.param(1));
            markAllClean(rowVar, rowInfo);
            mm.return_(rowVar);
        }

        {
            // Defined by RowDecoderEncoder.
            MethodMaker mm = cm.addMethod(byte[].class, "encodeKey", Object.class).public_();
            var rowVar = mm.param(0).cast(rowClass);
            var viewVar = mm.var(lookup.lookupClass());
            Label unchanged = mm.label();
            viewVar.invoke("checkPrimaryKeyAnyDirty", rowVar).ifFalse(unchanged);
            mm.return_(viewVar.invoke("encodePrimaryKey", rowVar));
            unchanged.here();
            mm.return_(null);
        }

        {
            // Defined by RowDecoderEncoder.
            MethodMaker mm = cm.addMethod(byte[].class, "encodeValue", Object.class).public_();
            var rowVar = mm.param(0).cast(rowClass);
            var viewVar = mm.var(lookup.lookupClass());
            mm.return_(viewVar.invoke("encodeValue", rowVar));
        }

        {
            // Used by filter subclasses.
            MethodMaker mm = cm.addMethod(null, "markAllClean", rowClass).protected_().static_();
            RowViewMaker.markAllClean(mm.param(0), rowInfo);
        }

        {
            // Used by filter subclasses. The int param is the schema version.
            MethodMaker mm = cm.addMethod
                (MethodHandle.class, "decodeValueHandle", int.class).protected_().static_();
            var viewVar = mm.var(lookup.lookupClass());
            mm.return_(viewVar.invoke("decodeValueHandle", mm.param(0)));
        }

        var clazz = cm.finish();

        return lookup.findConstructor(clazz, MethodType.methodType(void.class)).invoke();
    }

    private void addFilteredFactoryMethod() {
        MethodMaker mm = mClassMaker.addMethod
            (MethodHandle.class, "filteredFactory", String.class, RowFilter.class);
        var storeRefVar = mm.var(WeakReference.class).setExact(mStoreRef);
        var maker = mm.new_(RowFilterMaker.class, storeRefVar,
                            mm.class_(), mm.invoke("unfiltered").invoke("getClass"),
                            mRowType, mIndexId, mm.param(0), mm.param(1));
        mm.return_(maker.invoke("finish"));
    }
}
