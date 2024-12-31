/*
 *  Copyright (C) 2023 Cojen.org
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

import java.io.Closeable;
import java.io.IOException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.Map;
import java.util.TreeMap;

import org.cojen.dirmi.Pipe;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.remote.RemoteTableProxy;
import org.cojen.tupl.remote.RemoteTransaction;
import org.cojen.tupl.remote.RemoteUpdater;
import org.cojen.tupl.remote.ServerTransaction;
import org.cojen.tupl.remote.ServerUpdater;

import org.cojen.tupl.table.codec.ColumnCodec;

/**
 * Generates classes which are used by the remote server.
 *
 * @author Brian S O'Neill
 */
public final class RemoteProxyMaker {
    private static final SoftCache<TupleKey, MethodHandle, BaseTable> cCache = new SoftCache<>() {
        @Override
        protected MethodHandle newValue(TupleKey key, BaseTable table) {
            return new RemoteProxyMaker(table, (byte[]) key.get(1)).finish();
        }
    };

    /**
     * @param schemaVersion is zero if the table isn't evolvable
     * @param descriptor encoding format is defined by RowHeader class
     */
    static RemoteTableProxy make(BaseTable<?> table, int schemaVersion, byte[] descriptor) {
        var mh = cCache.obtain(TupleKey.make.with(table.getClass(), descriptor), table);
        try {
            return (RemoteTableProxy) mh.invoke(table, schemaVersion);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private final BaseTable mTable;
    private final Class<?> mRowType;
    private final Class<?> mRowClass;
    private final RowGen mRowGen;
    private final RowHeader mRowHeader;
    private final ClassMaker mClassMaker;
    private final ColumnInfo mAutoColumn;

    // Only needed by addUpdateDirectMethod.
    private Variable mUpdaterFactoryVar;

    private ColumnCodec[] mClientCodecs;

    private RemoteProxyMaker(BaseTable table, byte[] descriptor) {
        mTable = table;
        mRowType = table.rowType();
        mRowClass = RowMaker.find(mRowType);
        mRowGen = RowInfo.find(mRowType).rowGen();
        mRowHeader = RowHeader.decode(descriptor);
        mClassMaker = mRowGen.beginClassMaker(RemoteProxyMaker.class, mRowType, null);
        mClassMaker.implement(RemoteTableProxy.class);

        ColumnInfo auto = null;
        for (ColumnInfo column : mRowGen.info.keyColumns.values()) {
            if (column.isAutomatic()) {
                auto = column;
                break;
            }
        }

        mAutoColumn = auto;
    }

    private boolean isEvolvable() {
        return mTable instanceof StoredTable st && st.isEvolvable();
    }

    private static boolean isEvolvable(Class<?> tableClass, Class<?> rowType) {
        return StoredTable.class.isAssignableFrom(tableClass) && StoredTable.isEvolvable(rowType);
    }

    /**
     * Returns a factory MethodHandle which constructs a RemoteTableProxy:
     *
     *     RemoteTableProxy new(BaseTable table, int schemaVersion);
     */
    private MethodHandle finish() {
        mClassMaker.addField(boolean.class, "assert").private_().static_().final_();

        {
            MethodMaker mm = mClassMaker.addClinit();
            mm.field("assert").set(mm.class_().invoke("desiredAssertionStatus"));
        }

        var tableClass = mTable.getClass();
        mClassMaker.addField(tableClass, "table").private_().final_();

        MethodMaker ctorMaker = mClassMaker.addConstructor(tableClass, int.class).private_();
        ctorMaker.invokeSuperConstructor();
        ctorMaker.field("table").set(ctorMaker.param(0));

        MethodHandles.Lookup lookup;

        if (!(mTable instanceof StoredTable) || !RowHeader.make(mRowGen).equals(mRowHeader)) {
            lookup = makeConverter();
        } else {
            // Define a singleton empty row which is needed by the trigger methods.
            {
                mClassMaker.addField(mRowClass, "EMPTY_ROW").private_().static_().final_();
                MethodMaker mm = mClassMaker.addClinit();
                mm.field("EMPTY_ROW").set(mm.new_(mRowClass));
            }

            if (isEvolvable()) {
                mClassMaker.addField(int.class, "prefixLength").private_().final_();
                mClassMaker.addField(int.class, "schemaVersion").private_().final_();

                var schemaVersionVar = ctorMaker.param(1);

                Label small = ctorMaker.label();
                schemaVersionVar.ifLt(128, small);
                ctorMaker.field("prefixLength").set(4);
                // Version is pre-encoded using the prefix format.
                schemaVersionVar.set(schemaVersionVar.or(1 << 31));
                Label cont = ctorMaker.label().goto_();
                small.here();
                ctorMaker.field("prefixLength").set(1);
                cont.here();
                ctorMaker.field("schemaVersion").set(schemaVersionVar);
            }

            lookup = makeDirect();
        }

        try {
            MethodType mt = MethodType.methodType(void.class, tableClass, int.class);
            return lookup.findConstructor(lookup.lookupClass(), mt);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    /**
     * Makes a RemoteTableProxy class which can copy the encoded bytes to/from the index directly.
     */
    private MethodHandles.Lookup makeDirect() {
        addRequireSet("requireAllSet", mRowGen.info.allColumns);

        addByKeyDirectMethod("tryLoad");
        addByKeyDirectMethod("exists");
        addByKeyDirectMethod("tryDelete");

        addStoreDirectMethod("store");
        addStoreDirectMethod("exchange");
        addStoreDirectMethod("tryInsert");
        addStoreDirectMethod("tryReplace");

        addUpdateDirectMethod("tryUpdate");
        addUpdateDirectMethod("tryMerge");

        // The Updater methods don't currently act upon encoded bytes directly but instead act
        // upon resolved row objects. This is somewhat less efficient, but a remote Updater
        // isn't very efficient regardless. All operations require a roundtrip call.

        // The decode and encode methods are needed by the Updater methods.
        addDecodeRow();
        addEncodeColumns(true);
        addEncodeColumns(false);

        addUpdaterMethod("row");
        addUpdaterMethod("step");
        addUpdaterMethod("update");
        addUpdaterMethod("delete");

        return mClassMaker.finishLookup();
    }

    /**
     * Defines a static method which accepts a Pipe and state variables and writes a detailed
     * exception describing the required columns which aren't set. As a side-effect, the key
     * and value from the pipe are skipped, and the pipe is recycled.
     *
     * This method should only be called if checkSet returned false.
     */
    private void addRequireSet(String name, Map<String, ColumnInfo> columns) {
        var paramTypes = new Object[1 + ((columns.size() + 15) / 16)];
        paramTypes[0] = Pipe.class;
        for (int i=1; i<paramTypes.length; i++) {
            paramTypes[i] = int.class;
        }

        MethodMaker mm = mClassMaker.addMethod(null, name, paramTypes).private_().static_();

        var pipeVar = mm.param(0);

        mm.var(RemoteProxyMaker.class).invoke("skipKeyAndValue", pipeVar);

        var exVar = mm.var(Throwable.class);

        Label tryStart = mm.label().here();
        mRowGen.requireSet(mm, columns, num -> mm.param(1 + (num >> 4)));
        exVar.set(mm.new_(AssertionError.class)); // didn't call checkSet
        Label finish = mm.label().goto_();
        Label tryEnd = mm.label().here();
        exVar.set(mm.catch_(tryStart, tryEnd, Throwable.class));

        finish.here();

        pipeVar.invoke("writeObject", exVar);
        pipeVar.invoke("flush");
        pipeVar.invoke("recycle");

        mm.return_();
    }

    /**
     * Generates code which writes an exception if the primary key isn't fully specified. As a
     * side-effect, the key and value from the pipe are skipped, and the pipe is recycled.
     */
    private void checkPrimaryKeySet(MethodMaker mm, Variable pipeVar, Variable[] stateVars) {
        Label isReady = mm.label();
        checkSet(mm, mRowGen.info.keyColumns, stateVars).ifTrue(isReady);

        mm.var(RemoteProxyMaker.class).invoke("skipKeyAndValue", pipeVar);
        var iseVar = mm.new_(IllegalStateException.class, "Primary key isn't fully specified");
        pipeVar.invoke("writeObject", iseVar);
        pipeVar.invoke("flush");
        pipeVar.invoke("recycle");
        mm.return_(null);

        isReady.here();
    }

    /**
     * @param variant "tryLoad", "exists", or "tryDelete"
     */
    private void addByKeyDirectMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (Pipe.class, variant, RemoteTransaction.class, Pipe.class).public_();

        var txnVar = txn(mm.param(0));
        var pipeVar = mm.param(1);

        Label tryStart = mm.label().here();

        var stateVars = readStateFields(pipeVar);
        checkPrimaryKeySet(mm, pipeVar, stateVars);

        var makerVar = mm.var(RemoteProxyMaker.class);
        var keyVar = makerVar.invoke("decodeKey", pipeVar);
        makerVar.invoke("skipValue", pipeVar);

        var valueVar = makerVar.invoke(variant, mm.field("table"), txnVar, keyVar, pipeVar);

        if (variant == "tryLoad") {
            Label done = mm.label();
            valueVar.ifEq(null, done);
            convertAndWriteValue(pipeVar, valueVar);
            pipeVar.invoke("flush");
            pipeVar.invoke("recycle");
            done.here();
        }

        mm.return_(null);

        var exVar = mm.catch_(tryStart, mm.label().here(), Throwable.class);
        makerVar.invoke("handleException", exVar, pipeVar);
        mm.return_(null);
    }

    /**
     * @param variant "store", "exchange", "tryinsert", or "tryReplace"
     */
    private void addStoreDirectMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (Pipe.class, variant, RemoteTransaction.class, Pipe.class).public_();

        var txnVar = txn(mm.param(0));
        var pipeVar = mm.param(1);

        Label tryStart = mm.label().here();

        var stateVars = readStateFields(pipeVar);

        Label isReady = mm.label();
        checkSet(mm, mRowGen.info.allColumns, stateVars).ifTrue(isReady);

        var makerVar = mm.var(RemoteProxyMaker.class);

        if (variant != "tryReplace" && mAutoColumn != null) {
            // Check if all columns are set except the automatic column.
            var allButAuto = new TreeMap<>(mRowGen.info.allColumns);
            allButAuto.remove(mAutoColumn.name);
            Label notReady = mm.label();
            checkSet(mm, allButAuto, stateVars).ifFalse(notReady);

            int tailLen = mAutoColumn.type == int.class ? 4 : 8;
            var keyVar = makerVar.invoke("decodeKey", pipeVar, tailLen);
            var valueVar = decodeValue(makerVar, pipeVar);

            var tableVar = mm.field("table").get();

            makerVar.invoke
                ("storeAuto", tableVar, tableVar.field("autogen"),
                 txnVar, mm.field("EMPTY_ROW"), keyVar, valueVar, pipeVar);

            mm.return_(null);

            notReady.here();
        }

        var paramVars = new Object[1 + stateVars.length];
        paramVars[0] = pipeVar;
        System.arraycopy(stateVars, 0, paramVars, 1, stateVars.length);
        mm.invoke("requireAllSet", paramVars);
        mm.return_(null);

        isReady.here();

        var keyVar = makerVar.invoke("decodeKey", pipeVar);
        var valueVar = decodeValue(makerVar, pipeVar);

        var oldValueVar = makerVar.invoke(variant, mm.field("table"), txnVar, mm.field("EMPTY_ROW"),
                                          keyVar, valueVar, pipeVar);

        if (variant == "exchange") {
            Label done = mm.label();
            oldValueVar.ifEq(null, done);
            convertAndWriteValue(pipeVar, oldValueVar);
            pipeVar.invoke("flush");
            pipeVar.invoke("recycle");
            done.here();
        }

        mm.return_(null);

        var exVar = mm.catch_(tryStart, mm.label().here(), Throwable.class);
        makerVar.invoke("handleException", exVar, pipeVar);
        mm.return_(null);
    }

    /**
     * Decodes a value from the pipe and assignes a schema version if required.
     */
    private Variable decodeValue(Variable makerVar, Variable pipeVar) {
        MethodMaker mm = makerVar.methodMaker();
        Variable valueVar;
        if (isEvolvable()) {
            valueVar = makerVar.invoke("decodeValue", pipeVar, mm.field("prefixLength"));
            mm.var(RowUtils.class).invoke("encodePrefixPF", valueVar, 0, mm.field("schemaVersion"));
        } else {
            valueVar = makerVar.invoke("decodeValue", pipeVar);
        }
        return valueVar;
    }

    /**
     * @param variant "tryUpdate" or "tryMerge"
     */
    private void addUpdateDirectMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (Pipe.class, variant, RemoteTransaction.class, Pipe.class).public_();

        var txnVar = txn(mm.param(0));
        var pipeVar = mm.param(1);

        Label tryStart = mm.label().here();

        var stateVars = readStateFields(pipeVar);

        checkPrimaryKeySet(mm, pipeVar, stateVars);

        var makerVar = mm.var(RemoteProxyMaker.class);
        var keyVar = makerVar.invoke("decodeKey", pipeVar);

        Label partial = mm.label();
        checkDirty(mm, mRowGen.info.valueColumns, stateVars).ifFalse(partial);

        // All value columns are dirty, so just do a plain replace operation as an optimization.

        var newValueVar = decodeValue(makerVar, pipeVar);

        Label mergeReply;
        if (variant == "tryMerge") {
            var resultVar = makerVar.invoke("mergeReplace", mm.field("table"), txnVar,
                                            mm.field("EMPTY_ROW"), keyVar, newValueVar, pipeVar);
            mergeReply = mm.label();
            resultVar.ifTrue(mergeReply);
        } else {
            makerVar.invoke("tryReplace", mm.field("table"), txnVar,
                            mm.field("EMPTY_ROW"), keyVar, newValueVar, pipeVar);
            mergeReply = null;
        }

        mm.return_(null);

        // Normal update/merge operation follows...

        partial.here();

        var dirtyValueVar = makerVar.invoke("decodeValue", pipeVar);

        if (mUpdaterFactoryVar == null) {
            // Define and share a reference to the ValueUpdater factory constant.
            mUpdaterFactoryVar = makerVar.condy
                ("condyValueUpdater", mm.class_(), mTable.getClass(), mRowType)
                .invoke(MethodHandle.class, "_");
        }

        Variable updaterVar;
        {
            var params = new Object[2 + stateVars.length];
            params[0] = mm.this_();
            params[1] = dirtyValueVar;
            System.arraycopy(stateVars, 0, params, 2, stateVars.length);
            // Make sure the updaterFactoryVar is bound to this method.
            var updaterFactoryVar = mm.var(MethodHandle.class).set(mUpdaterFactoryVar);
            updaterVar = updaterFactoryVar.invoke
                (StoredTable.ValueUpdater.class, "invokeExact", (Object[]) null, params);
        }

        var resultVar = makerVar.invoke(variant, mm.field("table"), txnVar, mm.field("EMPTY_ROW"),
                                        keyVar, updaterVar, pipeVar);

        if (variant == "tryMerge") {
            newValueVar.set(resultVar);
            Label done = mm.label();
            newValueVar.ifEq(null, done);
            mergeReply.here();
            writeValue(pipeVar, newValueVar);
            pipeVar.invoke("flush");
            pipeVar.invoke("recycle");
            done.here();
        }

        mm.return_(null);

        var exVar = mm.catch_(tryStart, mm.label().here(), Throwable.class);
        makerVar.invoke("handleException", exVar, pipeVar);
        mm.return_(null);
    }

    /**
     * Returns a factory MethodHandle which constructs a ValueUpdater:
     *
     *     ValueUpdater new(RemoteTableProxy proxy, byte[] dirtyValue, int... states);
     *
     * The state parameters are defined as plain ints and not as varargs.
     */
    public static MethodHandle condyValueUpdater(MethodHandles.Lookup lookup, String name,
                                                 Class<?> type, Class<?> proxyClass,
                                                 Class<?> tableClass, Class<?> rowType)
    {
        RowInfo rowInfo = RowInfo.find(rowType);
        RowGen rowGen = rowInfo.rowGen();

        ClassMaker cm = ClassMaker.begin(proxyClass.getName(), lookup);
        cm.implement(StoredTable.ValueUpdater.class);

        int numStateFields = (rowInfo.allColumns.size() + 15) / 16;

        cm.addField(proxyClass, "proxy").private_().final_();
        cm.addField(byte[].class, "dirtyValue").private_().final_();

        for (int i=0; i<numStateFields; i++) {
            cm.addField(int.class, "state$" + i).private_().final_();
        }

        MethodType ctorType;

        // Add the constructor.
        {
            var paramTypes = new Class[2 + numStateFields];

            paramTypes[0] = proxyClass;
            paramTypes[1] = byte[].class;

            for (int i=2; i<paramTypes.length; i++) {
                paramTypes[i] = int.class;
            }

            ctorType = MethodType.methodType(void.class, paramTypes);

            MethodMaker mm = cm.addConstructor(ctorType);
            mm.invokeSuperConstructor();

            mm.field("proxy").set(mm.param(0));
            mm.field("dirtyValue").set(mm.param(1));

            for (int i=0; i<numStateFields; i++) {
                mm.field("state$" + i).set(mm.param(2 + i));
            }
        }

        MethodMaker mm = cm.addMethod(byte[].class, "updateValue", byte[].class).public_();

        var originalValueVar = mm.param(0);

        var tableVar = mm.var(tableClass);
        Class rowClass = RowMaker.find(rowType);

        Variable schemaVersion;
        if (isEvolvable(tableClass, rowType)) {
            schemaVersion = mm.field("proxy").field("schemaVersion");
            TableMaker.convertValueIfNecessary(tableVar, rowClass, schemaVersion, originalValueVar);
        } else {
            schemaVersion = null;
        }

        var dirtyValueVar = mm.field("dirtyValue").get();

        // Note: The following code is similar to TableMaker.encodeUpdateEntry.

        // Identify the offsets to all the columns in the original and dirty entries, and
        // calculate the size of the new entry.

        Map<String, Integer> columnNumbers = rowGen.columnNumbers();
        ColumnCodec[] codecs = ColumnCodec.bind(rowGen.valueCodecs(), mm);

        Variable[] offsetVars = new Variable[codecs.length];

        var newSizeVar = dirtyValueVar.alength();
        var startOffsetVar = mm.var(int.class);

        if (schemaVersion != null) {
            startOffsetVar.set(mm.var(RowUtils.class).invoke("lengthPrefixPF", schemaVersion));
            newSizeVar.inc(startOffsetVar); // need room for schemaVersion
        } else {
            startOffsetVar.set(0);
        }

        var offsetVar = mm.var(int.class).set(startOffsetVar);

        int stateFieldNum = -1;
        Variable stateField = null;

        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            codec.encodePrepare();

            offsetVars[i] = offsetVar.get();
            codec.decodeSkip(originalValueVar, offsetVar, null);

            ColumnInfo info = codec.info;
            int num = columnNumbers.get(info.name);

            int sfNum = RowGen.stateFieldNum(num);
            if (sfNum != stateFieldNum) {
                stateFieldNum = sfNum;
                stateField = mm.field("state$" + sfNum).get();
            }

            int sfMask = RowGen.stateFieldMask(num);
            Label cont = mm.label();
            stateField.and(sfMask).ifEq(sfMask, cont);

            // Add in the size of original column, which won't be updated.
            codec.encodeSkip();
            newSizeVar.inc(offsetVar.sub(offsetVars[i]));

            cont.here();
        }

        // Encode the new byte[] entry...

        var newValueVar = mm.new_(byte[].class, newSizeVar);

        var srcOffsetVar = mm.var(int.class).set(0);
        var dstOffsetVar = mm.var(int.class).set(0);
        var dirtyOffsetVar = mm.var(int.class).set(0);
        var spanLengthVar = mm.var(int.class).set(startOffsetVar);
        var sysVar = mm.var(System.class);

        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            ColumnInfo info = codec.info;
            int num = columnNumbers.get(info.name);

            Variable columnLenVar;
            {
                Variable endVar;
                if (i + 1 < codecs.length) {
                    endVar = offsetVars[i + 1];
                } else {
                    endVar = originalValueVar.alength();
                }
                columnLenVar = endVar.sub(offsetVars[i]);
            }

            int sfNum = RowGen.stateFieldNum(num);
            if (sfNum != stateFieldNum) {
                stateFieldNum = sfNum;
                stateField = mm.field("state$" + sfNum).get();
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
                sysVar.invoke("arraycopy", originalValueVar, srcOffsetVar,
                              newValueVar, dstOffsetVar, spanLengthVar);
                srcOffsetVar.inc(spanLengthVar);
                dstOffsetVar.inc(spanLengthVar);
                spanLengthVar.set(0);
                noSpan.here();
            }

            // Copy the encoded dirty column.
            {
                Object dirtyStart = (i == 0) ? 0 : dirtyOffsetVar.get();
                codec.decodeSkip(dirtyValueVar, dirtyOffsetVar, null);
                var dirtyLengthVar = dirtyOffsetVar.sub(dirtyStart);
                sysVar.invoke("arraycopy", dirtyValueVar, dirtyStart,
                              newValueVar, dstOffsetVar, dirtyLengthVar);
                dstOffsetVar.inc(dirtyLengthVar);
            }

            // Skip over the original encoded column.
            srcOffsetVar.inc(columnLenVar);

            cont.here();
        }

        // Copy any remaining span.
        {
            Label noSpan = mm.label();
            spanLengthVar.ifEq(0, noSpan);
            sysVar.invoke("arraycopy", originalValueVar, srcOffsetVar,
                          newValueVar, dstOffsetVar, spanLengthVar);
            noSpan.here();
        }

        mm.return_(newValueVar);

        // In order for the factory to access private fields of the proxy class, it must be in
        // the same nest. The factory needs to be a hidden class to be in the same nest.
        lookup = cm.finishHidden();

        MethodHandle factory;
        try {
            factory = lookup.findConstructor(lookup.lookupClass(), ctorType);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }

        return factory.asType(ctorType.changeReturnType(StoredTable.ValueUpdater.class));
    }

    /**
     * @param variant "row" "step", "update" or "delete"
     */
    private void addUpdaterMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (Pipe.class, variant, RemoteUpdater.class, Pipe.class).public_();

        var updaterVar = updater(mm.param(0));
        var pipeVar = mm.param(1);

        Label tryStart = mm.label().here();

        var rowVar = mm.var(mRowClass);
        var currentRowVar = updaterVar.invoke("row");
        Label finish = mm.label();

        var makerVar = mm.var(RemoteProxyMaker.class);

        if (variant == "row") {
            rowVar.set(currentRowVar.cast(mRowClass));
        } else if (variant == "step") {
            makerVar.invoke("currentRowCheck", currentRowVar);
            rowVar.set(updaterVar.invoke(variant, currentRowVar).cast(mRowClass));
        } else {
            makerVar.invoke("currentRowCheck", currentRowVar);
            rowVar.set(mm.invoke("decodeRow", currentRowVar.cast(mRowClass), pipeVar));
            rowVar.ifEq(null, finish);
            rowVar.set(updaterVar.invoke(variant, rowVar).cast(mRowClass));
        }

        Label noRow = mm.label();
        rowVar.ifEq(null, noRow);

        var keyBytesVar = mm.invoke("encodeKeyColumns", rowVar);
        var valueBytesVar = mm.invoke("encodeValueColumns", rowVar, false);

        mm.catch_(tryStart, Throwable.class, exVar -> {
            pipeVar.invoke("writeObject", exVar);
            finish.goto_();
        });

        pipeVar.invoke("writeNull"); // no exception
        keyBytesVar.aset(0, 1); // set the result code
        pipeVar.invoke("write", keyBytesVar);
        pipeVar.invoke("write", valueBytesVar);

        // Write the column states, but the client/server state slots can vary.

        String[] rowStateFieldNames = mRowGen.stateFields();
        Map<String, Integer> rowColumnNumbers = mRowGen.columnNumbers();

        ColumnCodec[] clientCodecs = clientCodecs();
        var clientStateVar = mm.var(int.class).set(0);

        for (int columnNum = 0; columnNum < clientCodecs.length; columnNum++) {
            if (columnNum > 0 && (columnNum & 0b1111) == 0) {
                pipeVar.invoke("writeInt", clientStateVar);
                clientStateVar.set(0);
            }

            Integer rowColumnNumObj = rowColumnNumbers.get(mRowHeader.columnNames[columnNum]);

            if (rowColumnNumObj == null) {
                continue;
            }

            int rowColumnNum = rowColumnNumObj;

            int stateFieldNum = RowGen.stateFieldNum(rowColumnNum);
            int stateFieldMask = RowGen.stateFieldMask(rowColumnNum);

            var rowStateVar = rowVar.field(rowStateFieldNames[stateFieldNum]).and(stateFieldMask);

            int stateShiftRight = RowGen.stateFieldShift(rowColumnNum)
                - RowGen.stateFieldShift(columnNum);

            if (stateShiftRight > 0) {
                rowStateVar.set(rowStateVar.ushr(stateShiftRight));
            } else if (stateShiftRight != 0) {
                rowStateVar.set(rowStateVar.shl(-stateShiftRight));
            }

            clientStateVar.set(clientStateVar.or(rowStateVar));
        }

        pipeVar.invoke("writeInt", clientStateVar);

        finish.here();

        pipeVar.invoke("flush");
        pipeVar.invoke("recycle");

        mm.return_(null);

        noRow.here();
        pipeVar.invoke("writeNull"); // no exception
        pipeVar.invoke("writeByte", 0); // result code
        finish.goto_();

        var exVar = mm.catch_(tryStart, mm.label().here(), Throwable.class);
        makerVar.invoke("handleException", exVar, pipeVar, updaterVar);
        mm.return_(null);
    }

    /**
     * Extracts the real transaction object from the remote transaction.
     */
    private Variable txn(Variable remoteTxnVar) {
        return remoteTxnVar.methodMaker().var(ServerTransaction.class).invoke("txn", remoteTxnVar);
    }

    /**
     * Extracts the real updater object from the remote updater.
     */
    private Variable updater(Variable remoteUpdaterVar) {
        return remoteUpdaterVar.methodMaker()
            .var(ServerUpdater.class).invoke("updater", remoteUpdaterVar);
    }

    /**
     * Reads one or more int variables.
     */
    private Variable[] readStateFields(Variable pipeVar) {
        var stateVars = new Variable[(mRowHeader.columnNames.length + 15) / 16];
        for (int i=0; i<stateVars.length; i++) {
            stateVars[i] = pipeVar.invoke("readInt");
        }
        return stateVars;
    }

    /**
     * Generates code which checks if all of the given columns are set, storing a boolean
     * result in the returned variable.
     */
    private Variable checkSet(MethodMaker mm, Map<String, ColumnInfo> columns,
                              Variable[] stateVars)
    {
        var allSetVar = mm.var(boolean.class);
        mRowGen.checkSet(mm, columns, allSetVar, num -> stateVars[num >> 4]);
        return allSetVar;
    }

    /**
     * Generates code which checks if all of the given columns are dirty, storing a boolean
     * result in the returned variable.
     */
    private Variable checkDirty(MethodMaker mm, Map<String, ColumnInfo> columns,
                                Variable[] stateVars)
    {
        var allDirtyVar = mm.var(boolean.class);
        mRowGen.checkDirty(mm, columns, allDirtyVar, num -> stateVars[num >> 4]);
        return allDirtyVar;
    }

    /**
     * Convert the value to the current schema if necessary, and then call writeValue.
     */
    private void convertAndWriteValue(Variable pipeVar, Variable valueVar) {
        if (isEvolvable()) {
            MethodMaker mm = pipeVar.methodMaker();
            var tableVar = mm.var(mTable.getClass());
            var schemaVersion = mm.field("schemaVersion");
            TableMaker.convertValueIfNecessary(tableVar, mRowClass, schemaVersion, valueVar);
        }

        writeValue(pipeVar, valueVar);
    }

    /**
     * Write the value sans the schema version. The total length of the value (sans schema
     * version) is written first using the prefix format.
     */
    private void writeValue(Variable pipeVar, Variable valueVar) {
        MethodMaker mm = pipeVar.methodMaker();
        if (isEvolvable()) {
            var prefixLengthVar = mm.field("prefixLength").get();
            var lengthVar = valueVar.alength().sub(prefixLengthVar);
            mm.var(RowUtils.class).invoke("encodePrefixPF", pipeVar, lengthVar);
            pipeVar.invoke("write", valueVar, prefixLengthVar, lengthVar);
        } else {
            mm.var(RowUtils.class).invoke("encodePrefixPF", pipeVar, valueVar.alength());
            pipeVar.invoke("write", valueVar);
        }
    }

    /**
     * Called by generated code.
     */
    public static void skipKeyAndValue(Pipe pipe) throws IOException {
        pipe.skip(RowUtils.decodePrefixPF(pipe));
        pipe.skip(RowUtils.decodePrefixPF(pipe));
    }

    /**
     * Called by generated code.
     */
    public static void skipValue(Pipe pipe) throws IOException {
        pipe.skip(RowUtils.decodePrefixPF(pipe));
    }

    /**
     * Called by generated code.
     */
    public static byte[] decodeKey(Pipe pipe) throws IOException {
        return decodeValue(pipe);
    }

    /**
     * Called by generated code.
     *
     * @param tailLen extra bytes to allocate at the end of the returned key
     */
    public static byte[] decodeKey(Pipe pipe, int tailLen) throws IOException {
        int length = RowUtils.decodePrefixPF(pipe);
        if (length == 0) {
            return new byte[tailLen];
        }
        var bytes = new byte[length + tailLen];
        pipe.readFully(bytes, 0, length);
        return bytes;
    }

    /**
     * Called by generated code.
     */
    public static byte[] decodeValue(Pipe pipe) throws IOException {
        int length = RowUtils.decodePrefixPF(pipe);
        if (length == 0) {
            return RowUtils.EMPTY_BYTES;
        }
        var bytes = new byte[length];
        pipe.readFully(bytes);
        return bytes;
    }

    /**
     * Called by generated code.
     *
     * @param prefixLength space to allocate at the beginning of the returned byte array,
     * intended to be used for encoding the schemaVersion
     */
    public static byte[] decodeValue(Pipe pipe, int prefixLength) throws IOException {
        int valueLength = RowUtils.decodePrefixPF(pipe);
        var bytes = new byte[prefixLength + valueLength];
        pipe.readFully(bytes, prefixLength, valueLength);
        return bytes;
    }

    /**
     * Called by generated code.
     *
     * @return the loaded value; if non-null, the caller must write the response, etc.
     */
    public static byte[] tryLoad(StoredTable table, Transaction txn, byte[] key, Pipe pipe)
        throws IOException
    {
        attempt: {
            byte[] value;
            try {
                value = table.mSource.load(txn, key);
            } catch (Throwable e) {
                pipe.writeObject(e);
                break attempt;
            }
            pipe.writeNull(); // no exception
            if (value == null) {
                pipe.writeByte(0);
            } else {
                pipe.writeByte(1);
                return value;
            }
        }

        pipe.flush();
        pipe.recycle();

        return null;
    }

    /**
     * Called by generated code.
     */
    public static void exists(StoredTable table, Transaction txn, byte[] key, Pipe pipe)
        throws IOException
    {
        attempt: {
            boolean result;
            try {
                result = table.mSource.exists(txn, key);
            } catch (Throwable e) {
                pipe.writeObject(e);
                break attempt;
            }
            pipe.writeNull(); // no exception
            writeResult(result, pipe);
        }

        pipe.flush();
        pipe.recycle();
    }

    /**
     * Called by generated code.
     */
    @SuppressWarnings("unchecked")
    public static void store(StoredTable table, Transaction txn, Object row,
                             byte[] key, byte[] value, Pipe pipe)
        throws IOException
    {
        attempt: {
            try {
                table.storeAndTrigger(txn, row, key, value);
            } catch (Throwable e) {
                pipe.writeObject(e);
                break attempt;
            }
            pipe.writeNull(); // no exception
            pipe.writeByte(1); // success
        }

        pipe.flush();
        pipe.recycle();
    }

    /**
     * Called by generated code.
     *
     * @return the old value; if non-null, the caller must write the response, etc.
     */
    @SuppressWarnings("unchecked")
    public static byte[] exchange(StoredTable table, Transaction txn, Object row,
                                  byte[] key, byte[] value, Pipe pipe)
        throws IOException
    {
        attempt: {
            byte[] oldValue;
            try {
                oldValue = table.exchangeAndTrigger(txn, row, key, value);
            } catch (Throwable e) {
                pipe.writeObject(e);
                break attempt;
            }
            pipe.writeNull(); // no exception
            if (oldValue == null) {
                pipe.writeByte(0);
            } else {
                pipe.writeByte(1);
                return oldValue;
            }
        }

        pipe.flush();
        pipe.recycle();

        return null;
    }

    /**
     * Called by generated code.
     */
    @SuppressWarnings("unchecked")
    public static void tryInsert(StoredTable table, Transaction txn, Object row,
                                 byte[] key, byte[] value, Pipe pipe)
        throws IOException
    {
        attempt: {
            boolean result;
            try {
                result = table.insertAndTrigger(txn, row, key, value);
            } catch (Throwable e) {
                pipe.writeObject(e);
                break attempt;
            }
            pipe.writeNull(); // no exception
            writeResult(result, pipe);
        }

        pipe.flush();
        pipe.recycle();
    }

    /**
     * Called by generated code.
     */
    @SuppressWarnings("unchecked")
    public static void tryReplace(StoredTable table, Transaction txn, Object row,
                                  byte[] key, byte[] value, Pipe pipe)
        throws IOException
    {
        attempt: {
            boolean result;
            try {
                result = table.replaceAndTrigger(txn, row, key, value);
            } catch (Throwable e) {
                pipe.writeObject(e);
                break attempt;
            }
            pipe.writeNull(); // no exception
            writeResult(result, pipe);
        }

        pipe.flush();
        pipe.recycle();
    }

    /**
     * Called by generated code.
     */
    @SuppressWarnings("unchecked")
    public static void tryUpdate(StoredTable table, Transaction txn, Object row,
                                 byte[] key, StoredTable.ValueUpdater updater, Pipe pipe)
        throws IOException
    {
        attempt: {
            byte[] newValue;
            try {
                newValue = table.updateAndTrigger(txn, row, key, updater);
            } catch (Throwable e) {
                pipe.writeObject(e);
                break attempt;
            }
            pipe.writeNull(); // no exception
            pipe.writeByte(newValue != null ? 1 : 0); // success or failure
        }

        pipe.flush();
        pipe.recycle();
    }

    /**
     * Called by generated code.
     *
     * @return the new value; if non-null, the caller must write the response, etc.
     */
    @SuppressWarnings("unchecked")
    public static byte[] tryMerge(StoredTable table, Transaction txn, Object row,
                                  byte[] key, StoredTable.ValueUpdater updater, Pipe pipe)
        throws IOException
    {
        attempt: {
            byte[] newValue;
            try {
                newValue = table.updateAndTrigger(txn, row, key, updater);
            } catch (Throwable e) {
                pipe.writeObject(e);
                break attempt;
            }
            pipe.writeNull(); // no exception
            if (newValue == null) {
                pipe.writeByte(0);
            } else {
                pipe.writeByte(1);
                return newValue;
            }
        }

        pipe.flush();
        pipe.recycle();

        return null;
    }

    /**
     * Called by generated code.
     *
     * @return true if the caller must write the response, etc.
     */
    @SuppressWarnings("unchecked")
    public static boolean mergeReplace(StoredTable table, Transaction txn, Object row,
                                       byte[] key, byte[] value, Pipe pipe)
        throws IOException
    {
        attempt: {
            boolean result;
            try {
                result = table.replaceAndTrigger(txn, row, key, value);
            } catch (Throwable e) {
                pipe.writeObject(e);
                break attempt;
            }
            pipe.writeNull(); // no exception
            if (!result) {
                pipe.writeByte(0);
            } else {
                pipe.writeByte(1);
                return true;
            }
        }

        pipe.flush();
        pipe.recycle();

        return false;
    }

    /**
     * Called by generated code.
     */
    public static void tryDelete(StoredTable table, Transaction txn, byte[] key, Pipe pipe)
        throws IOException
    {
        attempt: {
            boolean result;
            try {
                result = table.deleteAndTrigger(txn, key);
            } catch (Throwable e) {
                pipe.writeObject(e);
                break attempt;
            }
            pipe.writeNull(); // no exception
            writeResult(result, pipe);
        }

        pipe.flush();
        pipe.recycle();
    }

    /**
     * Called by generated code.
     */
    @SuppressWarnings("unchecked")
    public static void storeAuto(StoredTable table, AutomaticKeyGenerator autogen,
                                 Transaction txn, Object row, byte[] key, byte[] value, Pipe pipe)
        throws IOException
    {
        attempt: {
            try {
                key = table.storeAutoAndTrigger(autogen, txn, row, key, value);
            } catch (Throwable e) {
                pipe.writeObject(e);
                break attempt;
            }
            pipe.writeNull(); // no exception
            pipe.writeByte(2); // success; code 2 indicates that the key tail is written
            autogen.writeTail(key, pipe);
        }

        pipe.flush();
        pipe.recycle();
    }

    /**
     * Called by generated code.
     */
    public static void writeResult(boolean result, Pipe pipe) throws IOException {
        pipe.writeByte(result ? 1 : 0); // success or failure
    }

    /**
     * Called by generated code.
     */
    public static void handleException(Throwable e, Pipe pipe) {
        RowUtils.closeQuietly(pipe);
        if (!(e instanceof IOException)) {
            RowUtils.rethrow(e);
        }
    }

    /**
     * Called by generated code.
     */
    public static void handleException(Throwable e, Pipe pipe, Closeable c) {
        RowUtils.closeQuietly(pipe);
        RowUtils.closeQuietly(c);
        if (!(e instanceof IOException)) {
            RowUtils.rethrow(e);
        }
    }

    /**
     * Called by generated code.
     */
    public static void currentRowCheck(Object row) {
        if (row == null) {
            throw new IllegalStateException();
        }
    }

    /**
     * Makes a RemoteTableProxy class which must perform column conversions.
     */
    private MethodHandles.Lookup makeConverter() {
        addDecodeRow();
        addEncodeColumns(false);

        addByKeyConvertMethod("tryLoad");
        addByKeyConvertMethod("exists");
        addByKeyConvertMethod("tryDelete");

        addStoreConvertMethod("store");
        addStoreConvertMethod("exchange");
        addStoreConvertMethod("tryInsert");
        addStoreConvertMethod("tryReplace");

        addUpdateConvertMethod("tryUpdate");
        addUpdateConvertMethod("tryMerge");

        // The encodeKeyColumns method is needed by the Updater methods.
        addEncodeColumns(true);

        addUpdaterMethod("row");
        addUpdaterMethod("step");
        addUpdaterMethod("update");
        addUpdaterMethod("delete");

        return mClassMaker.finishLookup();
    }

    /**
     * @param variant "tryLoad", "exists", or "tryDelete"
     */
    private void addByKeyConvertMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (Pipe.class, variant, RemoteTransaction.class, Pipe.class).public_();

        var txnVar = txn(mm.param(0));
        var pipeVar = mm.param(1);

        Label tryStart = mm.label().here();

        var rowVar = mm.invoke("decodeRow", mm.new_(mRowClass), pipeVar);
        Label finish = mm.label();
        rowVar.ifEq(null, finish);

        Label opTryStart = mm.label().here();

        var resultVar = mm.field("table").invoke(variant, txnVar, rowVar);

        var makerVar = mm.var(RemoteProxyMaker.class);

        if (variant != "tryLoad") {
            mm.catch_(opTryStart, Throwable.class, exVar -> {
                pipeVar.invoke("writeObject", exVar);
                finish.goto_();
            });

            pipeVar.invoke("writeNull"); // no exception

            makerVar.invoke("writeResult", resultVar, pipeVar);
        } else {
            Label noOperation = mm.label();
            resultVar.ifFalse(noOperation);

            var bytesVar = mm.invoke("encodeValueColumns", rowVar, true);

            mm.catch_(opTryStart, Throwable.class, exVar -> {
                pipeVar.invoke("writeObject", exVar);
                finish.goto_();
            });

            pipeVar.invoke("writeNull"); // no exception
            bytesVar.aset(0, 1); // set the result code

            pipeVar.invoke("write", bytesVar);
            finish.goto_();

            noOperation.here();
            pipeVar.invoke("writeNull"); // no exception
            pipeVar.invoke("writeByte", 0);
        }

        finish.here();

        pipeVar.invoke("flush");
        pipeVar.invoke("recycle");

        mm.return_(null);

        var exVar = mm.catch_(tryStart, mm.label().here(), Throwable.class);
        makerVar.invoke("handleException", exVar, pipeVar);
        mm.return_(null);
    }

    /**
     * @param variant "store", "exchange", "tryInsert", or "tryReplace"
     */
    private void addStoreConvertMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (Pipe.class, variant, RemoteTransaction.class, Pipe.class).public_();

        var txnVar = txn(mm.param(0));
        var pipeVar = mm.param(1);

        Label tryStart = mm.label().here();

        var rowVar = mm.invoke("decodeRow", mm.new_(mRowClass), pipeVar);
        Label finish = mm.label();
        rowVar.ifEq(null, finish);

        Variable isAutoVar = null;

        if (variant != "tryReplace" && mAutoColumn != null) {
            // Check if the automatic column is unset -- the isAutoVar value will be zero.
            int columnNum = mRowGen.info.keyColumns.size() - 1;
            int mask = RowGen.stateFieldMask(columnNum);
            isAutoVar = rowVar.field(mRowGen.stateField(columnNum)).and(mask);
        }

        Label opTryStart = mm.label().here();

        var makerVar = mm.var(RemoteProxyMaker.class);

        if (variant != "exchange") {
            var resultVar = mm.field("table").invoke(variant, txnVar, rowVar);

            mm.catch_(opTryStart, Throwable.class, exVar -> {
                pipeVar.invoke("writeObject", exVar);
                finish.goto_();
            });

            pipeVar.invoke("writeNull"); // no exception

            autoCheck(isAutoVar, rowVar, pipeVar, finish);

            if (variant == "store") {
                pipeVar.invoke("writeByte", 1); // success
            } else {
                makerVar.invoke("writeResult", resultVar, pipeVar);
            }
        } else {
            // Enter a transaction scope to roll back the operation if the call to
            // encodeValueColumns throws an exception.
            txnVar.set(mm.field("table").invoke("enterScope", txnVar));
            Label txnStart = mm.label().here();

            var resultVar = mm.field("table").invoke(variant, txnVar, rowVar);

            final Variable bytesVar = mm.var(byte[].class);

            Label hasOldRow = mm.label();
            resultVar.ifNe(null, hasOldRow);
            bytesVar.set(null);
            Label cont = mm.label().goto_();
            hasOldRow.here();
            bytesVar.set(mm.invoke("encodeValueColumns", resultVar.cast(mRowClass), true));
            cont.here();

            txnVar.invoke("commit");

            mm.finally_(txnStart, () -> txnVar.invoke("exit"));

            mm.catch_(opTryStart, Throwable.class, exVar -> {
                pipeVar.invoke("writeObject", exVar);
                finish.goto_();
            });

            pipeVar.invoke("writeNull"); // no exception

            autoCheck(isAutoVar, rowVar, pipeVar, finish);

            Label hasBytes = mm.label();
            bytesVar.ifNe(null, hasBytes);
            pipeVar.invoke("writeByte", 0);
            finish.goto_();
            hasBytes.here();
            bytesVar.aset(0, 1); // set the result code
            pipeVar.invoke("write", bytesVar);
        }

        finish.here();

        pipeVar.invoke("flush");
        pipeVar.invoke("recycle");

        mm.return_(null);

        var exVar = mm.catch_(tryStart, mm.label().here(), Throwable.class);
        makerVar.invoke("handleException", exVar, pipeVar);
        mm.return_(null);
    }

    /**
     * Makes code which checks if an automatic column was assigned. If so, the column value is
     * written to the pipe and then the code jumps to the finish label.
     */
    private void autoCheck(Variable isAutoVar, Variable rowVar, Variable pipeVar, Label finish) {
        if (isAutoVar != null) {
            MethodMaker mm = isAutoVar.methodMaker();

            Label noAutoKey = mm.label();
            isAutoVar.ifNe(0, noAutoKey);

            // success; code 2 indicates that the key tail is written
            pipeVar.invoke("writeByte", 2);

            var columnVar = rowVar.field(mAutoColumn.name);

            if (mAutoColumn.type == int.class) {
                pipeVar.invoke("writeInt", columnVar);
            } else {
                pipeVar.invoke("writeLong", columnVar);
            }

            finish.goto_();
            noAutoKey.here();
        }
    }

    /**
     * @param variant "tryUpdate" or "tryMerge"
     */
    private void addUpdateConvertMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (Pipe.class, variant, RemoteTransaction.class, Pipe.class).public_();

        var txnVar = txn(mm.param(0));
        var pipeVar = mm.param(1);

        Label tryStart = mm.label().here();

        var rowVar = mm.invoke("decodeRow", mm.new_(mRowClass), pipeVar);
        Label finish = mm.label();
        rowVar.ifEq(null, finish);

        Label opTryStart = mm.label().here();

        var makerVar = mm.var(RemoteProxyMaker.class);

        if (variant == "tryUpdate") {
            var resultVar = mm.field("table").invoke(variant, txnVar, rowVar);

            mm.catch_(opTryStart, Throwable.class, exVar -> {
                pipeVar.invoke("writeObject", exVar);
                finish.goto_();
            });

            pipeVar.invoke("writeNull"); // no exception
            makerVar.invoke("writeResult", resultVar, pipeVar);
        } else {
            // Enter a transaction scope to roll back the operation if the call to
            // encodeValueColumns throws an exception.
            txnVar.set(mm.field("table").invoke("enterScope", txnVar));
            Label txnStart = mm.label().here();

            var resultVar = mm.field("table").invoke(variant, txnVar, rowVar);

            Label noOperation = mm.label();
            resultVar.ifFalse(noOperation);

            var bytesVar = mm.invoke("encodeValueColumns", rowVar.cast(mRowClass), true);

            txnVar.invoke("commit");

            mm.finally_(txnStart, () -> txnVar.invoke("exit"));

            mm.catch_(opTryStart, Throwable.class, exVar -> {
                pipeVar.invoke("writeObject", exVar);
                finish.goto_();
            });

            pipeVar.invoke("writeNull"); // no exception
            bytesVar.aset(0, 1); // set the result code

            pipeVar.invoke("write", bytesVar);
            finish.goto_();

            noOperation.here();
            pipeVar.invoke("writeNull"); // no exception
            pipeVar.invoke("writeByte", 0);
        }

        finish.here();

        pipeVar.invoke("flush");
        pipeVar.invoke("recycle");

        mm.return_(null);

        var exVar = mm.catch_(tryStart, mm.label().here(), Throwable.class);
        makerVar.invoke("handleException", exVar, pipeVar);
        mm.return_(null);
    }

    /**
     * Defines a static method which accepts a Pipe and returns a row object as written by the
     * client. The first parameter is the row object to fill in, which can be a new instance.
     * The second parameter is the pipe.
     *
     * The decodeRow method reads the column states and column values from the pipe. If a
     * conversion exception occurs, the exception is written to the pipe and null is returned.
     * The caller must then flush and recycle the pipe.
     */
    private void addDecodeRow() {
        MethodMaker mm = mClassMaker.addMethod(mRowClass, "decodeRow", mRowClass, Pipe.class);
        mm.private_().static_();

        var rowVar = mm.param(0);
        var pipeVar = mm.param(1);
        var stateVars = readStateFields(pipeVar);

        var makerVar = mm.var(RemoteProxyMaker.class);
        var keyVar = makerVar.invoke("decodeKey", pipeVar);
        var valueVar = makerVar.invoke("decodeValue", pipeVar);

        Map<String, Integer> rowColumnNumbers = mRowGen.columnNumbers();
        String[] rowStateFieldNames = mRowGen.stateFields();
        ColumnCodec[] rowKeyCodecs = mRowGen.keyCodecs();
        ColumnCodec[] rowValueCodecs = mRowGen.valueCodecs();

        var offsetVar = mm.var(int.class);

        ColumnCodec[] clientCodecs = clientCodecs();

        for (int columnNum = 0; columnNum < clientCodecs.length; columnNum++) {
            if (columnNum == 0 || columnNum == mRowHeader.numKeys) {
                offsetVar.set(0);
            }

            int stateFieldNum = RowGen.stateFieldNum(columnNum);
            int stateFieldMask = RowGen.stateFieldMask(columnNum);

            var stateVar = stateVars[stateFieldNum].and(stateFieldMask);
            Label skip = mm.label();
            stateVar.ifEq(0, skip);

            Variable bytesVar;
            if (columnNum < mRowHeader.numKeys) {
                bytesVar = keyVar;
            } else {
                bytesVar = valueVar;
            }

            ColumnCodec codec = clientCodecs[columnNum].bind(mm);

            var columnVar = mm.var(codec.info.type);

            codec.decode(columnVar, bytesVar, offsetVar, null);

            String rowFieldName = mRowHeader.columnNames[columnNum];

            if (!rowColumnNumbers.containsKey(rowFieldName)) {
                var exVar = mm.new_(IllegalStateException.class, "Unknown column: " + rowFieldName);
                pipeVar.invoke("writeObject", exVar);
                mm.return_(null);
            } else {
                var rowField = rowVar.field(rowFieldName);
                int rowColumnNum = rowColumnNumbers.get(rowFieldName);

                ColumnCodec rowFieldCodec;
                if (rowColumnNum < rowKeyCodecs.length) {
                    rowFieldCodec = rowKeyCodecs[rowColumnNum];
                } else {
                    rowFieldCodec = rowValueCodecs[rowColumnNum - rowKeyCodecs.length];
                }

                Label tryStart = mm.label().here();

                Converter.convertExact(mm, rowFieldName,
                                       codec.info, columnVar, rowFieldCodec.info, rowField);
 
                mm.catch_(tryStart, RuntimeException.class, exVar -> {
                    pipeVar.invoke("writeObject", exVar);
                    mm.return_(null);
                });

                int stateShiftLeft = RowGen.stateFieldShift(rowColumnNum)
                    - RowGen.stateFieldShift(columnNum);

                if (stateShiftLeft > 0) {
                    stateVar.set(stateVar.shl(stateShiftLeft));
                } else if (stateShiftLeft != 0) {
                    stateVar.set(stateVar.ushr(-stateShiftLeft));
                }

                int rowStateFieldNum = RowGen.stateFieldNum(rowColumnNum);
                var rowStateField = rowVar.field(rowStateFieldNames[rowStateFieldNum]);

                if (columnNum == 0) {
                    rowStateField.set(stateVar);
                } else {
                    int mask = ~RowGen.stateFieldMask(rowColumnNum);
                    rowStateField.set(rowStateField.and(mask).or(stateVar));
                }
            }

            skip.here();
        }

        mm.return_(rowVar);
    }

    /**
     * Defines a static method which accepts a row object and encodes key or value columns into
     * a new byte array which can be decoded by the client. If an exact conversion isn't
     * possible, a RuntimeException is thrown.
     *
     * <p>A second parameter is defined for the encodeValueColumns which when true, the encoder
     * throws an exception if the client provides a column unknown to the server. When false,
     * such a column is simply skipped over. The encodeKeyColumns is always strict.
     *
     * <p>The first byte of the returned byte array empty, and is expected to be set by the
     * caller with an operation result code.
     */
    private void addEncodeColumns(boolean forKey) {
        MethodMaker mm;
        if (forKey) {
            mm = mClassMaker.addMethod(byte[].class, "encodeKeyColumns", mRowClass);
        } else {
            mm = mClassMaker.addMethod(byte[].class, "encodeValueColumns", mRowClass,
                                       boolean.class);
        }

        mm.private_().static_();

        ColumnCodec[] clientCodecs = clientCodecs();

        if (clientCodecs.length == 0) {
            var emptyVar = mm.var(byte[].class).setExact(new byte[1]);
            mm.return_(emptyVar);
            return;
        }

        var rowVar = mm.param(0);
        var strictVar = forKey ? null : mm.param(1);

        // Prepare all the fields by applying any necessary conversions.

        int numStart, numEnd;
        if (forKey) {
            numStart = 0;
            numEnd = mRowHeader.numKeys;
        } else {
            numStart = mRowHeader.numKeys;
            numEnd = clientCodecs.length;
        }

        Label beginEncode = mm.label();
        if (strictVar != null) {
            strictVar.ifFalse(beginEncode);
        }

        // If the client provides a column unknown to the server, throw an exception.
        for (int columnNum = numStart; columnNum < numEnd; columnNum++) {
            String fieldName = mRowHeader.columnNames[columnNum];
            if (!mRowGen.info.allColumns.containsKey(fieldName)) {
                mm.new_(IllegalStateException.class, "Unknown column: " + fieldName).throw_();
                if (strictVar == null) { // always strict
                    return;
                }
            }
        }

        beginEncode.here();

        clientCodecs = ColumnCodec.bind(clientCodecs, mm);

        var fieldVars = new Variable[clientCodecs.length - numStart];

        for (int columnNum = numStart; columnNum < numEnd; columnNum++) {
            String fieldName = mRowHeader.columnNames[columnNum];
            ColumnInfo dstInfo = clientCodecs[columnNum].info;
            var dstVar = mm.var(dstInfo.type);
            fieldVars[columnNum - numStart] = dstVar;
            ColumnInfo srcInfo = mRowGen.info.allColumns.get(fieldName);

            if (srcInfo == null) {
                // The server doesn't have the column, so supply a default value instead. The
                // client will ignore the column anyhow because the column state it receives
                // should be UNSET. See the addUpdaterMethod method.
                Converter.setDefault(mm, dstInfo, dstVar);
                continue;
            }

            var srcField = rowVar.field(fieldName);

            Label cont = null;

            if (!srcInfo.isPrimitive() && !srcInfo.isNullable()) {
                // If the source field can be null (because it's unset), supply a default value
                // to prevent a NPE. Technically this is a lossy conversion, but the client
                // will ignore the column because the column state it receives should be UNSET.
                Label notNull = mm.label();
                srcField.ifNe(null, notNull);
                Converter.setDefault(mm, dstInfo, dstVar);
                cont = mm.label().goto_();
                notNull.here();
            }

            Converter.convertExact(mm, fieldName, srcInfo, srcField, dstInfo, dstVar);

            if (cont != null) {
                cont.here();
            }
        }

        // Calculate the value encoding length.

        Variable valueLengthVar = null;
        int minSize = 0;

        for (int columnNum = numStart; columnNum < numEnd; columnNum++) {
            var fieldVar = fieldVars[columnNum - numStart];
            ColumnCodec codec = clientCodecs[columnNum];
            codec.encodePrepare();
            valueLengthVar = codec.encodeSize(fieldVar, valueLengthVar);
            minSize += codec.minSize();
        }

        if (valueLengthVar == null) {
            valueLengthVar = mm.var(int.class).set(minSize);
        } else if (minSize != 0) {
            valueLengthVar.inc(minSize);
        }

        var utilsVar = mm.var(RowUtils.class);
        var fullLengthVar = mm.var(int.class).set(1); // for the result code
        fullLengthVar.inc(utilsVar.invoke("lengthPrefixPF", valueLengthVar));
        fullLengthVar.inc(valueLengthVar);

        // Allocate the array and encode into it.

        var bytesVar = mm.new_(byte[].class, fullLengthVar);
        var offsetVar = utilsVar.invoke("encodePrefixPF", bytesVar, 1, valueLengthVar);

        for (int columnNum = numStart; columnNum < numEnd; columnNum++) {
            var fieldVar = fieldVars[columnNum - numStart];
            clientCodecs[columnNum].encode(fieldVar, bytesVar, offsetVar);
        }

        Label cont = mm.label();
        mm.field("assert").ifFalse(cont);
        offsetVar.ifEq(bytesVar.alength(), cont);
        mm.new_(AssertionError.class,
                mm.concat(offsetVar, " != ", bytesVar.alength()), null).throw_();
        cont.here();

        mm.return_(bytesVar);
    }

    private ColumnCodec[] clientCodecs() {
        ColumnCodec[] codecs = mClientCodecs;
        if (codecs == null) {
            mClientCodecs = codecs = ColumnCodec.make(mRowHeader);
        }
        return codecs;
    }
}
