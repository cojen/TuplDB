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

package org.cojen.tupl.rows;

import java.io.IOException;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.Map;

import org.cojen.dirmi.Pipe;

import org.cojen.maker.Bootstrap;
import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Transaction;

import org.cojen.tupl.remote.RemoteTableProxy;
import org.cojen.tupl.remote.RemoteTransaction;
import org.cojen.tupl.remote.ServerTransaction;

/**
 * Generates classes which are used by the remote server.
 *
 * @author Brian S O'Neill
 */
public final class RemoteProxyMaker {
    private record CacheKey(Class<?> tableClass, byte[] descriptor) { }

    private static final SoftCache<CacheKey, MethodHandle, BaseTable> cCache = new SoftCache<>() {
        @Override
        protected MethodHandle newValue(CacheKey key, BaseTable table) {
            return new RemoteProxyMaker(table, key.descriptor).finish();
        }
    };

    /**
     * @param descriptor encoding format is defined by RowHeader class
     */
    static RemoteTableProxy make(BaseTable<?> table, int schemaVersion, byte[] descriptor) {
        var mh = cCache.obtain(new CacheKey(table.getClass(), descriptor), table);
        try {
            return (RemoteTableProxy) mh.invoke(table, schemaVersion);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private final Class<?> mTableClass;
    private final Class<?> mRowType;
    private final RowGen mRowGen;
    private final RowHeader mRowHeader;
    private final ClassMaker mClassMaker;

    private Variable mUpdaterFactoryVar;

    private RemoteProxyMaker(BaseTable table, byte[] descriptor) {
        mTableClass = table.getClass();
        mRowType = table.rowType();
        mRowGen = RowInfo.find(mRowType).rowGen();
        mRowHeader = RowHeader.decode(descriptor);
        mClassMaker = mRowGen.beginClassMaker(RemoteProxyMaker.class, mRowType, null);
        mClassMaker.implement(RemoteTableProxy.class);
    }

    /**
     * Returns a factory MethodHandle which constructs a RemoteTableProxy:
     *
     *     RemoteTableProxy new(BaseTable table, int schemaVersion);
     */
    private MethodHandle finish() {
        // Define a singleton empty row which is needed by the trigger methods.
        {
            Class rowClass = RowMaker.find(mRowType);
            mClassMaker.addField(rowClass, "EMPTY_ROW").private_().static_().final_();
            MethodMaker mm = mClassMaker.addClinit();
            mm.field("EMPTY_ROW").set(mm.new_(rowClass));
        }


        mClassMaker.addField(BaseTable.class, "table").private_().final_();

        MethodMaker ctorMaker = mClassMaker.addConstructor(BaseTable.class, int.class).private_();
        ctorMaker.invokeSuperConstructor();
        ctorMaker.field("table").set(ctorMaker.param(0));

        MethodHandles.Lookup lookup;

        if (RowHeader.make(mRowGen).equals(mRowHeader)) {
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

            lookup = makeDirect();
        } else {
            lookup = makeConverter();
        }

        try {
            MethodType mt = MethodType.methodType(void.class, BaseTable.class, int.class);
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

        addByKeyDirectMethod("load");
        addByKeyDirectMethod("exists");
        addByKeyDirectMethod("delete");

        addStoreDirectMethod("store");
        addStoreDirectMethod("exchange");
        addStoreDirectMethod("insert");
        addStoreDirectMethod("replace");

        addUpdateDirectMethod("update");
        addUpdateDirectMethod("merge");

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
        var readyVar = mm.var(boolean.class);
        mRowGen.checkSet(mm, mRowGen.info.keyColumns, readyVar, num -> stateVars[num >> 4]);

        Label isReady = mm.label();
        readyVar.ifTrue(isReady);

        mm.var(RemoteProxyMaker.class).invoke("skipKeyAndValue", pipeVar);
        var iseVar = mm.new_(IllegalStateException.class, "Primary key isn't fully specified");
        pipeVar.invoke("writeObject", iseVar);
        pipeVar.invoke("flush");
        pipeVar.invoke("recycle");
        mm.return_(null);

        isReady.here();
    }

    /**
     * @param variant "load", "exists", or "delete"
     */
    private void addByKeyDirectMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (Pipe.class, variant, RemoteTransaction.class, Pipe.class).public_();

        var txnVar = txn(mm.param(0));
        var pipeVar = mm.param(1);

        var tryStart = mm.label().here();

        var stateVars = readStateFields(pipeVar);
        checkPrimaryKeySet(mm, pipeVar, stateVars);

        var makerVar = mm.var(RemoteProxyMaker.class);
        var keyVar = makerVar.invoke("decodeKey", pipeVar);
        makerVar.invoke("skipValue", pipeVar);

        var valueVar = makerVar.invoke(variant, mm.field("table"), txnVar, keyVar, pipeVar);

        if (variant == "load") {
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
     * @param variant "store", "exchange", "insert", or "replace"
     */
    private void addStoreDirectMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (Pipe.class, variant, RemoteTransaction.class, Pipe.class).public_();

        // FIXME: handle automatic key column

        var txnVar = txn(mm.param(0));
        var pipeVar = mm.param(1);

        var tryStart = mm.label().here();

        var stateVars = readStateFields(pipeVar);

        var readyVar = mm.var(boolean.class);
        mRowGen.checkSet(mm, mRowGen.info.allColumns, readyVar, num -> stateVars[num >> 4]);

        Label isReady = mm.label();
        readyVar.ifTrue(isReady);

        var paramVars = new Object[1 + stateVars.length];
        paramVars[0] = pipeVar;
        System.arraycopy(stateVars, 0, paramVars, 1, stateVars.length);
        mm.invoke("requireAllSet", paramVars);

        isReady.here();

        var makerVar = mm.var(RemoteProxyMaker.class);
        var keyVar = makerVar.invoke("decodeKey", pipeVar);
        var valueVar = makerVar.invoke("decodeValue", pipeVar, mm.field("prefixLength"));

        mm.var(RowUtils.class).invoke("encodePrefixPF", valueVar, 0, mm.field("schemaVersion"));

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
     * @param variant "update" or "merge"
     */
    private void addUpdateDirectMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (Pipe.class, variant, RemoteTransaction.class, Pipe.class).public_();

        var txnVar = txn(mm.param(0));
        var pipeVar = mm.param(1);

        var tryStart = mm.label().here();

        var stateVars = readStateFields(pipeVar);
        checkPrimaryKeySet(mm, pipeVar, stateVars);

        var makerVar = mm.var(RemoteProxyMaker.class);
        var keyVar = makerVar.invoke("decodeKey", pipeVar);
        var dirtyValueVar = makerVar.invoke("decodeValue", pipeVar);

        if (mUpdaterFactoryVar == null) {
            // Define and share a reference to the ValueUpdater factory constant.
            mUpdaterFactoryVar = makerVar.condy
                ("condyValueUpdater", mm.class_(), mTableClass, mRowType)
                .invoke(MethodHandle.class, "_");
        }

        Variable updaterVar;
        {
            var params = new Object[2 + stateVars.length];
            params[0] = mm.this_();
            params[1] = dirtyValueVar;
            for (int i=0; i<stateVars.length; i++) {
                params[2 + i] = stateVars[i];
            }
            // Make sure the updaterFactoryVar is bound to this method.
            var updaterFactoryVar = mm.var(MethodHandle.class).set(mUpdaterFactoryVar);
            updaterVar = updaterFactoryVar.invoke
                (BaseTable.ValueUpdater.class, "invokeExact", (Object[]) null, params);
        }

        var newValueVar = makerVar.invoke(variant, mm.field("table"), txnVar, mm.field("EMPTY_ROW"),
                                          keyVar, updaterVar, pipeVar);

        if (variant == "merge") {
            Label done = mm.label();
            newValueVar.ifEq(null, done);
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
        cm.implement(BaseTable.ValueUpdater.class);

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
        var schemaVersion = mm.field("proxy").field("schemaVersion");
        TableMaker.convertValueIfNecessary(tableVar, rowClass, schemaVersion, originalValueVar);

        var dirtyValueVar = mm.field("dirtyValue");

        // Note: The following code is similar to TableMaker.encodeUpdateEntry.

        // Identify the offsets to all the columns in the original and dirty entries, and
        // calculate the size of the new entry.

        Map<String, Integer> columnNumbers = rowGen.columnNumbers();
        ColumnCodec[] codecs = ColumnCodec.bind(rowGen.valueCodecs(), mm);

        Variable[] offsetVars = new Variable[codecs.length];

        var startOffsetVar = mm.var(RowUtils.class).invoke("lengthPrefixPF", schemaVersion);
        var offsetVar = mm.var(int.class).set(startOffsetVar);
        var newSizeVar = mm.var(int.class).set(startOffsetVar); // need room for schemaVersion
        newSizeVar.inc(dirtyValueVar.alength());

        int stateFieldNum = -1;
        Variable stateField = null;

        for (int i=0; i<codecs.length; i++) {
            ColumnCodec codec = codecs[i];
            codec.encodePrepare();

            offsetVars[i] = offsetVar.get();
            codec.decodeSkip(originalValueVar, offsetVar, null);

            ColumnInfo info = codec.mInfo;
            int num = columnNumbers.get(info.name);

            int sfNum = rowGen.stateFieldNum(num);
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
            ColumnInfo info = codec.mInfo;
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

            int sfNum = rowGen.stateFieldNum(num);
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

        return factory.asType(ctorType.changeReturnType(BaseTable.ValueUpdater.class));
    }

    /**
     * Extracts the real transaction object from the remote transaction.
     */
    private Variable txn(Variable remoteTxnVar) {
        return remoteTxnVar.methodMaker().var(ServerTransaction.class).invoke("txn", remoteTxnVar);
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
     * Convert the value to the current schema if necessary, and then call writeValue.
     */
    private void convertAndWriteValue(Variable pipeVar, Variable valueVar) {
        MethodMaker mm = pipeVar.methodMaker();
        var tableVar = mm.var(mTableClass);
        Class rowClass = RowMaker.find(mRowType);
        var schemaVersion = mm.field("schemaVersion");
        TableMaker.convertValueIfNecessary(tableVar, rowClass, schemaVersion, valueVar);
        writeValue(pipeVar, valueVar);
    }

    /**
     * Write the value sans the schema version. The total length of the value (sans schema
     * version) is written first using the prefix format.
     */
    private void writeValue(Variable pipeVar, Variable valueVar) {
        MethodMaker mm = pipeVar.methodMaker();
        var prefixLengthVar = mm.field("prefixLength").get();
        var lengthVar = valueVar.alength().sub(prefixLengthVar);
        mm.var(RowUtils.class).invoke("encodePrefixPF", pipeVar, lengthVar);
        pipeVar.invoke("write", valueVar, prefixLengthVar, lengthVar);
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
     */
    public static byte[] decodeValue(Pipe pipe) throws IOException {
        var bytes = new byte[RowUtils.decodePrefixPF(pipe)];
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
    public static byte[] load(BaseTable table, Transaction txn, byte[] key, Pipe pipe)
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
            pipe.writeObject(null); // no exception
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
    public static void exists(BaseTable table, Transaction txn, byte[] key, Pipe pipe)
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
            pipe.writeObject(null); // no exception
            pipe.writeByte(result ? 1 : 0); // success or failure
        }

        pipe.flush();
        pipe.recycle();
    }

    /**
     * Called by generated code.
     */
    @SuppressWarnings("unchecked")
    public static void store(BaseTable table, Transaction txn, Object row,
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
            pipe.writeObject(null); // no exception
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
    public static byte[] exchange(BaseTable table, Transaction txn, Object row,
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
            pipe.writeObject(null); // no exception
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
    public static void insert(BaseTable table, Transaction txn, Object row,
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
            pipe.writeObject(null); // no exception
            pipe.writeByte(result ? 1 : 0); // success or failure
        }

        pipe.flush();
        pipe.recycle();
    }

    /**
     * Called by generated code.
     */
    @SuppressWarnings("unchecked")
    public static void replace(BaseTable table, Transaction txn, Object row,
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
            pipe.writeObject(null); // no exception
            pipe.writeByte(result ? 1 : 0); // success or failure
        }

        pipe.flush();
        pipe.recycle();
    }

    /**
     * Called by generated code.
     */
    @SuppressWarnings("unchecked")
    public static void update(BaseTable table, Transaction txn, Object row,
                              byte[] key, BaseTable.ValueUpdater updater, Pipe pipe)
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
            pipe.writeObject(null); // no exception
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
    public static byte[] merge(BaseTable table, Transaction txn, Object row,
                               byte[] key, BaseTable.ValueUpdater updater, Pipe pipe)
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
            pipe.writeObject(null); // no exception
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
     */
    @SuppressWarnings("unchecked")
    public static void delete(BaseTable table, Transaction txn, byte[] key, Pipe pipe)
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
            pipe.writeObject(null); // no exception
            pipe.writeByte(result ? 1 : 0); // success or failure
        }

        pipe.flush();
        pipe.recycle();
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
     * Makes a RemoteTableProxy class which must perform column conversions.
     */
    private MethodHandles.Lookup makeConverter() {
        // FIXME: makeConverter
        throw null;
    }
}
