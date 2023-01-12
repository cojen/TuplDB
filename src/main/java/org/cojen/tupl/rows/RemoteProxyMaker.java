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
    private record CacheKey(Class<?> rowType, byte[] descriptor) { }

    private static final SoftCache<CacheKey, MethodHandle, Object> cCache = new SoftCache<>() {
        @Override
        protected MethodHandle newValue(CacheKey key, Object unused) {
            return new RemoteProxyMaker(key.rowType, key.descriptor).finish();
        }
    };

    /**
     * @param descriptor encoding format is defined by RowHeader class
     */
    static RemoteTableProxy make(BaseTable<?> table, int schemaVersion, byte[] descriptor) {
        var mh = cCache.obtain(new CacheKey(table.rowType(), descriptor), null);
        try {
            return (RemoteTableProxy) mh.invoke(table, schemaVersion);
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private final Class<?> mRowType;
    private final RowGen mRowGen;
    private final RowHeader mRowHeader;
    private final ClassMaker mClassMaker;

    private RemoteProxyMaker(Class<?> rowType, byte[] descriptor) {
        mRowType = rowType;
        mRowGen = RowInfo.find(rowType).rowGen();
        mRowHeader = RowHeader.decode(descriptor);
        mClassMaker = mRowGen.beginClassMaker(RemoteProxyMaker.class, rowType, null);
        mClassMaker.implement(RemoteTableProxy.class);
    }

    /**
     * Returns a MethodHandle: RemoteTableProxy new(BaseTable table, int schemaVersion);
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

        return mClassMaker.finishHidden();
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

        makerVar.invoke(variant, mm.field("table"), txnVar, keyVar, pipeVar);

        mm.return_(null);

        var exVar = mm.catch_(tryStart, mm.label().here(), Throwable.class);
        makerVar.invoke("handleException", exVar, pipeVar);
        mm.return_(null);
    }

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

        makerVar.invoke(variant, mm.field("table"), txnVar, mm.field("EMPTY_ROW"),
                        keyVar, valueVar, pipeVar);

        mm.return_(null);

        var exVar = mm.catch_(tryStart, mm.label().here(), Throwable.class);
        makerVar.invoke("handleException", exVar, pipeVar);
        mm.return_(null);
    }

    private void addUpdateDirectMethod(String variant) {
        MethodMaker mm = mClassMaker.addMethod
            (Pipe.class, variant, RemoteTransaction.class, Pipe.class).public_();

        // FIXME: exception handling; pipe recycle; pipe close

        var txnVar = txn(mm.param(0));
        var pipeVar = mm.param(1);

        var stateVars = readStateFields(pipeVar);

        // FIXME: addUpdateDirectMethod
        pipeVar.invoke("close");

        mm.return_(null);
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

    private static int decodeKeyLength(Pipe pipe) throws IOException {
        int length = pipe.read();

        if (length < 0) {
            length = ((length & 0x7f) << 24)
                | (pipe.readUnsignedShort() << 8)
                | pipe.readUnsignedByte();
        }

        return length;
    }

    private static int decodeValueLength(Pipe pipe) throws IOException {
        int length = pipe.readUnsignedShort();

        if (length < 0) {
            length = ((length & 0x7fff) << 16) | pipe.readUnsignedShort();
        }

        return length;
    }

    /**
     * Called by generated code.
     */
    public static void skipKeyAndValue(Pipe pipe) throws IOException {
        pipe.skip(decodeKeyLength(pipe));
        pipe.skip(decodeValueLength(pipe));
    }

    /**
     * Called by generated code.
     */
    public static void skipValue(Pipe pipe) throws IOException {
        pipe.skip(decodeValueLength(pipe));
    }

    /**
     * Called by generated code.
     */
    public static byte[] decodeKey(Pipe pipe) throws IOException {
        var bytes = new byte[decodeKeyLength(pipe)];
        pipe.readFully(bytes);
        return bytes;
    }

    /**
     * Called by generated code.
     */
    public static byte[] decodeValue(Pipe pipe, int prefixLength) throws IOException {
        int valueLength = decodeValueLength(pipe);
        var bytes = new byte[prefixLength + valueLength];
        pipe.readFully(bytes, prefixLength, valueLength);
        return bytes;
    }

    /**
     * Called by generated code.
     */
    public static void load(BaseTable table, Transaction txn, byte[] key, Pipe pipe)
        throws IOException
    {
        // FIXME: If load fails, value columns must be unset. Just on the client side though?

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
                // FIXME: write the value in column format (lead with a bitmap)
                pipe.close();
            }
        }

        pipe.flush();
        pipe.recycle();
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
     */
    @SuppressWarnings("unchecked")
    public static void exchange(BaseTable table, Transaction txn, Object row,
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
                // FIXME: Write oldValue columns.
                pipe.close();
            }
        }

        pipe.flush();
        pipe.recycle();
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
