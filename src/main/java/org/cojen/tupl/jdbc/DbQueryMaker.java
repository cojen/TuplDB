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

package org.cojen.tupl.jdbc;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;

import java.util.Arrays;
import java.util.Map;

import java.sql.ResultSet;
import java.sql.SQLNonTransientException;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Scanner;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.rows.WeakCache;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class DbQueryMaker {
    /**
     * Returns a new Factory instance. Factory classes are cached by the provider's rowType,
     * projection, and argumentCount.
     *
     * @throws SQLNonTransientException if a requested column doesn't exist
     */
    public static DbQuery.Factory make(DbQuery.TableProvider<?> provider)
        throws SQLNonTransientException
    {
        var key = new Key(provider.rowType(), provider.projection(), provider.argumentCount());

        try {
            return (DbQuery.Factory) cCache.obtain(key, null).invoke(provider);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private record Key(Class<?> rowType, Map<String, String> projection, int argCount) { }

    private static final WeakCache<Key, MethodHandle, Object> cCache = new WeakCache<>() {
        protected MethodHandle newValue(Key key, Object unused) {
            try {
                return new DbQueryMaker(key).finish();
            } catch (SQLNonTransientException e) {
                throw Utils.rethrow(e); 
            }
        }
    };

    private final Class<?> mRowType;
    private final Map<String, String> mProjection;
    private final int mArgCount;

    private DbQueryMaker(Key key) {
        mRowType = key.rowType();
        mProjection = key.projection();
        mArgCount = key.argCount();
    }

    /**
     * Returns a MethodHandle to a factory constructor which accepts a provider instance.
     */
    private MethodHandle finish() throws SQLNonTransientException {
        var rsClass = ResultSetMaker.find(mRowType, mProjection, 1);

        ClassMaker cm = CodeUtils.beginClassMaker(DbQueryMaker.class, rsClass, null, "query");
        cm.extend(DbQuery.class).implement(ScannerFactory.class).final_();

        cm.addField(DbQuery.TableProvider.class, "provider").private_().final_();
        cm.addField(rsClass, "rs").private_();

        MethodMaker ctor = cm.addConstructor(DbConnection.class, DbQuery.TableProvider.class);
        ctor.invokeSuperConstructor(ctor.param(0));
        ctor.field("provider").set(ctor.param(1));

        if (mArgCount > 0) {
            cm.addField(Object[].class, "args").private_().final_();
            ctor.field("args").set(ctor.new_(Object[].class, mArgCount));

            MethodMaker mm = cm.addMethod(null, "clearParameters").public_().override();
            mm.var(Arrays.class).invoke("fill", mm.field("args"), null);

            mm = cm.addMethod(null, "setObject", int.class, Object.class).public_().override();
            mm.invoke("setObject", mm.field("args"), mm.param(0), mm.param(1));
        }

        // Define a private method to produce a ResultSet, as the generated type. This makes it
        // easier for the executeQuery method to call the isUninitialized method, which is
        // defined in BaseResultSet.
        {
            MethodMaker mm = cm.addMethod(rsClass, "resultSet").private_();
            var rsField = mm.field("rs");
            var rsVar = rsField.get();
            Label ready = mm.label();
            rsVar.ifNe(null, ready);
            mm.invoke("checkClosed");
            rsVar.set(mm.new_(rsClass));
            rsField.set(rsVar);
            ready.here();
            mm.return_(rsVar);
        }

        {
            MethodMaker mm = cm.addMethod(ResultSet.class, "getResultSet").public_();
            mm.return_(mm.invoke("resultSet"));
        }

        {
            MethodMaker mm = cm.addMethod(ResultSet.class, "executeQuery").public_();
            Label start = mm.label().here();
            var rsVar = mm.invoke("resultSet");
            Label ready = mm.label();
            rsVar.invoke("isUninitialized").ifTrue(ready);
            mm.invoke("closeResultSet");
            start.goto_();
            ready.here();
            rsVar.invoke("init", mm.this_());
            mm.return_(rsVar);
        }

        {
            MethodMaker mm = cm.addMethod(null, "closeResultSet").public_();
            var rsField = mm.field("rs");
            var rsVar = rsField.get();
            Label ready = mm.label();
            rsVar.ifEq(null, ready);
            rsField.set(null);
            rsVar.invoke("close");
            ready.here();
        }

        {
            MethodMaker mm = cm.addMethod(Scanner.class, "newScannerWith", Object.class).public_();
            var rowVar = mm.param(0);
            var txnVar = mm.invoke("txn");
            var providerField = mm.field("provider");
            Variable tableVar;
            if (mArgCount == 0) {
                tableVar = providerField.invoke("table");
            } else {
                tableVar = providerField.invoke("table", mm.field("args"));
            }
            mm.return_(tableVar.invoke("newScannerWith", txnVar, rowVar));
        }

        assert cm.unimplementedMethods().isEmpty();

        Class<?> dbQueryClass = cm.finish();

        // Now make the factory class.

        cm = CodeUtils.beginClassMaker(DbQueryMaker.class, rsClass, null, "factory");
        cm.implement(DbQuery.Factory.class).final_();

        // Keep a reference to the MethodHandle instance, to prevent it from being garbage
        // collected as long as the generated factory class still exists.
        cm.addField(Object.class, "_").static_().private_();

        cm.addField(DbQuery.TableProvider.class, "provider").private_().final_();

        ctor = cm.addConstructor(DbQuery.TableProvider.class);
        ctor.invokeSuperConstructor();
        ctor.field("provider").set(ctor.param(0));

        {
            MethodMaker mm = cm.addMethod(DbQuery.class, "newDbQuery", DbConnection.class);
            mm.public_();
            mm.return_(mm.new_(dbQueryClass, mm.param(0), mm.field("provider")));
        }

        MethodHandles.Lookup lookup = cm.finishHidden();
        Class<?> factoryClass = lookup.lookupClass();

        MethodMaker mm = MethodMaker.begin
            (lookup, DbQuery.Factory.class, null, DbQuery.TableProvider.class);
        mm.return_(mm.new_(factoryClass, mm.param(0)));

        MethodHandle mh = mm.finish();

        try {
            // Assign the singleton reference.
            lookup.findStaticVarHandle(factoryClass, "_", Object.class).set(mh);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }

        return mh;
    }
}
