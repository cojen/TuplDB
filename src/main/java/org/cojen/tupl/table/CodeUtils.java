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

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import static org.cojen.tupl.table.ColumnInfo.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CodeUtils {
    /**
     * Allocates a new instance if the given variable is null or else blindly casts it to the
     * given type.
     */
    static Variable castOrNew(Variable var, Class<?> type) {
        final var typedVar = var.cast(type);
        MethodMaker mm = var.methodMaker();
        Label notNull = mm.label();
        typedVar.ifNe(null, notNull);
        typedVar.set(mm.new_(type));
        notNull.here();
        return typedVar;
    }

    /**
     * Allocates a new instance if the given variable is null or else casts it to the given
     * type. If the cast would fail, branch to the `other` label.
     */
    static Variable castOrNew(Variable var, Class<?> type, Label other) {
        MethodMaker mm = var.methodMaker();
        final Variable typedVar = mm.var(type);
        Label notNull = mm.label();
        var.ifNe(null, notNull);
        typedVar.set(mm.new_(type));
        Label done = mm.label().goto_();
        notNull.here();
        var.instanceOf(type).ifFalse(other);
        typedVar.set(var.cast(type));
        done.here();
        return typedVar;
    }

    /**
     * Casts the given variable to a RowConsumer, calls the accept method, and then returns the
     * variable from the method being generated. If the cast fails, the given rowClass is used
     * in the thrown ClassCastException.
     */
    static void acceptAsRowConsumerAndReturn(Variable var, Class<?> rowClass,
                                             Object key, Object value)
    {
        MethodMaker mm = var.methodMaker();
        Label tryStart = mm.label().here();
        var consumerVar = var.cast(RowConsumer.class);
        Label tryEnd = mm.label().here();
        consumerVar.invoke("accept", key, value);
        mm.return_(var);
        Variable ex = mm.catch_(tryStart, tryEnd, ClassCastException.class);
        // If this cast succeeds, then the original exception is thrown instead.
        var.cast(rowClass);
        ex.throw_();
    }

    /**
     * Appends a column value to a StringBuilder.
     *
     * @param bob StringBuilder
     */
    public static void appendValue(Variable bob, ColumnInfo info, Variable value) {
        MethodMaker mm = bob.methodMaker();

        if (info.isArray()) {
            MethodHandle mh = ArrayStringMaker.make(info.type, info.isUnsignedInteger());
            bob.set(mm.invoke(mh, bob, value, 16)); // limit=16
            return;
        }

        Label done = null;

        if (info.isUnsignedInteger()) {
            if (info.isNullable()) {
                Label notNull = mm.label();
                value.ifNe(null, notNull);
                bob.invoke("append", "null");
                done = mm.label().goto_();
                notNull.here();
            }

            switch (info.plainTypeCode()) {
            default: throw new AssertionError();
            case TYPE_UBYTE: value = value.cast(int.class).and(0xff); break;
            case TYPE_USHORT: value = value.cast(int.class).and(0xffff); break;
            case TYPE_UINT: case TYPE_ULONG: break;
            }

            value = value.invoke("toUnsignedString", value);
        }

        bob.invoke("append", value);

        if (done != null) {
            done.here();
        }
    }

    /**
     * Generates code which follows a path to obtain a column value. The generated code might
     * throw an UnsetColumnException or NullPointerException (if following a null join path).
     *
     * @param rowVar can be a row type or row class
     * @param noException when true, return a cleared value instead of throwing
     * UnsetColumnException or NullPointerException (NPE is still thrown if initial row is null)
     * @return a local Variable or a Field
     */
    /*
    public static Variable getColumnValue(Variable rowVar, ColumnInfo ci, boolean noException) {
        String prefix = ci.prefix();

        if (prefix == null && noException && ci.isScalarType()
            && !rowVar.classType().isInterface())
        {
            return rowVar.field(ci.name);
        }

        MethodMaker mm = rowVar.methodMaker();

        final Variable columnVar = mm.var(ci.type);

        Label tryStart, done;

        if (noException) {
            tryStart = mm.label().here();
            done = mm.label();
        } else {
            tryStart = null;
            done = null;
        }

        while (true) {
            if (prefix == null) {
                columnVar.set(rowVar.invoke(ci.name));
                break;
            }

            rowVar = rowVar.invoke(prefix);

            if (noException) {
                Label cont = mm.label();
                rowVar.ifNe(null, cont);
                columnVar.clear();
                done.goto_();
                cont.here();
            }

            ci = ci.tail();
            prefix = ci.prefix();
        }

        if (tryStart != null) {
            mm.catch_(tryStart, UnsetColumnException.class, exVar -> {
                columnVar.clear();
            });

            done.here();
        }

        return columnVar;
    }
    */

    /**
     * Generates code which follows a path to assign a column value. The generated code might
     * throw an UnsetColumnException or NullPointerException (if following a null join path).
     *
     * @param rowVar can be a row type or row class
     */
    /*
    public static void setColumnValue(Variable rowVar, ColumnInfo ci, Variable columnVar) {
        for (String prefix = ci.prefix(); prefix != null; prefix = ci.prefix()) {
            rowVar = rowVar.invoke(prefix);
            ci = ci.tail();
            prefix = ci.prefix();
        }
        rowVar.invoke(ci.name, columnVar);
    }
    */
}
