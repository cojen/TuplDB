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

package org.cojen.tupl.model;

import java.util.List;
import java.util.Objects;

import org.cojen.maker.Label;
import org.cojen.maker.Variable;

import org.cojen.tupl.rows.ColumnInfo;

/**
 * Defines a simple node which just represents a constant value.
 *
 * @author Brian S. O'Neill
 */
public final class ConstantNode extends Node {
    public static ConstantNode make(Object value) {
        /* FIXME: needs a proper typeCode
        Type type = BasicType.make(value == null ? Object.class : value.getClass());
        return new ConstantNode(type, value);
        */
        throw null;
    }

    public static ConstantNode make(boolean value) {
        return new ConstantNode(boolean.class, ColumnInfo.TYPE_BOOLEAN, value);
    }

    public static ConstantNode make(byte value) {
        return new ConstantNode(byte.class, ColumnInfo.TYPE_BYTE, value);
    }

    public static ConstantNode make(short value) {
        return new ConstantNode(short.class, ColumnInfo.TYPE_SHORT, value);
    }

    public static ConstantNode make(char value) {
        return new ConstantNode(char.class, ColumnInfo.TYPE_CHAR, value);
    }

    public static ConstantNode make(int value) {
        return new ConstantNode(int.class, ColumnInfo.TYPE_INT, value);
    }

    public static ConstantNode make(long value) {
        return new ConstantNode(long.class, ColumnInfo.TYPE_LONG, value);
    }

    public static ConstantNode make(float value) {
        return new ConstantNode(float.class, ColumnInfo.TYPE_FLOAT, value);
    }

    public static ConstantNode make(double value) {
        return new ConstantNode(double.class, ColumnInfo.TYPE_DOUBLE, value);
    }

    private final Type mType;
    private final Object mValue;

    private ConstantNode(Class type, int typeCode, Object value) {
        this(BasicType.make(type, typeCode), value);
    }

    private ConstantNode(Type type, Object value) {
        mType = type;
        mValue = value;
    }

    @Override
    public Type type() {
        return mType;
    }

    @Override
    public Node asType(Type type) {
        if (type.equals(mType)) {
            return this;
        }
        if (mValue == null) {
            return new ConstantNode(type, null);
        }
        // FIXME: Support conversions; throw an exception if conversion fails. Conversion to
        // RelationType should always be supported (SelectNode). If converting to an object
        // like BigInteger, makeEval must call setExact.
        throw null;
    }

    @Override
    public String name() {
        return String.valueOf(mValue);
    }

    @Override
    public int highestParamOrdinal() {
        return 0;
    }

    @Override
    public boolean isPureFunction() {
        return true;
    }

    @Override
    public boolean isPureFilterTerm() {
        return true;
    }

    @Override
    public int appendPureFilter(StringBuilder query, List<Object> argConstants, int argOrdinal) {
        argConstants.add(mValue);
        query.append('?').append(++argOrdinal);
        return argOrdinal;
    }

    @Override
    public Variable makeEval(MakerContext context) {
        return context.methodMaker().var(mType.clazz()).set(mValue);
    }

    @Override
    public void makeFilter(MakerContext context, Label pass, Label fail) {
        if (mValue instanceof Boolean b) {
            context.methodMaker().goto_(b ? pass : fail);
        } else {
            throw new IllegalStateException();
        }
    }

    public Object value() {
        return mValue;
    }

    @Override
    public int hashCode() {
        return mType.hashCode() * 31 + Objects.hashCode(mValue);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ConstantNode cn
            && mType.equals(cn.mType) && Objects.equals(mValue, cn.mValue);
    }
}
