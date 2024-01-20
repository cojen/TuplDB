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

package org.cojen.tupl.model;

import java.util.Set;

import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.Converter;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class ConversionNode extends Node {
    /**
     * Returns a ConversionNode if the given type differs from the node's current type.
     */
    public static Node make(Node node, Type type) {
        if (node.type().equals(type)) {
            return node;
        }
        return new ConversionNode(node, type);
    }

    private final Node mNode;
    private final Type mType;

    private ConversionNode(Node node, Type type) {
        mNode = node;
        mType = type;
    }

    @Override
    public Type type() {
        return mType;
    }

    @Override
    public Node asType(Type type) {
        return make(this, type);
    }

    @Override
    public String name() {
        return mNode.name();
    }

    @Override
    public ConversionNode withName(String name) {
        if (name.equals(name())) {
            return this;
        }
        return new ConversionNode(mNode.withName(name), mType);
    }

    @Override
    public int maxArgument() {
        return mNode.maxArgument();
    }

    @Override
    public boolean isPureFunction() {
        return mNode.isPureFunction();
    }

    @Override
    public boolean isNullable() {
        return mType.isNullable();
    }

    @Override
    public void evalColumns(Set<String> columns) {
        mNode.evalColumns(columns);
    }

    @Override
    public Variable makeEval(EvalContext context) {
        MethodMaker mm = context.methodMaker();
        var dstVar = mm.var(mType.clazz());
        Converter.convertExact(mm, name(), mNode.type(), mNode.makeEval(context), mType, dstVar);
        return dstVar;
    }

    @Override
    public boolean canThrowRuntimeException() {
        return mNode.canThrowRuntimeException();
    }

    @Override
    public boolean hasOrderDependentException() {
        return mNode.hasOrderDependentException();
    }

    @Override
    public int hashCode() {
        int hash = mNode.hashCode();
        hash = hash * 31 + mType.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ConversionNode cn && mNode.equals(cn.mNode) && mType.equals(cn.mType);
    }
}
