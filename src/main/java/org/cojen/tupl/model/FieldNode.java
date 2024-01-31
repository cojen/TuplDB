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

import java.util.Map;
import java.util.Set;

import org.cojen.maker.Variable;

/**
 * Represents a constant which has been replaced with an instance field. This is used for
 * reducing the amount of generated classes, for when they only differ by a few constants. The
 * instance that the field belongs to must be the enclosing class of the method being made.
 *
 * @author Brian S. O'Neill
 */
public final class FieldNode extends Node {
    static FieldNode make(Type type, String name) {
        return new FieldNode(type, name);
    }

    private final Type mType;
    private final String mName;

    private FieldNode(Type type, String name) {
        mType = type;
        mName = name;
    }

    @Override
    public Type type() {
        return mType;
    }

    @Override
    public Node asType(Type type) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String name() {
        return mName;
    }

    @Override
    public ParamNode withName(String name) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int maxArgument() {
        return 0;
    }

    @Override
    public boolean isPureFunction() {
        return true;
    }

    @Override
    public boolean isNullable() {
        return mType.isNullable();
    }

    @Override
    public void evalColumns(Set<String> columns) {
    }

    @Override
    public Variable makeEval(EvalContext context) {
        var resultRef = context.refFor(this);
        var result = resultRef.get();
        if (result != null) {
            return result;
        } else {
            return resultRef.set(context.methodMaker().field(mName));
        }
    }

    @Override
    public boolean canThrowRuntimeException() {
        return false;
    }

    @Override
    public FieldNode replaceConstants(Map<ConstantNode, FieldNode> map, String prefix) {
        return this;
    }

    @Override
    public int hashCode() {
        return mName.hashCode() * 168924149;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof FieldNode fn && mName.equals(fn.mName);
    }
}
