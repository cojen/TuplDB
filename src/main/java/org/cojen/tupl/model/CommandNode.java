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

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public abstract sealed class CommandNode extends Node permits InsertNode {
    private final String mName;

    protected CommandNode(String name) {
        mName = name;
    }

    @Override
    public final Type type() {
        return BasicType.make(long.class, Type.TYPE_LONG);
    }

    @Override
    public final Node asType(Type type) {
        if (type().equals(type)) {
            return this;
        }
        throw new IllegalStateException("Cannot convert " + type() + " to " + type);
    }

    @Override
    public final boolean isPureFunction() {
        return false;
    }

    @Override
    public final boolean isNullable() {
        return false;
    }

    @Override
    public final String name() {
        return mName;
    }

    /**
     * Makes a fully functional Command instance from this node.
     */
    public abstract Command makeCommand();
}
