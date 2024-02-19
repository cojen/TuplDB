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
 * 
 *
 * @author Brian S. O'Neill
 */
public abstract sealed class CommandNode extends Node permits CommandNode.Basic, InsertNode {
    /**
     * Make a basic CommandNode which just wraps a command instance. The makeEval method isn't
     * supported.
     */
    public static CommandNode make(String name, Command command) {
        return new Basic(name, command);
    }

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

    static final class Basic extends CommandNode {
        private final Command mCommand;

        Basic(String name, Command command) {
            super(name);
            mCommand = command;
        }

        @Override
        public Command makeCommand() {
            return mCommand;
        }

        @Override
        public Basic withName(String name) {
            return name.equals(name()) ? this : new Basic(name, mCommand);
        }

        @Override
        public int maxArgument() {
            return mCommand.argumentCount();
        }

        @Override
        public void evalColumns(Set<String> columns) {
        }

        @Override
        public Variable makeEval(EvalContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Basic replaceConstants(Map<ConstantNode, FieldNode> map, String prefix) {
            return this;
        }

        private static final byte K_TYPE = KeyEncoder.allocType();

        @Override
        protected void encodeKey(KeyEncoder enc) {
            if (enc.encode(this, K_TYPE)) {
                enc.encodeObject(mCommand);
            }
        }

        @Override
        public int hashCode() {
            return name().hashCode() * 31 + mCommand.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Basic b && name().equals(b.name()) && mCommand.equals(b.mCommand);
        }
    }
}
