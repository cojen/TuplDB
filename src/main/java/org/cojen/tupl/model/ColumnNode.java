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

import java.util.Set;

import org.cojen.maker.Variable;

/**
 * Defines a node which accesses a named column found in a RelationNode.
 *
 * @author Brian S. O'Neill
 * @see RelationNode#findColumn
 */
public final class ColumnNode extends Node {
    /**
     * @param name qualified or unqualified name which was requested
     * @param column must refer to a column in a RelationNode; name is a fully qualified field
     */
    public static ColumnNode make(String name, Column column) {
        return new ColumnNode(name, column);
    }

    private final String mName;
    private final Column mColumn;

    private ColumnNode(String name, Column column) {
        mName = name;
        mColumn = column;
    }

    @Override
    public Type type() {
        return mColumn.type();
    }

    @Override
    public Node asType(Type type) {
        if (type.equals(type())) {
            return this;
        }
        // FIXME: runtime cast
        throw null;
    }

    /**
     * Returns the qualified or unqualified name which was requested.
     */
    @Override
    public String name() {
        return mName;
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
    public void evalColumns(Set<String> columns) {
        columns.add(mColumn.name());
    }

    @Override
    public Variable makeEval(EvalContext context) {
        var resultRef = context.refFor(this);
        var result = resultRef.get();
        if (result != null) {
            return result;
        } else {
            return resultRef.set(context.rowVar.invoke(mColumn.name()));
        }
    }

    @Override
    public boolean canThrowRuntimeException() {
        return false;
    }

    /**
     * Returns a RelationNode column with a fully qualified field name.
     */
    public Column column() {
        return mColumn;
    }

    @Override
    public int hashCode() {
        return mColumn.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ColumnNode cn && mColumn.equals(cn.mColumn);
    }
}
