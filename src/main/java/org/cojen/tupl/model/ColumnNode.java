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

import org.cojen.maker.Variable;

/**
 * Defines a node which accesses a named column found in a RelationNode.
 *
 * @author Brian S. O'Neill
 */
public final class ColumnNode extends Node {
    /**
     * @param name qualified or unqualified name which was requested
     * @param column must refer to a column in a RelationNode; name is a fully qualified field
     */
    public static ColumnNode make(RelationNode from, String name, Column column) {
        return new ColumnNode(from, name, column);
    }

    private final RelationNode mFrom;
    private final String mName;
    private final Column mColumn;

    private ColumnNode(RelationNode from, String name, Column column) {
        mFrom = from;
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

    @Override
    public String name() {
        return mName;
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
        query.append(mColumn.name());
        return argOrdinal;
    }

    @Override
    public Variable makeEval(MakerContext context) {
        var resultRef = context.refFor(this);
        var result = resultRef.get();
        if (result != null) {
            return result;
        } else {
            return resultRef.set(context.rowVar.invoke(mColumn.name()));
        }
    }

    public RelationNode from() {
        return mFrom;
    }

    /**
     * Returns a RelationNode column with a fully qualified field name.
     */
    public Column column() {
        return mColumn;
    }

    @Override
    public int hashCode() {
        int hash = mFrom.hashCode();
        hash = hash * 31 + mColumn.hashCode();
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ColumnNode cn && mFrom.equals(cn.mFrom) && mColumn.equals(cn.mColumn);
    }
}
