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

import org.cojen.maker.Variable;

/**
 * Defines a node which accesses a relation/table of rows.
 *
 * @author Brian S. O'Neill
 */
public abstract class RelationNode extends Node {
    private final RelationType mType;
    private final String mName;

    /**
     * @param name can be null to automatically assign a name
     */
    protected RelationNode(RelationType type, String name) {
        mType = type;
        mName = name != null ? name : type.tupleType().clazz().getName();
    }

    @Override
    public final RelationType type() {
        return mType;
    }

    @Override
    public final Node asType(Type type) {
        // FIXME: Should this ever be supported? Yes, for supporting renames, and for
        // converting to basic nodes (depending on cardinality).
        throw null;
    }

    @Override
    public Variable makeEval(EvalContext context) {
        // FIXME: makeEval - return a Table
        throw null;
    }

    public final String name() {
        return mName;
    }

    /**
     * Find a column in this relation which matches the given name.
     *
     * @param name qualified or unqualified column name to find
     * @return column with a fully qualified name, with the canonical case
     * @throws IllegalArgumentException if not found or is ambiguous
     */
    public ColumnNode findColumn(String name) {
        return ColumnNode.make(name, type().tupleType().findColumn(name, true));
    }

    /**
     * Makes a fully functional query from this node.
     */
    public abstract Query<?> makeQuery();
}
