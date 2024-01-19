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

package org.cojen.tupl.sql;

import java.io.IOException;

import org.cojen.tupl.Table;

import org.cojen.tupl.model.ColumnNode;
import org.cojen.tupl.model.RelationNode;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class Scope implements TableFinder {
    private final Scope mParent;
    private final TableFinder mFinder;
    private final RelationNode mFrom;

    public Scope(TableFinder finder) {
        this(null, finder, null);
    }

    private Scope(Scope parent, TableFinder finder, RelationNode from) {
        mParent = parent;
        mFinder = finder;
        mFrom = from;
    }

    @Override
    public Table findTable(String name) throws IOException {
        return mFinder.findTable(name);
    }

    @Override
    public String schema() {
        return mFinder.schema();
    }

    /**
     * Returns a new Scope object.
     */
    @Override
    public Scope withSchema(String schema) {
        return new Scope(this, mFinder.withSchema(schema), mFrom);
    }

    /**
     * Returns null if no "from" relation is in scope.
     */
    public RelationNode from() {
        return mFrom;
    }

    /**
     * Returns a new Scope object.
     *
     * @param from can be null
     */
    public Scope withFrom(RelationNode from) {
        return new Scope(this, mFinder, from);
    }

    /**
     * Returns the parent Scope object.
     */
    public Scope parent() {
        return mParent;
    }

    /**
     * Find a column in the "from" relation which matches the given name.
     *
     * @param name qualified or unqualified column name to find
     * @return column with a fully qualified name, with the canonical case
     * @throws IllegalArgumentException if not found or is ambiguous
     */
    public ColumnNode findColumn(String name) {
        // TODO: search parent scope if not found locally
        RelationNode from = mFrom;
        if (from == null) {
            throw new IllegalStateException("Column isn't found: " + name);
        }
        return from.findColumn(name);
    }
}
