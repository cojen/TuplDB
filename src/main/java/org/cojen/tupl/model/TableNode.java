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

import java.util.Objects;

import org.cojen.tupl.Table;

import org.cojen.tupl.rows.IdentityTable;
import org.cojen.tupl.rows.RowInfo;

/**
 * Defines a node which access an ordinary table.
 *
 * @author Brian S. O'Neill
 */
public final class TableNode extends RelationNode {
    /**
     * @param name can be null to automatically assign a name
     */
    public static TableNode make(String name, Table table) {
        var cardinality = table instanceof IdentityTable ? Cardinality.ONE : Cardinality.MANY;
        var type = RelationType.make(TupleType.make(table.rowType(), null), cardinality);
        return new TableNode(type, name, table);
    }

    private final Table<?> mTable;

    private TableNode(RelationType type, String name, Table table) {
        super(type, name);
        mTable = table;
    }

    @Override
    public Node asType(Type type) {
        // FIXME: Should this ever be supported? Yes, for supporting renames, and for
        // converting to basic nodes (depending on cardinality).
        throw null;
    }

    @Override
    public int highestParamOrdinal() {
        return 0;
    }

    @Override
    public boolean isPureFunction() {
        return mTable instanceof IdentityTable;
    }

    @Override
    public Query<?> makeQuery() {
        return Query.make(mTable);
    }

    public Table table() {
        return mTable;
    }

    @Override
    public int hashCode() {
        return mTable.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TableNode tn && mTable == tn.mTable;
    }
}
