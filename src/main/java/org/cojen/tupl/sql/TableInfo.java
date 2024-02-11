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

import org.cojen.tupl.Nullable;
import org.cojen.tupl.PrimaryKey;

/**
 * Stores information regarding tables and views which were created using SQL statements.
 *
 * @author Brian S. O'Neill
 */
@PrimaryKey({"schema", "name"})
public interface TableInfo {
    static final byte TYPE_TABLE = 1, TYPE_VIEW = 2;

    /**
     * The optional table schema, in lowercase.
     */
    @Nullable
    String schema();
    void schema(String schema);

    /**
     * The simple name of this entity, in lowercase, with no dots in it.
     */
    String name();
    void name(String name);

    /**
     * The original full name given to the table, with the original case. Is null if the
     * original name was lowercase.
     */
    @Nullable
    String originalName();
    void originalName(String name);

    /**
     * The type of this entity: TYPE_TABLE, TYPE_VIEW, etc.
     */
    byte type();
    void type(byte type);

    /**
     * The SQL for defining this entity, which is only needed for views.
     */
    @Nullable
    String definition();
    void definition(String definition);

    /**
     * Tracks the views which immediately depend on this entity. The format of each element is
     * "schema.name", but it can be just ".name" if the schema is the same as for this entity.
     * If the dependent view has no schema, then the format is just "name". A semicolon is used
     * to separate each of the dependents.
     */
    @Nullable
    String dependents();
    void dependents(String dependents);
}
