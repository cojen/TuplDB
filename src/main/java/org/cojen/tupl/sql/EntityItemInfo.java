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
 * @author Brian S. O'Neill
 * @see EntityInfo
 */
@PrimaryKey({"entitySchema", "entityName", "type", "name"})
public interface EntityItemInfo {
    static final byte TYPE_COLUMN = 1, TYPE_INDEX = 2;

    /**
     * @see EntityInfo#schema
     */
    @Nullable
    String entitySchema();
    void entitySchema(String schema);

    /**
     * @see EntityInfo#name
     */
    String entityName();
    void entityName(String name);

    /**
     * The type of this item: TYPE_COLUMN, TYPE_INDEX, etc.
     */
    byte type();
    void type(byte type);

    /**
     * The lowercase name given to this item.
     */
    @Nullable
    String name();
    void name(String name);

    /**
     * The original name given to this item, with the original case. Is null if the original
     * name was lowercase.
     */
    @Nullable
    String originalName();
    void originalName(String name);

    /**
     * Defines this item in a fashion which is specific for each type. If the item doesn't
     * require a special definition, then this item info row isn't needed at all. For example,
     * plain columns don't require anything special.
     */
    byte[] definition();
    void definition(byte[] definition);
}
