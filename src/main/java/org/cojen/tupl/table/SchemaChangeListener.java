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

package org.cojen.tupl.table;

/**
 * @author Brian S. O'Neill
 * @see RowStore#addSchemaChangeListener
 */
public interface SchemaChangeListener {
    static final int CHANGE_CREATE = 1;      // table is created
    static final int CHANGE_SCHEMA = 2;      // columns are added or dropped
    static final int CHANGE_DROP = 4;        // the table is dropped
    static final int CHANGE_ADD_INDEX = 8;   // a secondary index or alternate key is added
    static final int CHANGE_DROP_INDEX = 16; // a secondary index or alternate key is dropped

    /**
     * @param tableId id of table's primary index
     */
    void schemaChanged(long tableId, int changes);
}
