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

package org.cojen.tupl.rows.join;

import java.util.Map;

import org.cojen.tupl.PrimaryKey;

import org.cojen.tupl.rows.ColumnInfo;
import org.cojen.tupl.rows.RowInfo;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class JoinColumnInfo extends ColumnInfo {
    @Override
    public boolean isScalarType() {
        return false;
    }

    /**
     * @return null if not found
     */
    @Override
    public ColumnInfo subColumn(String name) {
        if (isPlainColumnType()) {
            return RowInfo.find(type).allColumns.get(name);
        } else {
            return JoinRowInfo.find(type).allColumns.get(name);
        }
    }

    @Override
    public void gatherScalarColumns(Map<String, ColumnInfo> dst) {
        gatherScalarColumns(name, dst);
    }

    @Override
    public void gatherScalarColumns(String path, Map<String, ColumnInfo> dst) {
        if (isPlainColumnType()) {
            for (ColumnInfo info : RowInfo.find(type).allColumns.values()) {
                info.gatherScalarColumns(path, dst);
            }
        } else {
            for (ColumnInfo info : JoinRowInfo.find(type).allColumns.values()) {
                info.gatherScalarColumns(path + '.' + info.name, dst);
            }
        }
    }

    private boolean isPlainColumnType() {
        // This simple check works because join row types cannot specify a primary key.
        return type.isAnnotationPresent(PrimaryKey.class);
    }
}
