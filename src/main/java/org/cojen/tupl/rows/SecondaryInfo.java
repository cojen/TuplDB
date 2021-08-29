/*
 *  Copyright (C) 2021 Cojen.org
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

package org.cojen.tupl.rows;

/**
 * Decoded RowInfo for a secondary index.
 *
 * @author Brian S O'Neill
 * @see RowStore#indexRowInfo
 */
class SecondaryInfo extends RowInfo {
    final RowInfo primaryInfo;
    final boolean isAltKey;

    SecondaryInfo(RowInfo primaryInfo, boolean isAltKey) {
        super(null);
        this.primaryInfo = primaryInfo;
        this.isAltKey = isAltKey;
    }

    @Override
    public String toString() {
        return keyColumns.values() + " -> " + valueColumns.values() +
            ", allColumns: " + allColumns.values() + ", isAltKey: " + isAltKey;
    }
}
