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

package org.cojen.tupl.table;

/**
 * Decoded RowInfo for a secondary index.
 *
 * @author Brian S O'Neill
 * @see RowStore#secondaryRowInfo
 */
class SecondaryInfo extends RowInfo {
    final RowInfo primaryInfo;

    private final boolean mIsAltKey;

    private String mIndexSpec;

    SecondaryInfo(RowInfo primaryInfo, boolean isAltKey) {
        super(primaryInfo.name);
        this.primaryInfo = primaryInfo;
        mIsAltKey = isAltKey;
    }

    @Override
    String indexSpec() {
        String spec = mIndexSpec;
        if (spec == null) {
            mIndexSpec = spec = super.indexSpec();
        }
        return spec;
    }

    @Override
    boolean isAltKey() {
        return mIsAltKey;
    }

    @Override
    public String toString() {
        return "name: " + name + ", " + keyColumns.values() + " -> " + valueColumns.values() +
            ", allColumns: " + allColumns.values() + ", isAltKey: " + mIsAltKey;
    }
}
