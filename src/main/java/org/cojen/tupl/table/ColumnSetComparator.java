/*
 *  Copyright 2021 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.table;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class ColumnSetComparator implements Comparator<ColumnSet> {
    static final ColumnSetComparator THE = new ColumnSetComparator(false);

    private final boolean mUnspecifiedIsEqual;

    ColumnSetComparator(boolean unspecifiedIsEqual) {
        mUnspecifiedIsEqual = unspecifiedIsEqual;
    }

    @Override
    public int compare(ColumnSet a, ColumnSet b) {
        if (a == b) {
            return 0;
        }
        int compare = compareColumns(a.keyColumns, b.keyColumns);
        if (compare == 0) {
            compare = compareColumns(a.valueColumns, b.valueColumns);
        }
        return compare;
    }

    private int compareColumns(Map<String, ColumnInfo> a, Map<String, ColumnInfo> b) {
        if (a.size() < b.size()) {
            return -1;
        } else if (a.size() > b.size()) {
            return 1;
        }

        Iterator<ColumnInfo> ait = a.values().iterator();
        Iterator<ColumnInfo> bit = b.values().iterator();

        while (ait.hasNext()) {
            var ainfo = ait.next();
            var binfo = bit.next();

            int compare = ainfo.name.compareTo(binfo.name);
            if (compare != 0) {
                return compare;
            }

            if (mUnspecifiedIsEqual && (ainfo.typeCode == -1 || binfo.typeCode == -1)) {
                continue;
            }

            compare = Integer.compare(ainfo.typeCode, binfo.typeCode);

            if (compare != 0) {
                return compare;
            }
        }

        return 0;
    }
}
