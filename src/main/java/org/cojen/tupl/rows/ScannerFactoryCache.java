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

package org.cojen.tupl.rows;

import org.cojen.tupl.rows.filter.Parser;
import org.cojen.tupl.rows.filter.QuerySpec;

/**
 * Specialized cache used by MappedTable, AggregatedTable, and GroupedTable.
 *
 * @author Brian S. O'Neill
 */
public final class ScannerFactoryCache<F> extends SoftCache<String, Object, Object> {

    @SuppressWarnings("unchecked")
    public F obtain(String queryStr, Helper<F> helper) {
        return (F) super.obtain(queryStr, helper);
    }

    @Override
    protected Object newValue(String queryStr, Object helperObj) {
        if (helperObj instanceof Helper helper) {
            RowInfo rowInfo = RowInfo.find(helper.rowType());
            QuerySpec query = new Parser(rowInfo.allColumns, queryStr).parseQuery(null);
            String canonicalStr = query.toString();
            if (canonicalStr.equals(queryStr)) {
                return helper.makeScannerFactory(query);
            } else {
                return obtain(canonicalStr, new ForCanonical(helper, query));
            }
        } else {
            return ((ForCanonical) helperObj).makeScannerFactory();
        }
    }

    public static interface Helper<F> {
        Class<?> rowType();

        F makeScannerFactory(QuerySpec query);
    }

    private static record ForCanonical(Helper helper, QuerySpec query) {
        Object makeScannerFactory() {
            return helper.makeScannerFactory(query);
        }
    }
}