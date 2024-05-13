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

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import org.cojen.tupl.table.expr.Parser;

import org.cojen.tupl.table.filter.QuerySpec;

/**
 * Specialized cache used by MappedTable, AggregatedTable, and GroupedTable.
 *
 * @author Brian S. O'Neill
 */
public final class QueryFactoryCache extends SoftCache<String, MethodHandle, Object> {
    public MethodHandle obtain(String queryStr, Helper helper) {
        return super.obtain(queryStr, helper);
    }

    /**
     * Returns a constructor MethodHandle which accepts the given parameter type. The lookup
     * class must have a private static Object field named "handle". This is used to keep a
     * reference to the MethodHandle instance, to prevent it from being garbage collected as
     * long as the generated factory class still exists.
     */
    static MethodHandle ctorHandle(MethodHandles.Lookup lookup, Class<?> paramType) {
        try {
            MethodHandle mh = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class, paramType));
            lookup.findStaticVarHandle
                (lookup.lookupClass(), "handle", Object.class).set(mh);
            return mh;
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    public static interface Helper {
        Class<?> rowType();

        MethodHandle makeQueryFactory(QuerySpec query);
    }

    @Override
    protected MethodHandle newValue(String queryStr, Object helperObj) {
        if (helperObj instanceof Helper helper) {
            QuerySpec query = Parser.parseQuerySpec(helper.rowType(), queryStr);
            String canonicalStr = query.toString();
            if (canonicalStr.equals(queryStr)) {
                return helper.makeQueryFactory(query);
            } else {
                return obtain(canonicalStr, new ForCanonical(helper, query));
            }
        } else {
            return ((ForCanonical) helperObj).makeQueryFactory();
        }
    }

    private static record ForCanonical(Helper helper, QuerySpec query) {
        MethodHandle makeQueryFactory() {
            return helper.makeQueryFactory(query);
        }
    }
}
