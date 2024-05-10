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

package org.cojen.tupl.table.expr;

import java.io.IOException;

import org.cojen.tupl.Row;
import org.cojen.tupl.Table;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.table.SoftCache;

/**
 * Generic cache for compiled queries which produce Row objects.
 *
 * @author Brian S. O'Neill
 */
public final class CompiledQueryCache extends SoftCache<Object, CompiledQuery<Row>, Object> {
    public CompiledQueryCache() {
    }

    public CompiledQuery<Row> obtain(String query, Table<?> table) throws IOException {
        return obtain((Object) query, (Object) table);
    }

    @Override
    public CompiledQuery<Row> newValue(Object queryOrKey, Object helper) {
        if (queryOrKey instanceof TupleKey) {
            try {
                return ((RelationExpr) helper).makeCompiledRowQuery();
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }
        }
        String queryStr = (String) queryOrKey;
        RelationExpr expr = Parser.parse((Table) helper, queryStr);
        // Obtain the canonical instance and map to that.
        TupleKey key = expr.makeKey();
        return obtain(key, expr);
    }
}
