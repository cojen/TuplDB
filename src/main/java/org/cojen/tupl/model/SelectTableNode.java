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

package org.cojen.tupl.model;

import java.util.ArrayList;

import org.cojen.tupl.Table;

/**
 * Defines a SelectNode in which the node's row type can be used for storing select results,
 * and so no Mapper is needed. A call to {@link SelectNode#make} returns a SelectTableNode
 * when all of the following conditions are met:
 *
 * <ul>
 * <li> "from" is a TableNode
 * <li> "where" is null or a pure filter
 * <li> "projection" only consists of ColumnNodes from the table, requested at most once
 * </ul>
 *
 * @author Brian S. O'Neill
 */
public final class SelectTableNode extends SelectNode {
    /**
     * @see SelectNode#make
     */
    SelectTableNode(TupleType type, String name, TableNode from, Node where, Node[] projection) {
        super(type, name, from, where, projection);
    }

    @Override
    protected Query<?> doMakeQuery() {
        Query<?> fromQuery = mFrom.makeQuery();

        boolean fullProjection = mProjection == null
            || mProjection.length == mFrom.type().tupleType().numColumns();

        if (fullProjection && mWhere == null) {
            return fromQuery;
        }

        // TODO: Should probably use an empty query if the effective where filter is false.

        int argCount = highestParamOrdinal();

        // Build up the full query string.
        var qb = new StringBuilder();
        {
            qb.append('{');
            if (fullProjection) {
                qb.append('*');
            } else {
                for (int i=0; i<mProjection.length; i++) {
                    if (i > 0) {
                        qb.append(", ");
                    }
                    qb.append(((ColumnNode) mProjection[i]).column().name());
                }
            }
            qb.append('}');
        }

        final Object[] viewArgs;
        if (mWhere == null) {
            viewArgs = Query.NO_ARGS;
        } else {
            qb.append(' ');
            var argConstants = new ArrayList<Object>();
            mWhere.appendPureFilter(qb, argConstants, argCount);
            if (argConstants.isEmpty()) {
                viewArgs = Query.NO_ARGS;
            } else {
                viewArgs = argConstants.toArray(new Object[argConstants.size()]);
            }
        }

        String viewQuery = qb.toString();

        if (argCount == 0) {
            return Query.make(fromQuery.asTable().view(viewQuery, viewArgs));
        }

        if (viewArgs.length == 0) {
            return new Query.Wrapped(fromQuery, argCount) {
                @Override
                public Table asTable(Object... args) {
                    return mFromQuery.asTable(args).view(viewQuery, args);
                }
            };
        }

        return new Query.Wrapped(fromQuery, argCount) {
            @Override
            public Table asTable(Object... args) {
                int argCount = checkArgumentCount(args);
                var fullArgs = new Object[argCount + viewArgs.length];
                System.arraycopy(args, 0, fullArgs, 0, argCount);
                System.arraycopy(viewArgs, 0, fullArgs, argCount, viewArgs.length);
                return mFromQuery.asTable(args).view(viewQuery, fullArgs);
            }
        };
    }
}
