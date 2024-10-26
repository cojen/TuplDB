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

import org.cojen.tupl.table.filter.FalseFilter;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;

/**
 * Defines the number of rows expected to be found in a relation.
 *
 * @author Brian S. O'Neill
 * @see RelationType
 */
public enum Cardinality {
    /** Relation doesn't have any rows. */
    ZERO,

    /** Relation has exactly one row. */
    ONE,

    /** Relation has at most one row. */
    OPTIONAL,

    /** Relation has zero or more rows. */
    MANY;

    public Cardinality multiply(Cardinality c) {
        if (this == ZERO || c == ONE) {
            return this;
        }
        if (c == ZERO || this == ONE) {
            return c;
        }
        if (this == OPTIONAL && c == OPTIONAL) {
            return OPTIONAL;
        }
        return MANY;
    }

    /**
     * Returns an adjusted cardinality, when combined with a filter result.
     */
    public Cardinality filter(Expr filter) {
        if (filter instanceof ConstantExpr ce) {
            Object value = ce.value();
            if (value == Boolean.FALSE) {
                return ZERO;
            } else if (value == Boolean.TRUE) {
                return this;
            }
        }

        return multiply(OPTIONAL);
    }

    /**
     * Returns an adjusted cardinality, when combined with a filter result.
     */
    public Cardinality filter(RowFilter filter) {
        if (filter == TrueFilter.THE) {
            return this;
        } else if (filter == FalseFilter.THE) {
            return ZERO;
        } else {
            return multiply(OPTIONAL);
        }
    }
}
