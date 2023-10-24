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

import org.cojen.maker.Label;
import org.cojen.maker.Variable;

import org.cojen.tupl.rows.RowInfo;

import org.cojen.tupl.rows.filter.OpaqueFilter;
import org.cojen.tupl.rows.filter.RowFilter;

/**
 * A node represents an AST element of a query.
 *
 * @author Brian S. O'Neill
 */
public abstract class Node {
    public abstract Type type();

    /**
     * @return this or a replacement node
     */
    public abstract Node asType(Type type);

    /**
     * Returns a non-null name, which doesn't affect the functionality of this node.
     *
     * @see #equals
     */
    public abstract String name();

    /**
     * Returns the highest query argument needed by this node, which is zero of none are
     * needed.
     */
    public abstract int maxArgument();

    /**
     * Returns true if this node represents a pure function with respect to the current row,
     * returning the same result upon each invocation.
     */
    public abstract boolean isPureFunction();

    /**
     * Performs best effert conversion of this node into a RowFilter. And nodes which cannot be
     * converted are represented by OpaqueFilters which have the node attached.
     */
    public RowFilter toFilter(RowInfo info) {
        return new OpaqueFilter(false, this);
    }

    /**
     * Generates code which evaluates an expression. The context tracks nodes which have
     * already been evaluated and is updated by this method.
     */
    public abstract Variable makeEval(EvalContext context);

    /**
     * Generates code which evaluates an expression for branching to a pass or fail label.
     * Short-circuit logic is used, and so the expression might only be partially evaluated.
     *
     * @throws IllegalStateException if unsupported
     * @see FilterVisitor
     */
    public void makeFilter(EvalContext context, Label pass, Label fail) {
        makeEval(context).ifTrue(pass);
        fail.goto_();
    }

    /**
     * @see #equals
     */
    @Override
    public abstract int hashCode();

    /**
     * The equals and hashCode methods only compare for equivalent functionality, and thus the
     * node's name is generally excluded from the comparison.
     */
    @Override
    public abstract boolean equals(Object obj);

    @Override
    public String toString() {
        return name();
    }
}
