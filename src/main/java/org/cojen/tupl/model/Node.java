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

import java.util.List;

import org.cojen.maker.Label;
import org.cojen.maker.Variable;

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
     * Returns the highest ordinal from all the ParamNodes referenced by this node.
     */
    public abstract int highestParamOrdinal();

    /**
     * Returns true if this node represents a pure function with respect to the current row,
     * returning the same result upon each invocation.
     */
    public abstract boolean isPureFunction();

    /**
     * Returns true if this node represents a filter which only accesses columns and constants,
     * and thus it can be easily converted to a Table query filter expression.
     */
    public boolean isPureFilter() {
        return false;
    }

    /**
     * Returns true if this node can be a term of a pure filter. Applicable to columns,
     * constants, and parameters.
     */
    public boolean isPureFilterTerm() {
        return isPureFilter();
    }

    /**
     * @param query is appended to by this method
     * @param argConstants constants converted to args are added to this list
     * @param argOrdinal the highest arg ordinal so far, is used for constants converted to args
     * @return updated argOrdinal
     * @throw IllegalStateException if isPureFilterTerm returns false
     */
    public int appendPureFilter(StringBuilder query, List<Object> argConstants, int argOrdinal) {
        throw new IllegalStateException();
    }

    /**
     * Generates code which evaluates an expression. The context tracks nodes which have
     * already been evaluated and is updated by this method.
     */
    public abstract Variable makeEval(MakerContext context);

    /**
     * Generates code which evaluates an expression for branching to a pass or fail label.
     * Short-circuit logic is used, and so the expression might only be partially evaluated.
     *
     * @throws IllegalStateException if unsupported
     */
    public void makeFilter(MakerContext context, Label pass, Label fail) {
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
}
