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

package org.cojen.tupl.filter;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see Parser
 */
public abstract class RowFilter {
    private final int mHash;

    RowFilter(int hash) {
        mHash = hash;
    }

    public abstract void accept(Visitor visitor);

    /**
     * Apply partial or full reduction of the filter.
     */
    public abstract RowFilter reduce();

    /**
     * @return true if this filter is in disjunctive normal form
     */
    public abstract boolean isDnf();

    /**
     * Returns this filter in disjunctive normal form.
     */
    public abstract RowFilter dnf();

    /**
     * @return true if this filter is in conjunctive normal form
     */
    public abstract boolean isCnf();

    /**
     * Returns this filter in conjunctive normal form.
     */
    public abstract RowFilter cnf();

    /**
     * Checks if the given filter (or its inverse) matches this one. If the filter consists of
     * a commutative group of sub-filters, then exact order isn't required for a match.
     *
     * @return 0 if doesn't match, 1 if equal match, or -1 if inverse equally matches
     */
    public abstract int isMatch(RowFilter filter);

    /**
     * Checks if the given filter (or its inverse) matches this one, or if the given filter
     * matches against a sub-filter of this one.
     *
     * @return 0 if doesn't match, 1 if equal match, or -1 if inverse equally matches
     */
    public abstract int isSubMatch(RowFilter filter);

    /**
     * Returns a hash code for use with the isMatch method.
     */
    public abstract int matchHashCode();

    /**
     * Returns the inverse of this filter.
     */
    public abstract RowFilter not();

    @Override
    public final int hashCode() {
        return mHash;
    }

    @Override
    public final String toString() {
        var b = new StringBuilder();
        appendTo(b);
        return b.toString();
    }

    abstract void appendTo(StringBuilder b);
}
