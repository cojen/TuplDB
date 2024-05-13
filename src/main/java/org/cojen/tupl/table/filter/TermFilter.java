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

package org.cojen.tupl.table.filter;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public abstract sealed class TermFilter extends RowFilter permits ColumnFilter, ExprFilter {
    TermFilter(int hash) {
        super(hash);
    }

    @Override
    public final int numTerms() {
        return 1;
    }

    @Override
    public final RowFilter reduce() {
        return this;
    }

    @Override
    final RowFilter reduce(long limit, boolean merge) {
        return this;
    }

    @Override
    public final boolean isDnf() {
        return true;
    }

    @Override
    public final RowFilter dnf() {
        return this;
    }

    @Override
    final RowFilter dnf(long limit, boolean merge) {
        return this;
    }

    @Override
    public final boolean isCnf() {
        return true;
    }

    @Override
    public final RowFilter cnf() {
        return this;
    }

    @Override
    final RowFilter cnf(long limit, boolean merge) {
        return this;
    }

    @Override
    public final int isSubMatch(RowFilter filter) {
        return isMatch(filter);
    }

    @Override
    public final RowFilter sort() {
        return this;
    }
}
