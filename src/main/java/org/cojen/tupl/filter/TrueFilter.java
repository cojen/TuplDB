/*
 *  Copyright (C) 2021 Cojen.org
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

package org.cojen.tupl.filter;

/**
 * An empty 'and' filter always evaluates to true.
 *
 * @author Brian S O'Neill
 */
public final class TrueFilter extends AndFilter {
    static final TrueFilter THE = new TrueFilter();

    private TrueFilter() {
        mReduced = true;
    }

    @Override
    public FalseFilter not() {
        return FalseFilter.THE;
    }

    @Override
    public TrueFilter or(RowFilter filter) {
        return this;
    }

    @Override
    public RowFilter and(RowFilter filter) {
        return filter;
    }
}
