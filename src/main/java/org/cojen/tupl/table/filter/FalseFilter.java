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

package org.cojen.tupl.table.filter;

/**
 * An empty "or" filter which always evaluates to false.
 *
 * @author Brian S O'Neill
 */
public final class FalseFilter extends OrFilter {
    public static final FalseFilter THE = new FalseFilter();

    private FalseFilter() {
        mFlags = FLAG_DNF_SET | FLAG_IS_DNF | FLAG_CNF_SET | FLAG_IS_CNF;
        mReduced = this;
        mDnf = this;
        mCnf = this;
    }

    @Override
    public TrueFilter not() {
        return TrueFilter.THE;
    }

    @Override
    public RowFilter or(RowFilter filter) {
        return filter;
    }

    @Override
    public FalseFilter and(RowFilter filter) {
        return this;
    }
}
