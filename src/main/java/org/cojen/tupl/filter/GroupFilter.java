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

import java.util.Arrays;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public abstract class GroupFilter extends RowFilter {
    final RowFilter[] mSubFilters;

    GroupFilter(int hash, RowFilter... subFilters) {
        super(hash);
        mSubFilters = subFilters;
    }

    @Override
    public final boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof GroupFilter) {
            var other = (GroupFilter) obj;
            return Arrays.equals(mSubFilters, other.mSubFilters) && opChar() == other.opChar();
        }
        return false;
    }

    public RowFilter[] subFilters() {
        return mSubFilters;
    }

    @Override
    void appendTo(StringBuilder b) {
        char opChar = opChar();
        for (int i=0; i<mSubFilters.length; i++) {
            if (i != 0) {
                b.append(' ').append(opChar).append(opChar).append(' ');
            }
            RowFilter sub = mSubFilters[i];
            if (sub instanceof GroupFilter) {
                b.append('(');
                sub.appendTo(b);
                b.append(')');
            } else {
                sub.appendTo(b);
            }
        }
    }

    abstract char opChar();

    RowFilter[] subNot() {
        RowFilter[] subFilters = mSubFilters.clone();
        for (int i=0; i<subFilters.length; i++) {
            subFilters[i] = subFilters[i].not();
        }
        return subFilters;
    }
}
