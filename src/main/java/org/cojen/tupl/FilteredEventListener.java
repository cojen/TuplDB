/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl;

import java.util.logging.Level;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * 
 *
 * @author Brian S O'Neill
 */
abstract class FilteredEventListener extends SafeEventListener {
    protected final Set<EventType.Category> mCategories;
    protected final Set<Level> mLevels;

    FilteredEventListener(EventListener listener,
                          Set<EventType.Category> categories, Set<Level> levels)
    {
        super(listener);
        mCategories = categories;
        mLevels = levels;
    }

    @Override
    final boolean shouldNotify(EventType type) {
        return isObserved(type.category) && isObserved(type.level);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj != null && sameClass(obj)) {
            FilteredEventListener other = (FilteredEventListener) obj;
            return mListener.equals(other.mListener)
                && Objects.equals(mCategories, other.mCategories)
                && Objects.equals(mLevels, other.mLevels);
        }
        return false;
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        hash = hash * 31 + Objects.hashCode(mCategories);
        hash = hash * 31 + Objects.hashCode(mLevels);
        return hash;
    }

    protected abstract boolean sameClass(Object obj);
}
