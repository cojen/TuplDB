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

package org.cojen.tupl.ev;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

/**
 * Wraps an EventListener and disallows notifications for specific event categories and
 * levels. Also drops all exceptions that might be thrown by the wrapped EventListener.
 *
 * @author Brian S O'Neill
 */
public final class DisallowEventListener extends FilteredEventListener {
    public static EventListener make(EventListener listener, EventType.Category... categories) {
        if (categories.length == 0) {
            return listener;
        }

        final EventListener original = listener;

        var categorySet = new HashSet<EventType.Category>(Arrays.asList(categories));

        Set<System.Logger.Level> levelSet;
        if (listener instanceof DisallowEventListener disallowed) {
            listener = disallowed.mListener;
            categorySet.addAll(disallowed.mCategories);
            levelSet = disallowed.mLevels;
        } else {
            levelSet = null;
        }

        return make(original, listener, categorySet, levelSet);
    }

    public static EventListener make(EventListener listener, System.Logger.Level... levels) {
        if (levels.length == 0) {
            return listener;
        }

        final EventListener original = listener;

        var levelSet = new HashSet<System.Logger.Level>(Arrays.asList(levels));

        Set<EventType.Category> categorySet;
        if (listener instanceof DisallowEventListener disallowed) {
            listener = disallowed.mListener;
            levelSet.addAll(disallowed.mLevels);
            categorySet = disallowed.mCategories;
        } else {
            categorySet = null;
        }

        return make(original, listener, categorySet, levelSet);
    }

    private static EventListener make(EventListener original, EventListener listener,
                                      Set<EventType.Category> categories,
                                      Set<System.Logger.Level> levels)
    {
        var newListener = new DisallowEventListener(listener, categories, levels);
        return newListener.equals(original) ? original : newListener;
    }

    DisallowEventListener(EventListener listener,
                          Set<EventType.Category> categories, Set<System.Logger.Level> levels)
    {
        super(listener, categories, levels);
    }

    @Override
    public boolean isObserved(EventType.Category category) {
        return mCategories == null || !mCategories.contains(category);
    }

    @Override
    public boolean isObserved(System.Logger.Level level) {
        return mLevels == null || !mLevels.contains(level);
    }

    @Override
    protected boolean sameClass(Object obj) {
        return obj.getClass() == DisallowEventListener.class;
    }
}
