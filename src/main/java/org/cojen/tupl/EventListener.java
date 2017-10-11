/*
 *  Copyright (C) 2011-2017 Cojen.org
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

/**
 * Listener which receives notifications of actions being performed by the
 * database. Implementations must not suspend the calling thread or throw any
 * exceptions.
 *
 * @author Brian S O'Neill
 * @see DatabaseConfig#eventListener
 */
public interface EventListener {
    /**
     * @param message event message format
     * @param args arguments for message
     */
    public void notify(EventType type, String message, Object... args);

    public default boolean isObserved(EventType type) {
        return isObserved(type.category) && isObserved(type.level);
    }

    public default boolean isObserved(EventType.Category category) {
        return true;
    }

    public default boolean isObserved(Level level) {
        return true;
    }

    /**
     * Returns a filtered listener which only observes the given event categories.
     */
    public default EventListener observe(EventType.Category... categories) {
        return AllowEventListener.make(this, categories);
    }

    /**
     * Returns a filtered listener which only observes the given event levels.
     */
    public default EventListener observe(Level... levels) {
        return AllowEventListener.make(this, levels);
    }

    /**
     * Returns a filtered listener which never observes the given event categories.
     */
    public default EventListener ignore(EventType.Category... categories) {
        return DisallowEventListener.make(this, categories);
    }

    /**
     * Returns a filtered listener which never observes the given event levels.
     */
    public default EventListener ignore(Level... levels) {
        return DisallowEventListener.make(this, levels);
    }
}
