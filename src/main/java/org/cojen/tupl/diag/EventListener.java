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

package org.cojen.tupl.diag;

import org.cojen.tupl.DatabaseConfig;

import org.cojen.tupl.ev.AllowEventListener;
import org.cojen.tupl.ev.DisallowEventListener;
import org.cojen.tupl.ev.EventLogger;
import org.cojen.tupl.ev.EventPrinter;
import org.cojen.tupl.ev.Slf4jLogger;

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
     * Returns a new listener that prints messages to the given stream.
     */
    public static EventListener printTo(java.io.PrintStream out) {
        return new EventPrinter(out);
    }

    /**
     * Returns a new listener that logs messages to the given logger.
     */
    public static EventListener logTo(java.util.logging.Logger logger) {
        return new EventLogger(logger);
    }

    /**
     * Returns a new listener that logs messages to the given logger.
     */
    public static EventListener logTo(System.Logger logger) {
        return new EventLogger(logger);
    }

    /**
     * Returns a new listener that logs messages to the given logger.
     */
    public static EventListener logTo(org.slf4j.Logger logger) {
        return new Slf4jLogger(logger);
    }

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

    public default boolean isObserved(System.Logger.Level level) {
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
    public default EventListener observe(System.Logger.Level... levels) {
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
    public default EventListener ignore(System.Logger.Level... levels) {
        return DisallowEventListener.make(this, levels);
    }
}
