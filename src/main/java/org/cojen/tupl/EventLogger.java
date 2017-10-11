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

import java.util.logging.Logger;
import java.util.logging.LogRecord;

/**
 * Event listener implementation which passes events to a {@link Logger logger}.
 *
 * @author Brian S O'Neill
 */
public final class EventLogger implements EventListener {
    private final Logger mLogger;

    /**
     * Passes events to the global logger.
     */
    public EventLogger() {
        this(Logger.getGlobal());
    }

    /**
     * Passes events to the given logger.
     */
    public EventLogger(Logger logger) {
        if (logger == null) {
            throw null;
        }
        mLogger = logger;
    }

    @Override
    public void notify(EventType type, String message, Object... args) {
        try {
            if (mLogger.isLoggable(type.level)) {
                String msg = type.category + ": " + String.format(message, args);
                LogRecord record = new LogRecord(type.level, msg);
                record.setSourceClassName(null);
                record.setSourceMethodName(null);
                mLogger.log(record);
            }
        } catch (Throwable e) {
            // Ignore, and so this listener is safe for the caller.
        }
    }
}
