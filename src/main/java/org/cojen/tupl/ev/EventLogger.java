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

import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.LogRecord;

import java.util.Objects;

import org.cojen.tupl.EventListener;
import org.cojen.tupl.EventType;

import org.cojen.tupl.io.Utils;

/**
 * Event listener implementation which passes events to a logger.
 *
 * @author Brian S O'Neill
 */
public final class EventLogger implements EventListener {
    private final Object mLogger;

    /**
     * Passes events to the given logger.
     */
    public EventLogger(Logger logger) {
        mLogger = Objects.requireNonNull(logger);
    }

    /**
     * Passes events to the given logger.
     */
    public EventLogger(System.Logger logger) {
        mLogger = Objects.requireNonNull(logger);
    }

    @Override
    public void notify(EventType type, String message, Object... args) {
        try {
            if (mLogger instanceof System.Logger) {
                var logger = (System.Logger) mLogger;
                if (logger.isLoggable(type.level)) {
                    logger.log(type.level, (java.util.ResourceBundle) null,
                               makeMessage(type, message, args), makeThrown(args));
                }
            } else {
                notify((Logger) mLogger, type, message, args);
            }
        } catch (Throwable e) {
            // Ignore, and so this listener is safe for the caller.
        }
    }

    private static void notify(Logger logger,
                               EventType type, String message, Object... args)
    {
        Level level;
        switch (type.level) {
        case ALL:
            level = Level.ALL;
            break;
        case TRACE:
            level = Level.FINER;
            break;
        case DEBUG:
            level = Level.FINE;
            break;
        case INFO:
            level = Level.INFO;
            break;
        case WARNING:
            level = Level.WARNING;
            break;
        case ERROR: default:
            level = Level.SEVERE;
            break;
        case OFF:
            level = Level.OFF;
            break;
        }

        if (logger.isLoggable(level)) {
            var record = new LogRecord(level, makeMessage(type, message, args));
            record.setSourceClassName(null);
            record.setSourceMethodName(null);
            record.setThrown(makeThrown(args));
            logger.log(record);
        }
    }

    private static String makeMessage(EventType type, String message, Object... args) {
        return type.category + ": " + String.format(message, args);
    }

    private static Throwable makeThrown(Object... args) {
        Throwable thrown = null;
        for (Object obj : args) {
            if (obj instanceof Throwable) {
                if (thrown != null) {
                    Utils.suppress(thrown, (Throwable) obj);
                } else {
                    thrown = (Throwable) obj;
                }
            }
        }
        return thrown;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof EventLogger) {
            return mLogger.equals(((EventLogger) obj).mLogger);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return mLogger.hashCode();
    }
}
