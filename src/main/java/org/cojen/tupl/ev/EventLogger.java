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

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

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
            if (mLogger instanceof System.Logger logger) {
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
        final Level level = switch (type.level) {
            case ALL -> Level.ALL;
            case TRACE -> Level.FINER;
            case DEBUG -> Level.FINE;
            case INFO -> Level.INFO;
            case WARNING -> Level.WARNING;
            default -> Level.SEVERE;
            case OFF -> Level.OFF;
        };

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
            if (obj instanceof Throwable t) {
                if (thrown != null) {
                    Utils.suppress(thrown, t);
                } else {
                    thrown = t;
                }
            }
        }
        return thrown;
    }

    @Override
    public boolean equals(Object obj) {
        return obj == this || obj instanceof EventLogger el && mLogger.equals(el.mLogger);
    }

    @Override
    public int hashCode() {
        return mLogger.hashCode();
    }
}
