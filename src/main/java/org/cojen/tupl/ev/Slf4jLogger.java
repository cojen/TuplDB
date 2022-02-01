/*
 *  Copyright (C) 2020 Cojen.org
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

import java.util.Objects;

import org.slf4j.Logger;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

import org.cojen.tupl.io.Utils;

/**
 * Event listener implementation which passes events to a logger.
 *
 * @author Brian S O'Neill
 */
public final class Slf4jLogger implements EventListener {
    private final Logger mLogger;

    /**
     * Passes events to the given logger.
     */
    public Slf4jLogger(Logger logger) {
        mLogger = Objects.requireNonNull(logger);
    }

    @Override
    public void notify(EventType type, String message, Object... args) {
        try {
            var logger = mLogger;

            switch (type.level) {
            case ALL: case TRACE:
                if (logger.isTraceEnabled()) {
                    logger.trace(makeMessage(type, message, args), makeThrown(args));
                }
                break;
            case DEBUG:
                if (logger.isDebugEnabled()) {
                    logger.debug(makeMessage(type, message, args), makeThrown(args));
                }
                break;
            case INFO:
                if (logger.isInfoEnabled()) {
                    logger.info(makeMessage(type, message, args), makeThrown(args));
                }
                break;
            case WARNING:
                if (logger.isWarnEnabled()) {
                    logger.warn(makeMessage(type, message, args), makeThrown(args));
                }
                break;
            case ERROR: default:
                if (logger.isErrorEnabled()) {
                    logger.error(makeMessage(type, message, args), makeThrown(args));
                }
                break;
            case OFF:
                break;
            }
        } catch (Throwable e) {
            // Ignore, and so this listener is safe for the caller.
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
        return obj == this || obj instanceof Slf4jLogger sl && mLogger.equals(sl.mLogger);
    }

    @Override
    public int hashCode() {
        return mLogger.hashCode();
    }
}
