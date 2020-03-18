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

import java.util.function.BiConsumer;

import java.util.logging.Level;

import org.cojen.tupl.EventListener;
import org.cojen.tupl.EventType;

/**
 * Passes events from the replication system (org.cojen.tupl.repl.Replicator) to a database
 * event listener.
 *
 * @author Brian S O'Neill
 */
public final class ReplicationEventListener implements BiConsumer<System.Logger.Level, String> {
    private final EventListener mListener;

    public ReplicationEventListener(EventListener listener) {
        mListener = listener;
    }

    @Override
    public void accept(System.Logger.Level level, String message) {
        EventType type;
        switch (level) {
        case DEBUG:
            type = EventType.REPLICATION_DEBUG;
            break;
        case INFO:
            type = EventType.REPLICATION_INFO;
            break;
        case WARNING:
            type = EventType.REPLICATION_WARNING;
            break;
        case ERROR: default:
            type = EventType.REPLICATION_PANIC;
            break;
        }

        mListener.notify(type, message);
    }
}
