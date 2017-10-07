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

import java.util.function.BiConsumer;

import java.util.logging.Level;

/**
 * Passes events from the replication system (org.cojen.tupl.repl.Replicator) to a database
 * event listener.
 *
 * @author Brian S O'Neill
 */
final class ReplicationEventListener implements BiConsumer<Level, String> {
    private final EventListener mListener;

    ReplicationEventListener(EventListener listener) {
        mListener = listener;
    }

    @Override
    public void accept(Level level, String message) {
        EventType type;
        int value = level.intValue();

        if (value <= Level.FINE.intValue()) {
            type = EventType.REPLICATION_DEBUG;
        } else if (value <= Level.INFO.intValue()) {
            type = EventType.REPLICATION_INFO;
        } else if (value <= Level.WARNING.intValue()) {
            type = EventType.REPLICATION_WARNING;
        } else if (value < Level.OFF.intValue()) {
            type = EventType.REPLICATION_PANIC;
        } else {
            return;
        }

        mListener.notify(type, message);
    }
}
