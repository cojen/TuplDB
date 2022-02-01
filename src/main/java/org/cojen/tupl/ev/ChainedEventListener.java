/*
 *  Copyright 2019 Cojen.org
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

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

/**
 * Calls a chain of EventListeners, in sequential order.
 *
 * @author Brian S O'Neill
 */
public class ChainedEventListener extends SafeEventListener {
    public static EventListener make(EventListener... listeners) {
        if (listeners == null || listeners.length == 0) {
            return null;
        }

        int count = 0;
        EventListener first = null;
        for (EventListener listener : listeners) {
            if (listener != null) {
                count++;
                if (first == null) {
                    first = listener;
                }
            }
        }

        if (count <= 1) {
            return first;
        }

        var rest = new EventListener[count - 1];
        for (int i=0, j=0; i<listeners.length; i++) {
            EventListener listener = listeners[i];
            if (listener != null && listener != first) {
                rest[j++] = listener;
            }
        }

        return new ChainedEventListener(first, rest);
    }

    private final EventListener[] mRest;

    ChainedEventListener(EventListener first, EventListener... rest) {
        super(first);
        mRest = rest;
    }

    @Override
    public void notify(EventType type, String message, Object... args) {
        try {
            if (shouldNotify(type)) {
                mListener.notify(type, message, args);

                for (EventListener listener : mRest) {
                    try {
                        listener.notify(type, message, args);
                    } catch (Throwable e) {
                        // Ignore, and so this listener is safe for the caller.
                    }
                }
            }
        } catch (Throwable e) {
            // Ignore, and so this listener is safe for the caller.
        }
    }
}
