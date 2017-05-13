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

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class SafeEventListener implements EventListener {
    static EventListener makeSafe(EventListener listener) {
        return (listener == null
                || listener instanceof SafeEventListener
                || listener.getClass() == EventPrinter.class) ? listener
            : new SafeEventListener(listener);
    }

    private final EventListener mListener;

    private SafeEventListener(EventListener listener) {
        mListener = listener;
    }

    @Override
    public void notify(EventType type, String message, Object... args) {
        try {
            mListener.notify(type, message, args);
        } catch (Throwable e) {
            // Ignore, and so this listener is safe for the caller.
        }
    }
}
