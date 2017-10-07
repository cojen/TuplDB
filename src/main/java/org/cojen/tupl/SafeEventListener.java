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
 * 
 *
 * @author Brian S O'Neill
 */
class SafeEventListener implements EventListener {
    static EventListener makeSafe(EventListener listener) {
        return (listener == null
                || listener instanceof SafeEventListener
                || listener.getClass() == EventLogger.class
                || listener.getClass() == EventPrinter.class) ? listener
            : new SafeEventListener(listener);
    }

    final EventListener mListener;

    SafeEventListener(EventListener listener) {
        mListener = listener;
    }

    @Override
    public void notify(EventType type, String message, Object... args) {
        try {
            if (shouldNotify(type)) {
                mListener.notify(type, message, args);
            }
        } catch (Throwable e) {
            // Ignore, and so this listener is safe for the caller.
        }
    }

    @Override
    public boolean isObserved(EventType type) {
        return mListener.isObserved(type);
    }

    @Override
    public boolean isObserved(EventType.Category category) {
        return mListener.isObserved(category);
    }

    @Override
    public boolean isObserved(Level level) {
        return mListener.isObserved(level);
    }

    boolean shouldNotify(EventType type) {
        return true;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj != null && obj.getClass() == SafeEventListener.class) {
            return mListener.equals(((SafeEventListener) obj).mListener);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return mListener.hashCode() + getClass().hashCode();
    }
}
