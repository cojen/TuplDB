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

import java.io.PrintStream;

/**
 * Event listener implementation which prints events to an output stream.
 *
 * @author Brian S O'Neill
 */
public class EventPrinter implements EventListener {
    private final PrintStream mOut;

    /**
     * Prints events to standard out.
     */
    public EventPrinter() {
        this(System.out);
    }

    /**
     * Prints events to the given stream.
     */
    public EventPrinter(PrintStream out) {
        if (out == null) {
            throw null;
        }
        mOut = out;
    }

    @Override
    public void notify(EventType type, String message, Object... args) {
        try {
            mOut.println(type.category + ": " + String.format(message, args));
        } catch (Throwable e) {
            // Ignore, and so this listener is safe for the caller.
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof EventPrinter) {
            return mOut.equals(((EventPrinter) obj).mOut);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return mOut.hashCode();
    }
}
