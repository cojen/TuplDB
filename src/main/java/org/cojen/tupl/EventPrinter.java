/*
 *  Copyright 2012-2015 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
}
