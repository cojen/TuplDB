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

package org.cojen.tupl.core;

import java.io.PrintStream;

import org.cojen.tupl.Crypto;
import org.cojen.tupl.EventListener;
import org.cojen.tupl.EventType;

/**
 * Debugging tool which reads and prints all the redo operations from a local log.
 *
 * @author Brian S O'Neill
 */
class RedoPrinter implements EventListener {
    /**
     * @param args [0]: base file, [1]: first log number to read from, [2]: optional crypto
     * class; remaining args are passed to its constructor as separate parameters
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        java.io.File baseFile = new java.io.File(args[0]);
        long logId = Long.parseLong(args[1]);

        Crypto crypto = null;
        if (args.length > 2) {
            Class clazz = Class.forName(args[2]);
            Class[] types = new Class[args.length - 3];
            String[] params = new String[types.length];
            for (int i=0; i<types.length; i++) {
                types[i] = String.class;
                params[i] = args[i + 3];
            }
            crypto = (Crypto) clazz.getConstructor(types).newInstance((Object[]) params);
        }

        new RedoLog(crypto, baseFile, null, logId, 0, null)
            .replay(new RedoEventPrinter(new RedoPrinter(), EventType.DEBUG), null, null, null);
    }

    private final PrintStream mOut;

    RedoPrinter() {
        mOut = System.out;
    }

    @Override
    public void notify(EventType type, String message, Object... args) {
        mOut.println(String.format(message, args));
    }
}
