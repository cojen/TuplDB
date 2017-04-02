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

import java.lang.ref.WeakReference;

/**
 * Shutdown operation registered with Checkpointer.
 *
 * @author Brian S O'Neill
 */
interface ShutdownHook {
    void shutdown();

    /**
     * ShutdownHook must not maintain a strong reference to the Database.
     */
    static abstract class Weak<A> extends WeakReference<A> implements ShutdownHook {
        Weak(A obj) {
            super(obj);
        }

        @Override
        public final void shutdown() {
            A obj = get();
            if (obj != null) {
                doShutdown(obj);
            }
        }

        abstract void doShutdown(A obj);
    }
}
