/*
 *  Copyright 2016 Cojen.org
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
