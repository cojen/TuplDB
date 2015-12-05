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

/**
 * Listener which receives notifications of actions being performed by the
 * database. Implementations must not suspend the calling thread or throw any
 * exceptions.
 *
 * @author Brian S O'Neill
 * @see DatabaseConfig#eventListener
 */
public interface EventListener {
    /**
     * @param message event message format
     * @param args arguments for message
     */
    public void notify(EventType type, String message, Object... args);
}
