/*
 *  Copyright 2014-2015 Cojen.org
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
 * Thrown when database is full and no more data can be stored into it.
 *
 * @author Brian S O'Neill
 */
public class DatabaseFullException extends DatabaseException {
    private static final long serialVersionUID = 1L;

    public DatabaseFullException() {
    }

    public DatabaseFullException(Throwable cause) {
        super(cause);
    }

    public DatabaseFullException(String message) {
        super(message);
    }

    @Override
    boolean isRecoverable() {
        return true;
    }
}
