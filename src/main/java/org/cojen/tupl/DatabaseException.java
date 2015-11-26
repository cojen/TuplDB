/*
 *  Copyright 2011-2015 Cojen.org
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

import java.io.IOException;

import java.util.concurrent.TimeUnit;

/**
 * Exception thrown which indicates a {@link Database database} problem not due
 * to general I/O problems.
 *
 * @author Brian S O'Neill
 */
public class DatabaseException extends IOException {
    private static final long serialVersionUID = 1L;

    public DatabaseException() {
    }

    public DatabaseException(String message) {
        super(message);
    }

    public DatabaseException(Throwable cause) {
        super(cause);
    }

    public DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Returns false if database should be closed as a result of this exception.
     */
    boolean isRecoverable() {
        return false;
    }

    /*
     * Applicable to timeout exceptions.
     */
    long getTimeout() {
        return 0;
    }

    /*
     * Applicable to timeout exceptions.
     */
    TimeUnit getUnit() {
        return null;
    }

    /**
     * Rethrows if given a recoverable exception.
     */
    static void rethrowIfRecoverable(Throwable e) throws DatabaseException {
        if (e instanceof DatabaseException) {
            DatabaseException de = (DatabaseException) e;
            if (de.isRecoverable()) {
                throw de;
            }
        }
    }
}
