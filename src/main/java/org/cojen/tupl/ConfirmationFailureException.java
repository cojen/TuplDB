/*
 *  Copyright 2013 Brian S O'Neill
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

import org.cojen.tupl.ext.ReplicationManager;

/**
 * Thrown by {@link ReplicationManager} when replication cannot be confirmed.
 *
 * @author Brian S O'Neill
 */
public class ConfirmationFailureException extends DatabaseException {
    private static final long serialVersionUID = 1L;

    public ConfirmationFailureException() {
    }

    public ConfirmationFailureException(String message) {
        super(message);
    }

    public ConfirmationFailureException(Throwable cause) {
        super(cause);
    }

    public ConfirmationFailureException(String message, Throwable cause) {
        super(message, cause);
    }

    @Override
    boolean isRecoverable() {
        return true;
    }
}
