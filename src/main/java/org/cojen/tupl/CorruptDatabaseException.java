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

/**
 * Thrown when the internal structure of the {@link Database database} is
 * corrupt.
 *
 * @author Brian S O'Neill
 */
public class CorruptDatabaseException extends DatabaseException {
    private static final long serialVersionUID = 1L;

    public CorruptDatabaseException(String message) {
        super(message);
    }

    public CorruptDatabaseException(Throwable cause) {
        super(cause);
    }
}
