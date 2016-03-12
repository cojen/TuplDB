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

/**
 * Used by DurablePageDb.
 *
 * @author Brian S O'Neill
 */
class WrongPageSize extends Exception {
    private static final long serialVersionUID = 1L;

    final int mExpected;
    final int mActual;

    WrongPageSize(int expected, int actual) {
        mExpected = expected;
        mActual = actual;
    }

    @Override
    public Throwable fillInStackTrace() {
        return this;
    }

    DatabaseException rethrow() throws DatabaseException {
        throw new DatabaseException
            ("Actual page size does not match configured page size: "
             + mActual + " != " + mExpected);
    }
}
