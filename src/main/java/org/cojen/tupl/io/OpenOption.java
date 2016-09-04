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

package org.cojen.tupl.io;

/**
 * Open options for {@link FilePageArray}.
 *
 * @author Brian S O'Neill
 */
public enum OpenOption {
    /** Open file in read-only mode. */
    READ_ONLY,

    /** Create the file if it doesn't already exist. */
    CREATE,

    /** Map the file into main memory. */
    MAPPED,

    /** All file I/O should be durable. */
    SYNC_IO,

    /** All file I/O should be durable and bypass the file system cache, if possible. */
    DIRECT_IO,

    /** File contents don't persist after an OS crash or power failure. */
    NON_DURABLE,

    /** Preallocate file blocks when increasing file length, if possible. */
    PREALLOCATE,
}
