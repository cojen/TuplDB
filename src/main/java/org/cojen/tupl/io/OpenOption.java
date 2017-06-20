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

    /** Indicate that file will be accessed in random order. */
    RANDOM_ACCESS,

    /** Optional hint to perform readahead on the file. */
    READAHEAD,

    /** 
     * Optional hint to apply at file close indicating that the file data will not 
     * be accessed in the near future.
     */
    CLOSE_DONTNEED,
}
