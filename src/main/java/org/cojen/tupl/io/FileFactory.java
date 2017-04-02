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

import java.io.File;
import java.io.IOException;

/**
 * Provides control over how files and directories are created.
 *
 * @author Brian S O'Neill
 */
public interface FileFactory {
    /**
     * Create the given file for read/write access, unless the file already exists.
     */
    public abstract boolean createFile(File file) throws IOException;

    /**
     * Create the given directory for read/write access, unless the directory already exists.
     */
    public abstract boolean createDirectory(File dir) throws IOException;

    /**
     * Create the given directory path for read/write access, unless the directory path already
     * exists.
     */
    public abstract boolean createDirectories(File dir) throws IOException;
}
