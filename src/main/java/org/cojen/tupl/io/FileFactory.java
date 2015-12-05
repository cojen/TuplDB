/*
 *  Copyright 2013-2015 Cojen.org
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
