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

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;

import java.util.Set;

/**
 * Applies file attributes to newly created files and directories.
 *
 * @author Brian S O'Neill
 */
public class AttributedFileFactory implements FileFactory {
    private final FileAttribute<?>[] mFileAttrs;
    private final FileAttribute<?>[] mDirAttrs;

    /**
     * @param posixFilePerms permissions to apply to newly created files (ignored if null)
     * @param posixDirPerms permissions to apply to newly created directories (ignored if null)
     */
    public AttributedFileFactory(String posixFilePerms, String posixDirPerms) {
        mFileAttrs = toAttrs(posixFilePerms);
        mDirAttrs = toAttrs(posixDirPerms);
    }

    /**
     * @param fileAttrs attributes to apply to newly created files (ignored if null)
     * @param dirAttrs attributes to apply to newly created directories (ignored if null)
     */
    public AttributedFileFactory(FileAttribute<?>[] fileAttrs, FileAttribute<?>[] dirAttrs) {
        mFileAttrs = copy(fileAttrs);
        mDirAttrs = copy(dirAttrs);
    }

    @Override
    public boolean createFile(File file) throws IOException {
        if (mFileAttrs == null) {
            return file.createNewFile();
        }

        try {
            Files.createFile(file.toPath(), mFileAttrs);
            return true;
        } catch (FileAlreadyExistsException e) {
            return false;
        }
    }

    @Override
    public boolean createDirectory(File dir) throws IOException {
        if (mDirAttrs == null) {
            return dir.mkdir();
        }

        try {
            Files.createDirectory(dir.toPath(), mDirAttrs);
            return true;
        } catch (FileAlreadyExistsException e) {
            return false;
        }
    }

    @Override
    public boolean createDirectories(File dir) throws IOException {
        if (mDirAttrs == null) {
            return dir.mkdirs();
        }

        try {
            Files.createDirectories(dir.toPath(), mDirAttrs);
            return true;
        } catch (FileAlreadyExistsException e) {
            return false;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static FileAttribute<Set<PosixFilePermission>>[] toAttrs(String posixPerms) {
        if (posixPerms == null) {
            return null;
        }

        return new FileAttribute[] {
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString(posixPerms))
        };
    }

    private static FileAttribute<?>[] copy(FileAttribute<?>[] attrs) {
        return (attrs == null || attrs.length == 0) ? null : attrs.clone();
    }
}
