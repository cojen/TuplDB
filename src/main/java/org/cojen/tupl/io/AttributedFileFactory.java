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

    @SuppressWarnings("unchecked")
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
