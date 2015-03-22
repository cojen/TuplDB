/*
 *  Copyright 2012-2013 Brian S O'Neill
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

import java.util.EnumSet;

/**
 * Basic {@link PageArray} implementation which accesses a file.
 *
 * @author Brian S O'Neill
 */
public class FilePageArray extends PageArray {
    final FileIO mFio;

    public FilePageArray(int pageSize, File file, EnumSet<OpenOption> options) throws IOException {
        this(pageSize, file, null, options);
    }

    public FilePageArray(int pageSize, File file, FileFactory factory,
                         EnumSet<OpenOption> options)
        throws IOException
    {
        super(pageSize);

        if (factory != null
            && options.contains(OpenOption.CREATE) && !options.contains(OpenOption.READ_ONLY))
        {
            factory.createFile(file);
        }

        mFio = JavaFileIO.open(file, options);
    }

    @Override
    public boolean isReadOnly() {
        return mFio.isReadOnly();
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mFio.length() == 0;
    }

    @Override
    public long getPageCount() throws IOException {
        // Always round page count down. A partial last page effectively doesn't exist.
        return mFio.length() / mPageSize;
    }

    @Override
    public void setPageCount(long count) throws IOException {
        if (count < 0) {
            throw new IllegalArgumentException(String.valueOf(count));
        }
        if (isReadOnly()) {
            return;
        }
        mFio.setLength(count * mPageSize);
    }

    @Override
    public void readPage(long index, /*P*/ byte[] buf, int offset, int length) throws IOException {
        if (index < 0) {
            throw new IndexOutOfBoundsException(String.valueOf(index));
        }
        mFio.read(index * mPageSize, buf, offset, length);
    }

    @Override
    public void writePage(long index, /*P*/ byte[] buf, int offset) throws IOException {
        int pageSize = mPageSize;
        mFio.write(index * pageSize, buf, offset, pageSize);
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        mFio.sync(metadata);
        // If mapped, now is a good time to remap if length has changed.
        mFio.remap();
    }

    @Override
    public void close(Throwable cause) throws IOException {
        Utils.close(mFio, cause);
    }
}
