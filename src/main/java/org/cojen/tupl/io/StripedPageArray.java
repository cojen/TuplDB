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

import java.io.IOException;

/**
 * {@link PageArray} implementation which stripes pages in a <a
 * href="http://en.wikipedia.org/wiki/Raid_0#RAID_0">RAID 0</a> fashion.
 *
 * @author Brian S O'Neill
 */
public class StripedPageArray extends PageArray {
    private final PageArray[] mArrays;
    private final boolean mReadOnly;

    public StripedPageArray(PageArray... arrays) {
        super(pageSize(arrays));
        mArrays = arrays;
        boolean readOnly = false;
        for (PageArray pa : arrays) {
            readOnly |= pa.isReadOnly();
        }
        mReadOnly = readOnly;
    }

    private static int pageSize(PageArray... arrays) {
        int pageSize = arrays[0].pageSize();
        for (int i=1; i<arrays.length; i++) {
            if (arrays[i].pageSize() != pageSize) {
                throw new IllegalArgumentException("Inconsistent page sizes");
            }
        }
        return pageSize;
    }

    @Override
    public boolean isReadOnly() {
        return mReadOnly;
    }

    @Override
    public boolean isEmpty() throws IOException {
        for (PageArray pa : mArrays) {
            if (!pa.isEmpty()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public long getPageCount() throws IOException {
        long count = 0;
        for (PageArray pa : mArrays) {
            count += pa.getPageCount();
        }
        return count;
    }

    @Override
    public void setPageCount(long count) throws IOException {
        int stripes = mArrays.length;
        // Divide among stripes, rounding up.
        count = (count + stripes - 1) / stripes;
        for (PageArray pa : mArrays) {
            pa.setPageCount(count);
        }
    }

    @Override
    public void readPage(long index, byte[] buf, int offset) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        arrays[(int) (index % stripes)].readPage(index / stripes, buf, offset);
    }

    @Override
    public int readPartial(long index, int start, byte[] buf, int offset, int length)
        throws IOException
    {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        return arrays[(int) (index % stripes)]
            .readPartial(index / stripes, start, buf, offset, length);
    }

    /*
    @Override
    public int readCluster(long index, byte[] buf, int offset, int count)
        throws IOException
    {
        if (count <= 0) {
            return 0;
        }

        int pageSize = pageSize();
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        int stripe = (int) (index % stripes);
        index /= stripes;

        while (true) {
            arrays[stripe].readPage(index, buf, offset);
            if (--count < 0) {
                break;
            }
            offset += pageSize;
            if (++stripe == stripes) {
                stripe = 0;
                index++;
            }
        }

        return pageSize * count;
    }
    */

    @Override
    public void writePage(long index, byte[] buf, int offset) throws IOException {
        PageArray[] arrays = mArrays;
        int stripes = arrays.length;
        arrays[(int) (index % stripes)].writePage(index / stripes, buf, offset);
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        for (PageArray pa : mArrays) {
            pa.sync(metadata);
        }
    }

    @Override
    public void close(Throwable cause) throws IOException {
        IOException ex = null;
        for (PageArray pa : mArrays) {
            ex = Utils.closeQuietly(ex, pa, cause);
        }
        if (ex != null) {
            throw ex;
        }
    }
}
