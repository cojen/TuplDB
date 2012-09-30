/*
 *  Copyright 2012 Brian S O'Neill
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

import java.io.IOException;

import java.security.GeneralSecurityException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class CryptoPageArray extends PageArray {
    private final PageArray mSource;
    private final Crypto mCrypto;

    CryptoPageArray(PageArray source, Crypto crypto) {
        super(source.pageSize());
        mSource = source;
        mCrypto = crypto;
    }

    @Override
    public PageArray rawPageArray() {
        return mSource;
    }

    @Override
    public boolean isReadOnly() {
        return mSource.isReadOnly();
    }

    @Override
    public boolean isEmpty() throws IOException {
        return mSource.isEmpty();
    }

    @Override
    public long getPageCount() throws IOException {
        return mSource.getPageCount();
    }

    @Override
    public void setPageCount(long count) throws IOException {
        mSource.setPageCount(count);
    }

    @Override
    public void readPage(long index, byte[] buf, int offset) throws IOException {
        try {
            mSource.readPage(index, buf, offset);
            mCrypto.decryptPage(index, mPageSize, buf, offset);
        } catch (GeneralSecurityException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public int readPartial(long index, int start, byte[] buf, int offset, int length)
        throws IOException
    {
        int pageSize = mPageSize;
        if (start == 0 && length == pageSize) {
            readPage(index, buf, offset);
        } else {
            // Page is encrypted as a unit, and so partial read cannot be optimized.
            byte[] page = new byte[pageSize];
            readPage(index, page, 0);
            System.arraycopy(page, start, buf, offset, length);
        }
        return length;
    }

    @Override
    public int readCluster(long index, byte[] buf, int offset, int count) throws IOException {
        int pageSize = mPageSize;
        if (count > 0) while (true) {
            readPage(index, buf, offset);
            if (--count <= 0) {
                break;
            }
            index++;
            offset += pageSize;
        }
        return pageSize * count;
    }

    @Override
    void doWritePage(long index, byte[] buf, int offset) throws IOException {
        try {
            int pageSize = mPageSize;
            // Unknown if buf contents can be destroyed, so create a new one.
            byte[] encrypted = new byte[pageSize];
            mCrypto.encryptPage(index, pageSize, buf, offset, encrypted, 0);
            mSource.doWritePage(index, encrypted, 0);
        } catch (GeneralSecurityException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        mSource.sync(metadata);
    }

    @Override
    public void close(Throwable cause) throws IOException {
        mSource.close(cause);
    }
}
