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

package org.cojen.tupl;

import java.io.IOException;

import java.security.GeneralSecurityException;

import org.cojen.tupl.io.PageArray;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class CryptoPageArray extends PageArray {
    private final PageArray mSource;
    private final Crypto mCrypto;

    CryptoPageArray(PageArray source, Crypto crypto) {
        super(source.pageSize());
        mSource = source;
        mCrypto = crypto;
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
    public void readPage(long index, /*P*/ byte[] buf) throws IOException {
        try {
            mSource.readPage(index, buf);
            mCrypto.decryptPage(index, pageSize(), buf, 0);
        } catch (GeneralSecurityException e) {
            throw new DatabaseException(e);
        }
    }

    @Override
    public void readPage(long index, /*P*/ byte[] buf, int offset, int length) throws IOException {
        int pageSize = pageSize();
        if (offset == 0 && length == pageSize) {
            readPage(index, buf);
            return;
        }

        /*P*/ byte[] page = PageOps.p_alloc(pageSize);
        try {
            readPage(index, page);
            PageOps.p_copy(page, 0, buf, offset, length);
        } finally {
            PageOps.p_delete(page);
        }
    }

    @Override
    public void writePage(long index, /*P*/ byte[] buf, int offset) throws IOException {
        try {
            int pageSize = pageSize();
            // Unknown if buf contents can be destroyed, so create a new one.
            /*P*/ byte[] encrypted = PageOps.p_alloc(pageSize);
            try {
                mCrypto.encryptPage(index, pageSize, buf, offset, encrypted, 0);
                mSource.writePage(index, encrypted, 0);
            } finally {
                PageOps.p_delete(encrypted);
            }
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
