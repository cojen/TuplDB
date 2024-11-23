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

package org.cojen.tupl.core;

import java.io.IOException;

import java.security.GeneralSecurityException;

import org.cojen.tupl.CorruptDatabaseException;

import org.cojen.tupl.ext.Crypto;

import org.cojen.tupl.io.PageArray;
import org.cojen.tupl.io.Utils;

/**
 * Wraps a PageArray to apply encryption operations on all pages. Is constructed by {@link
 * StoredPageDb} when encryption is enabled via {@link DatabaseConfig#encrypt}.
 *
 * @author Brian S O'Neill
 */
final class CryptoPageArray extends TransformedPageArray {
    private final Crypto mCrypto;

    CryptoPageArray(PageArray source, Crypto crypto) {
        super(source);
        mCrypto = crypto;
    }

    @Override
    public int directPageSize() {
        return mSource.directPageSize();
    }

    @Override
    public boolean isFullyMapped() {
        return mSource.isFullyMapped();
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
    public long pageCount() throws IOException {
        return mSource.pageCount();
    }

    @Override
    public void truncatePageCount(long count) throws IOException {
        mSource.truncatePageCount(count);
    }

    @Override
    public void expandPageCount(long count) throws IOException {
        mSource.expandPageCount(count);
    }

    @Override
    public long pageCountLimit() throws IOException {
        return mSource.pageCountLimit();
    }

    @Override
    public void readPage(long index, byte[] dst) throws IOException {
        try {
            mSource.readPage(index, dst);
            mCrypto.decryptPage(index, pageSize(), dst, 0);
        } catch (GeneralSecurityException e) {
            throw new CorruptDatabaseException(e);
        }
    }

    @Override
    public void readPage(long index, byte[] dst, int offset, int length) throws IOException {
        int pageSize = pageSize();
        if (offset == 0 && length == pageSize) {
            readPage(index, dst);
            return;
        }

        var page = new byte[pageSize];

        readPage(index, page);
        System.arraycopy(page, 0, dst, offset, length);
    }

    @Override
    public void readPage(long index, long dstAddr) throws IOException {
        try {
            mSource.readPage(index, dstAddr);
            mCrypto.decryptPage(index, pageSize(), dstAddr, 0);
        } catch (GeneralSecurityException e) {
            throw new CorruptDatabaseException(e);
        }
    }

    @Override
    public void readPage(long index, long dstAddr, int offset, int length) throws IOException {
        int pageSize = pageSize();
        if (offset == 0 && length == pageSize) {
            readPage(index, dstAddr);
            return;
        }

        long page = DirectPageOps.p_allocPage(mSource.directPageSize());
        try {
            readPage(index, page);
            DirectPageOps.p_copy(page, 0, dstAddr, offset, length);
        } finally {
            DirectPageOps.p_delete(page);
        }
    }

    @Override
    public void writePage(long index, byte[] src, int offset) throws IOException {
        try {
            int pageSize = pageSize();
            // Unknown if source contents can be destroyed, so create a new one.
            var encrypted = new byte[pageSize];

            mCrypto.encryptPage(index, pageSize, src, offset, encrypted, 0);
            mSource.writePage(index, encrypted, 0);
        } catch (GeneralSecurityException e) {
            throw new CorruptDatabaseException(e);
        }
    }

    @Override
    public void writePage(long index, long srcAddr, int offset) throws IOException {
        try {
            int pageSize = pageSize();
            // Unknown if source contents can be destroyed, so create a new one.
            long encrypted = DirectPageOps.p_allocPage(mSource.directPageSize());
            try {
                mCrypto.encryptPage(index, pageSize, srcAddr, offset, encrypted, 0);
                mSource.writePage(index, encrypted, 0);
            } finally {
                DirectPageOps.p_delete(encrypted);
            }
        } catch (GeneralSecurityException e) {
            throw new CorruptDatabaseException(e);
        }
    }

    @Override
    public byte[] evictPage(long index, byte[] buf) throws IOException {
        try {
            // Page is being evicted, and so buf contents can be destroyed.
            mCrypto.encryptPage(index, pageSize(), buf, 0);
        } catch (GeneralSecurityException e) {
            throw new CorruptDatabaseException(e);
        }

        try {
            return mSource.evictPage(index, buf);
        } catch (Throwable e) {
            // Oops, better restore the page.
            try {
                mCrypto.decryptPage(index, pageSize(), buf, 0);
            } catch (Throwable e2) {
                // Time to panic.
                Utils.closeQuietly(mSource, e2);
            }
            throw e;
        }
    }

    @Override
    public long evictPage(long index, long bufAddr) throws IOException {
        try {
            // Page is being evicted, and so buf contents can be destroyed.
            mCrypto.encryptPage(index, pageSize(), bufAddr, 0);
        } catch (GeneralSecurityException e) {
            throw new CorruptDatabaseException(e);
        }

        try {
            return mSource.evictPage(index, bufAddr);
        } catch (Throwable e) {
            // Oops, better restore the page.
            try {
                mCrypto.decryptPage(index, pageSize(), bufAddr, 0);
            } catch (Throwable e2) {
                // Time to panic.
                Utils.closeQuietly(mSource, e2);
            }
            throw e;
        }
    }

    @Override
    public void sync(boolean metadata) throws IOException {
        mSource.sync(metadata);
    }

    @Override
    public void syncPage(long index) throws IOException {
        mSource.syncPage(index);
    }

    @Override
    public void close(Throwable cause) throws IOException {
        mSource.close(cause);
    }
}
