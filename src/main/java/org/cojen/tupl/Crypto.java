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

package org.cojen.tupl;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.security.GeneralSecurityException;

/**
 * Support for encrypting an entire database.
 *
 * @author Brian S O'Neill
 * @see DatabaseConfig#encrypt
 */
public interface Crypto {
    /**
     * Called by multiple threads to encrypt a fixed-size database
     * page. Encrypted length must exactly match original length.
     *
     * @param pageIndex page index within database
     * @param page initially the original unencrypted page; replaced with encrypted page
     * @param pageOffset offset into page
     */
    public default void encryptPage(long pageIndex, int pageSize, byte[] page, int pageOffset)
        throws GeneralSecurityException
    {
        encryptPage(pageIndex, pageSize, page, pageOffset, page, pageOffset);
    }

    /**
     * Called by multiple threads to encrypt a fixed-size database
     * page. Encrypted length must exactly match original length.
     *
     * @param pageIndex page index within database
     * @param src original unencrypted page
     * @param srcOffset offset into unencrypted page
     * @param dst destination for encrypted page
     * @param dstOffset offset into encrypted page
     */
    public void encryptPage(long pageIndex, int pageSize,
                            byte[] src, int srcOffset, byte[] dst, int dstOffset)
        throws GeneralSecurityException;

    /**
     * Called by multiple threads to encrypt a fixed-size database
     * page. Encrypted length must exactly match original length.
     *
     * @param pageIndex page index within database
     * @param pagePtr initially the original unencrypted page; replaced with encrypted page
     * @param pageOffset offset into page
     */
    public default void encryptPage(long pageIndex, int pageSize, long pagePtr, int pageOffset)
        throws GeneralSecurityException
    {
        encryptPage(pageIndex, pageSize, pagePtr, pageOffset, pagePtr, pageOffset);
    }

    /**
     * Called by multiple threads to encrypt a fixed-size database
     * page. Encrypted length must exactly match original length.
     *
     * @param pageIndex page index within database
     * @param srcPtr original unencrypted page
     * @param srcOffset offset into unencrypted page
     * @param dstPtr destination for encrypted page
     * @param dstOffset offset into encrypted page
     */
    public default void encryptPage(long pageIndex, int pageSize,
                                    long srcPtr, int srcOffset, long dstPtr, int dstOffset)
        throws GeneralSecurityException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Called by multiple threads to decrypt a fixed-size database
     * page. Decrypted length must exactly match encrypted length.
     *
     * @param pageIndex page index within database
     * @param page initially the encrypted page; replaced with decrypted page
     * @param pageOffset offset into page
     */
    public default void decryptPage(long pageIndex, int pageSize, byte[] page, int pageOffset)
        throws GeneralSecurityException
    {
        decryptPage(pageIndex, pageSize, page, pageOffset, page, pageOffset);
    }

    /**
     * Called by multiple threads to decrypt a fixed-size database
     * page. Decrypted length must exactly match encrypted length.
     *
     * @param pageIndex page index within database
     * @param src encrypted page
     * @param srcOffset offset into encrypted page
     * @param dst destination for decrypted page
     * @param dstOffset offset into decrypted page
     */
    public void decryptPage(long pageIndex, int pageSize,
                            byte[] src, int srcOffset, byte[] dst, int dstOffset)
        throws GeneralSecurityException;

    /**
     * Called by multiple threads to decrypt a fixed-size database
     * page. Decrypted length must exactly match encrypted length.
     *
     * @param pageIndex page index within database
     * @param pagePtr initially the encrypted page; replaced with decrypted page
     * @param pageOffset offset into page
     */
    public default void decryptPage(long pageIndex, int pageSize, long pagePtr, int pageOffset)
        throws GeneralSecurityException
    {
        decryptPage(pageIndex, pageSize, pagePtr, pageOffset, pagePtr, pageOffset);
    }

    /**
     * Called by multiple threads to decrypt a fixed-size database
     * page. Decrypted length must exactly match encrypted length.
     *
     * @param pageIndex page index within database
     * @param srcPtr encrypted page
     * @param srcOffset offset into encrypted page
     * @param dstPtr destination for decrypted page
     * @param dstOffset offset into decrypted page
     */
    public default void decryptPage(long pageIndex, int pageSize,
                                    long srcPtr, int srcOffset, long dstPtr, int dstOffset)
        throws GeneralSecurityException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Called to wrap an OutputStream for supporting encryption. Implementation
     * of this method must be thread-safe, but the stream doesn't need to be.
     *
     * @param id stream identifier
     * @param out encrypted data destination
     * @return stream which encrypts all data
     */
    public OutputStream newEncryptingStream(long id, OutputStream out)
        throws GeneralSecurityException, IOException;

    /**
     * Called to wrap an InputStream for supporting decryption. Implementation
     * of this method must be thread-safe, but the stream doesn't need to be.
     *
     * @param id stream identifier
     * @param in encrypted data source
     * @return stream which decrypts all data
     */
    public InputStream newDecryptingStream(long id, InputStream in)
        throws GeneralSecurityException, IOException;
}
