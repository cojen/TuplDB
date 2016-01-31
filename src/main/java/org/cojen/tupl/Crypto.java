/*
 *  Copyright 2012-2015 Cojen.org
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
