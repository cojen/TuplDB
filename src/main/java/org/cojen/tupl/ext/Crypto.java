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

package org.cojen.tupl.ext;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.security.GeneralSecurityException;

import org.cojen.tupl.DatabaseConfig;

/**
 * Support for encrypting an entire database.
 *
 * @author Brian S O'Neill
 * @see DatabaseConfig#encrypt
 */
public interface Crypto {
    /**
     * Called by multiple threads to encrypt a fixed-size database page. Encrypted length must
     * exactly match original length.
     *
     * @param pageIndex page index within database
     * @param pageAddr initially the original unencrypted page; replaced with encrypted page
     * @param pageOffset offset into page
     */
    public default void encryptPage(long pageIndex, int pageSize, long pageAddr, int pageOffset)
        throws GeneralSecurityException
    {
        encryptPage(pageIndex, pageSize, pageAddr, pageOffset, pageAddr, pageOffset);
    }

    /**
     * Called by multiple threads to encrypt a fixed-size database page. Encrypted length must
     * exactly match original length.
     *
     * @param pageIndex page index within database
     * @param srcAddr original unencrypted page
     * @param srcOffset offset into unencrypted page
     * @param dstAddr destination for encrypted page
     * @param dstOffset offset into encrypted page
     */
    public default void encryptPage(long pageIndex, int pageSize,
                                    long srcAddr, int srcOffset, long dstAddr, int dstOffset)
        throws GeneralSecurityException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Called by multiple threads to decrypt a fixed-size database page. Decrypted length must
     * exactly match encrypted length.
     *
     * @param pageIndex page index within database
     * @param pageAddr initially the encrypted page; replaced with decrypted page
     * @param pageOffset offset into page
     */
    public default void decryptPage(long pageIndex, int pageSize, long pageAddr, int pageOffset)
        throws GeneralSecurityException
    {
        decryptPage(pageIndex, pageSize, pageAddr, pageOffset, pageAddr, pageOffset);
    }

    /**
     * Called by multiple threads to decrypt a fixed-size database page. Decrypted length must
     * exactly match encrypted length.
     *
     * @param pageIndex page index within database
     * @param srcAddr encrypted page
     * @param srcOffset offset into encrypted page
     * @param dstAddr destination for decrypted page
     * @param dstOffset offset into decrypted page
     */
    public default void decryptPage(long pageIndex, int pageSize,
                                    long srcAddr, int srcOffset, long dstAddr, int dstOffset)
        throws GeneralSecurityException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Called to wrap an OutputStream for supporting encryption. Implementation of this method
     * must be thread-safe, but the stream doesn't need to be.
     *
     * @param out encrypted data destination
     * @return stream which encrypts all data
     */
    public OutputStream newEncryptingStream(OutputStream out)
        throws GeneralSecurityException, IOException;

    /**
     * Called to wrap an InputStream for supporting decryption. Implementation of this method
     * must be thread-safe, but the stream doesn't need to be.
     *
     * @param in encrypted data source
     * @return stream which decrypts all data
     */
    public InputStream newDecryptingStream(InputStream in)
        throws GeneralSecurityException, IOException;
}
