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

import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.security.GeneralSecurityException;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.cojen.tupl.io.DirectAccess;

/**
 * Crypto implementation which uses {@link Cipher} and defaults to the AES algorithm with a
 * 128-bit key.
 *
 * @author Brian S O'Neill
 */
public class CipherCrypto implements Crypto {
    /**
     * Generates and prints a new key.
     */
    public static void main(String[] args) throws Exception {
        System.out.println(toString(new CipherCrypto().secretKey()));
    }

    private final ThreadLocal<Cipher> mHeaderPageCipher = new ThreadLocal<>();
    private final ThreadLocal<Cipher> mDataPageCipher = new ThreadLocal<>();
    private final SecretKey mRootKey;
    private final boolean mIsNewKey;

    private volatile byte[] mDataIvSalt;
    private volatile SecretKey mDataKey;


    /**
     * Construct with a new key, available from the {@link #secretKey secretKey} method.
     */
    public CipherCrypto() throws GeneralSecurityException {
        this(null, null);
    }

    /**
     * Construct with an existing key, which is wrapped with {@link SecretKeySpec}.
     */
    public CipherCrypto(byte[] encodedKey) throws GeneralSecurityException {
        this(null, encodedKey);
    }

    /**
     * Construct with an existing key.
     */
    public CipherCrypto(SecretKey key) throws GeneralSecurityException {
        this(key, null);
    }

    private CipherCrypto(SecretKey key, byte[] encodedKey) throws GeneralSecurityException {
        boolean isNewKey;
        initKey: {
            if (key == null) {
                if (encodedKey == null) {
                    key = generateKey();
                    isNewKey = true;
                    break initKey;
                }
                key = new SecretKeySpec(encodedKey, algorithm());
            }
            isNewKey = false;
        }

        mRootKey = key;
        mIsNewKey = isNewKey;
    }

    /**
     * @throws IllegalStateException if key was passed into the constructor
     */
    public SecretKey secretKey() {
        if (!mIsNewKey) {
            throw new IllegalStateException("Unavailable");
        }
        return mRootKey;
    }

    @Override
    public final void encryptPage(long pageIndex, int pageSize,
                                  byte[] src, int srcOffset, byte[] dst, int dstOffset)
        throws GeneralSecurityException
    {
        byte[] dataIvSalt = mDataIvSalt;
        SecretKey dataKey = mDataKey;

        if (dataIvSalt == null) {
            createDataKeyIfNecessary();
            dataIvSalt = mDataIvSalt;
            dataKey = mDataKey;
        }

        Cipher cipher;
        if (pageIndex <= 1) {
            cipher = headerPageCipher();
            initCipher(cipher, Cipher.ENCRYPT_MODE, mRootKey);

            // Store IV and addtional keys at end of header page, which (presently) has at
            // least 196 bytes available. Max AES block size is 32 bytes, so required space is
            // 99 bytes. If block size is 64 bytes, required header space is 195 bytes.

            byte[] srcCopy = new byte[pageSize];

            System.arraycopy(src, srcOffset, srcCopy, 0, pageSize);
            src = srcCopy;
            srcOffset = 0;
            int offset = pageSize;

            byte[] headerIv = cipher.getIV();
            checkBlockLength(headerIv);
            offset = encodeBlock(src, offset, headerIv);
            // Don't encrypt the IV.
            encodeBlock(dst, dstOffset + pageSize, headerIv);
            pageSize = offset;

            offset = encodeBlock(src, offset, dataIvSalt);
            offset = encodeBlock(src, offset, dataKey.getEncoded());
        } else {
            cipher = dataPageCipher();
            IvParameterSpec ivSpec = generateDataPageIv(cipher, pageIndex, dataIvSalt, dataKey);
            initCipher(cipher, Cipher.ENCRYPT_MODE, dataKey, ivSpec);
        }

        if (cipher.doFinal(src, srcOffset, pageSize, dst, dstOffset) != pageSize) {
            throw new GeneralSecurityException("Encrypted length does not match");
        }
    }

    @Override
    public final void encryptPage(long pageIndex, int pageSize,
                                  long srcPtr, int srcOffset, long dstPtr, int dstOffset)
        throws GeneralSecurityException
    {
        // Note: Same as other encryptPage method, except acts on direct memory pointers.

        byte[] dataIvSalt = mDataIvSalt;
        SecretKey dataKey = mDataKey;

        if (dataIvSalt == null) {
            createDataKeyIfNecessary();
            dataIvSalt = mDataIvSalt;
            dataKey = mDataKey;
        }

        Cipher cipher;
        if (pageIndex <= 1) {
            cipher = headerPageCipher();
            initCipher(cipher, Cipher.ENCRYPT_MODE, mRootKey);

            long srcCopy = DirectPageOps.p_alloc(pageSize);
            try {
                DirectPageOps.p_copy(srcPtr, srcOffset, srcCopy, 0, pageSize);
                srcPtr = srcCopy;
                srcOffset = 0;
                int offset = pageSize;

                byte[] headerIv = cipher.getIV();
                checkBlockLength(headerIv);
                offset = encodeBlock(srcPtr, offset, headerIv);
                // Don't encrypt the IV.
                encodeBlock(dstPtr, dstOffset + pageSize, headerIv);
                pageSize = offset;

                offset = encodeBlock(srcPtr, offset, dataIvSalt);
                offset = encodeBlock(srcPtr, offset, dataKey.getEncoded());

                if (cipherDoFinal(cipher, srcPtr, srcOffset, pageSize, dstPtr, dstOffset)
                    != pageSize)
                {
                    throw new GeneralSecurityException("Encrypted length does not match");
                }
            } finally {
                DirectPageOps.p_delete(srcCopy);
            }
        } else {
            cipher = dataPageCipher();
            IvParameterSpec ivSpec = generateDataPageIv(cipher, pageIndex, dataIvSalt, dataKey);
            initCipher(cipher, Cipher.ENCRYPT_MODE, dataKey, ivSpec);

            if (cipherDoFinal(cipher, srcPtr, srcOffset, pageSize, dstPtr, dstOffset) != pageSize)
            {
                throw new GeneralSecurityException("Encrypted length does not match");
            }
        }
    }

    private void createDataKeyIfNecessary() throws GeneralSecurityException {
        synchronized (mRootKey) {
            if (mDataIvSalt == null) {
                byte[] dataIvSalt = generateKey().getEncoded();
                SecretKey dataKey = generateKey();
                checkBlockLength(dataIvSalt);
                checkBlockLength(dataKey.getEncoded());
                mDataIvSalt = dataIvSalt;
                mDataKey = dataKey;
            }
        }
    }

    @Override
    public final void decryptPage(long pageIndex, int pageSize,
                                  byte[] src, int srcOffset, byte[] dst, int dstOffset)
        throws GeneralSecurityException
    {
        Cipher cipher;
        if (pageIndex <= 1) {
            byte[] headerIv = decodeBlock(src, srcOffset + pageSize);

            // Don't decrypt the IV.
            pageSize = pageSize - headerIv.length - 1;

            cipher = headerPageCipher();
            initCipher(cipher, Cipher.DECRYPT_MODE, mRootKey, new IvParameterSpec(headerIv));

            if (cipher.doFinal(src, srcOffset, pageSize, dst, dstOffset) != pageSize) {
                throw new GeneralSecurityException("Decrypted length does not match");
            }

            if (mDataIvSalt == null) synchronized (mRootKey) {
                if (mDataIvSalt == null) {
                    int offset = dstOffset + pageSize;
                    byte[] dataIvSalt = decodeBlock(dst, offset);
                    mDataIvSalt = dataIvSalt;
                    offset = offset - dataIvSalt.length - 1;
                    byte[] dataKeyValue = decodeBlock(dst, offset);
                    mDataKey = new SecretKeySpec(dataKeyValue, algorithm());
                }
            }
        } else {
            cipher = dataPageCipher();
            SecretKey dataKey = mDataKey;
            IvParameterSpec ivSpec = generateDataPageIv(cipher, pageIndex, mDataIvSalt, dataKey);
            initCipher(cipher, Cipher.DECRYPT_MODE, dataKey, ivSpec);

            if (cipher.doFinal(src, srcOffset, pageSize, dst, dstOffset) != pageSize) {
                throw new GeneralSecurityException("Decrypted length does not match");
            }
        }
    }

    @Override
    public final void decryptPage(long pageIndex, int pageSize,
                                  long srcPtr, int srcOffset, long dstPtr, int dstOffset)
        throws GeneralSecurityException
    {
        // Note: Same as other decryptPage method, except acts on direct memory pointers.

        Cipher cipher;
        if (pageIndex <= 1) {
            byte[] headerIv = decodeBlock(srcPtr, srcOffset + pageSize);

            // Don't decrypt the IV.
            pageSize = pageSize - headerIv.length - 1;

            cipher = headerPageCipher();
            initCipher(cipher, Cipher.DECRYPT_MODE, mRootKey, new IvParameterSpec(headerIv));

            if (cipherDoFinal(cipher, srcPtr, srcOffset, pageSize, dstPtr, dstOffset) != pageSize)
            {
                throw new GeneralSecurityException("Decrypted length does not match");
            }

            if (mDataIvSalt == null) synchronized (mRootKey) {
                if (mDataIvSalt == null) {
                    int offset = dstOffset + pageSize;
                    byte[] dataIvSalt = decodeBlock(dstPtr, offset);
                    mDataIvSalt = dataIvSalt;
                    offset = offset - dataIvSalt.length - 1;
                    byte[] dataKeyValue = decodeBlock(dstPtr, offset);
                    mDataKey = new SecretKeySpec(dataKeyValue, algorithm());
                }
            }
        } else {
            cipher = dataPageCipher();
            SecretKey dataKey = mDataKey;
            IvParameterSpec ivSpec = generateDataPageIv(cipher, pageIndex, mDataIvSalt, dataKey);
            initCipher(cipher, Cipher.DECRYPT_MODE, dataKey, ivSpec);

            if (cipherDoFinal(cipher, srcPtr, srcOffset, pageSize, dstPtr, dstOffset) != pageSize)
            {
                throw new GeneralSecurityException("Decrypted length does not match");
            }
        }
    }

    private IvParameterSpec generateDataPageIv(Cipher cipher, long pageIndex,
                                               byte[] salt, SecretKey dataKey)
        throws GeneralSecurityException
    {
        byte[] iv = new byte[cipher.getBlockSize()];
        Utils.encodeLongLE(iv, 0, pageIndex);
        initCipher(cipher, Cipher.ENCRYPT_MODE, dataKey, new IvParameterSpec(iv));
        return new IvParameterSpec(cipher.doFinal(salt));
    }

    @Override
    public final OutputStream newEncryptingStream(long id, OutputStream out)
        throws GeneralSecurityException, IOException
    {
        Cipher cipher = newStreamCipher();
        initCipher(cipher, Cipher.ENCRYPT_MODE, mRootKey);
        byte[] iv = cipher.getIV();
        checkBlockLength(iv);
        out.write((byte) (iv.length - 1));
        out.write(iv);
        return new CipherOutputStream(out, cipher);
    }

    @Override
    public final InputStream newDecryptingStream(long id, InputStream in)
        throws GeneralSecurityException, IOException
    {
        int length = in.read();
        if (length < 0) {
            throw new EOFException();
        }
        byte[] iv = new byte[length + 1];
        Utils.readFully(in, iv, 0, iv.length);
        Cipher cipher = newStreamCipher();
        initCipher(cipher, Cipher.DECRYPT_MODE, mRootKey, new IvParameterSpec(iv));
        return new CipherInputStream(in, cipher);
    }

    /**
     * Returns a String with a parseable Java byte array declaration.
     */
    public static String toString(SecretKey key) {
        return toString(key.getEncoded());
    }

    /**
     * Returns a String with a parseable Java byte array declaration.
     */
    public static String toString(byte[] key) {
        StringBuilder b = new StringBuilder(200);
        b.append('{');
        for (int i=0; i<key.length; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append(key[i]);
        }
        b.append('}');
        return b.toString();
    }

    /**
     * Returns "AES" by default; override to change the algorithm.
     */
    protected String algorithm() {
        return "AES";
    }

    /**
     * Returns 128 bits by default; override to use any size supported by the algorithm.
     */
    protected int keySize() {
        return 128;
    }

    /**
     * Called to generate a key, using the {@link #algorithm algorithm} and {@link #keySize key
     * size} of this instance.
     */
    protected SecretKey generateKey() throws GeneralSecurityException {
        KeyGenerator gen = KeyGenerator.getInstance(algorithm());
        gen.init(keySize());
        return gen.generateKey();
    }

    /**
     * Called to instantiate all {@link Cipher} instances, with the given transformation.
     */
    protected Cipher newCipher(String transformation) throws GeneralSecurityException {
        return Cipher.getInstance(transformation);
    }

    /**
     * Called to instantiate a {@link Cipher} for encrypting and decrypting database pages,
     * using the fixed instance {@link #algorithm algorithm}. Mode applied is CTR, with no
     * padding.
     */
    protected Cipher newPageCipher() throws GeneralSecurityException {
        return newCipher(algorithm() + "/CTR/NoPadding");
    }

    /**
     * Called to instantiate a {@link Cipher} for encrypting and decrypting redo logs, using
     * the fixed instance {@link #algorithm algorithm}. Mode applied is CTR, with no padding.
     */
    protected Cipher newStreamCipher() throws GeneralSecurityException {
        return newCipher(algorithm() + "/CTR/NoPadding");
    }

    /**
     * Called to initialize a new or re-used {@link Cipher}, generating a random initialization
     * vector.
     */
    protected void initCipher(Cipher cipher, int opmode, SecretKey key)
        throws GeneralSecurityException
    {
        cipher.init(opmode, key);
    }

    /**
     * Called to initialize a new or re-used {@link Cipher}, using the given initialization
     * vector.
     */
    protected void initCipher(Cipher cipher, int opmode, SecretKey key, IvParameterSpec ivSpec)
        throws GeneralSecurityException
    {
        cipher.init(opmode, key, ivSpec);
    }

    private Cipher headerPageCipher() throws GeneralSecurityException {
        Cipher cipher = mHeaderPageCipher.get();
        if (cipher == null) {
            cipher = newStreamCipher();
            mHeaderPageCipher.set(cipher);
        }
        return cipher;
    }

    private Cipher dataPageCipher() throws GeneralSecurityException {
        Cipher cipher = mDataPageCipher.get();
        if (cipher == null) {
            cipher = newPageCipher();
            mDataPageCipher.set(cipher);
        }
        return cipher;
    }

    private static void checkBlockLength(byte[] bytes) throws GeneralSecurityException {
        if (bytes.length == 0 || bytes.length > 256) {
            throw new GeneralSecurityException
                ("Unsupported block length: " + bytes.length);
        }
    }

    private static int encodeBlock(byte[] dst, int offset, byte[] value) {
        dst[--offset] = (byte) (value.length - 1);
        System.arraycopy(value, 0, dst, offset -= value.length, value.length);
        return offset;
    }

    private static int encodeBlock(long dstPtr, int offset, byte[] value) {
        DirectPageOps.p_bytePut(dstPtr, --offset, value.length - 1);
        DirectPageOps.p_copyFromArray(value, 0, dstPtr, offset -= value.length, value.length);
        return offset;
    }

    private static byte[] decodeBlock(byte[] src, int offset) {
        byte[] value = new byte[(src[--offset] & 0xff) + 1];
        System.arraycopy(src, offset - value.length, value, 0, value.length);
        return value;
    }

    private static byte[] decodeBlock(long srcPtr, int offset) {
        byte[] value = new byte[DirectPageOps.p_ubyteGet(srcPtr, --offset) + 1];
        DirectPageOps.p_copyToArray(srcPtr, offset - value.length, value, 0, value.length);
        return value;
    }

    private static int cipherDoFinal(Cipher cipher,
                                     long srcPage, int srcStart, int srcLen,
                                     long dstPage, int dstStart)
        throws GeneralSecurityException
    {
        return cipher.doFinal(DirectAccess.ref(srcPage + srcStart, srcLen),
                              DirectAccess.ref2(dstPage + dstStart, srcLen));
    }
}
