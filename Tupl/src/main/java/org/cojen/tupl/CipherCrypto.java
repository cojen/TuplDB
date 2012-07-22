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

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.security.GeneralSecurityException;
import java.security.MessageDigest;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.CipherOutputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Crypto implementation which defaults to the AES algorithm.
 *
 * @author Brian S O'Neill
 */
public class CipherCrypto implements Crypto {
    /**
     * Generates and prints a new key.
     */
    public static void main(String[] args) throws Exception {
        byte[] encodedKey = new CipherCrypto().secretKey().getEncoded();
        StringBuilder b = new StringBuilder(200);
        b.append('{');
        for (int i=0; i<encodedKey.length; i++) {
            if (i > 0) {
                b.append(',');
            }
            b.append(encodedKey[i]);
        }
        b.append('}');

        System.out.println(b);
    }

    private final ThreadLocal<Cipher> mUnpaddedCipher = new ThreadLocal<Cipher>();
    private final SecretKey mKey;
    private final boolean mIsNewKey;

    private volatile byte[] mHeaderIv;
    private volatile byte[] mPageIv;

    /**
     * Construct with a new key, available from the {@link #secretKey
     * secretKey} method.
     */
    public CipherCrypto() throws GeneralSecurityException {
        mKey = generateKey();
        mIsNewKey = true;
        mHeaderIv = generateHeaderIv();
    }

    /**
     * Construct with an existing key, which is wrapped with {@link SecretKeySpec}.
     */
    public CipherCrypto(byte[] encodedKey) throws GeneralSecurityException {
        mKey = keyFor(encodedKey);
        mIsNewKey = false;
        mHeaderIv = generateHeaderIv();
    }

    /**
     * Construct with an existing key.
     */
    public CipherCrypto(SecretKey key) throws GeneralSecurityException {
        mKey = key;
        mIsNewKey = false;
        mHeaderIv = generateHeaderIv();
    }

    /**
     * @throws IllegalStateException if key was passed into the constructor
     */
    public SecretKey secretKey() {
        if (!mIsNewKey) {
            throw new IllegalStateException("Unavailable");
        }
        return mKey;
    }

    @Override
    public void setDatabaseId(byte[] databaseId) throws GeneralSecurityException {
        int size = algorithmBlockSizeInBytes();
        byte[] iv;
        int ivOffset = 0;
        if (databaseId.length == size) {
            iv = databaseId.clone();
        } else {
            iv = new byte[size];
            ivOffset = mix(iv, ivOffset, databaseId);
        }
        mix(iv, ivOffset, mKey.getEncoded());
        mPageIv = iv;
    }

    @Override
    public void encryptPage(long pageIndex, int pageSize, byte[] page, int pageOffset)
        throws GeneralSecurityException
    {
        Cipher cipher = unpaddedCipher();
        initCipher(cipher, Cipher.ENCRYPT_MODE, mKey, generateIv(pageIndex));
        if (cipher.doFinal(page, pageOffset, pageSize, page, pageOffset) != pageSize) {
            throw new GeneralSecurityException("Encrypted length does not match");
        }
    }

    @Override
    public void encryptPage(long pageIndex, int pageSize,
                            byte[] src, int srcOffset, byte[] dst, int dstOffset)
        throws GeneralSecurityException
    {
        Cipher cipher = unpaddedCipher();
        initCipher(cipher, Cipher.ENCRYPT_MODE, mKey, generateIv(pageIndex));
        if (cipher.doFinal(src, srcOffset, pageSize, dst, dstOffset) != pageSize) {
            throw new GeneralSecurityException("Encrypted length does not match");
        }
    }

    @Override
    public void decryptPage(long pageIndex, int pageSize, byte[] page, int pageOffset)
        throws GeneralSecurityException
    {
        Cipher cipher = unpaddedCipher();
        initCipher(cipher, Cipher.DECRYPT_MODE, mKey, generateIv(pageIndex));
        if (cipher.doFinal(page, pageOffset, pageSize, page, pageOffset) != pageSize) {
            throw new GeneralSecurityException("Decrypted length does not match");
        }
    }

    @Override
    public void decryptPage(long pageIndex, int pageSize,
                            byte[] src, int srcOffset, byte[] dst, int dstOffset)
        throws GeneralSecurityException
    {
        Cipher cipher = unpaddedCipher();
        initCipher(cipher, Cipher.DECRYPT_MODE, mKey, generateIv(pageIndex));
        cipher.doFinal(src, srcOffset, pageSize, dst, dstOffset);
        if (cipher.doFinal(src, srcOffset, pageSize, dst, dstOffset) != pageSize) {
            throw new GeneralSecurityException("Decrypted length does not match");
        }
    }

    @Override
    public OutputStream newEncryptingStream(long id, OutputStream out)
        throws GeneralSecurityException, IOException
    {
        Cipher cipher = newPaddedCipher();
        initCipher(cipher, Cipher.ENCRYPT_MODE, mKey, generatePageIv(id));
        return new CipherOutputStream(out, cipher);
    }

    @Override
    public InputStream newDecryptingStream(long id, InputStream in)
        throws GeneralSecurityException, IOException
    {
        Cipher cipher = newPaddedCipher();
        initCipher(cipher, Cipher.DECRYPT_MODE, mKey, generatePageIv(id));
        return new CipherInputStream(in, cipher);
    }

    protected String algorithm() {
        return "AES";
    }

    protected int algorithmBlockSizeInBytes() {
        return 16;
    }

    protected SecretKey generateKey() throws GeneralSecurityException {
        KeyGenerator gen = KeyGenerator.getInstance(algorithm());
        gen.init(256);
        return gen.generateKey();
    }

    protected SecretKey keyFor(byte[] encodedKey) {
        return new SecretKeySpec(encodedKey, algorithm());
    }

    protected Cipher newUnpaddedCipher() throws GeneralSecurityException {
        return Cipher.getInstance(algorithm() + "/CBC/NoPadding");
    }

    protected Cipher newPaddedCipher() throws GeneralSecurityException {
        return Cipher.getInstance(algorithm() + "/CBC/PKCS5Padding");
    }

    protected void initCipher(Cipher cipher, int opmode, SecretKey key, byte[] iv)
        throws GeneralSecurityException
    {
        cipher.init(opmode, key, new IvParameterSpec(iv));
    }

    private byte[] generateHeaderIv() throws GeneralSecurityException {
        byte[] hash = MessageDigest.getInstance("SHA-1").digest(mKey.getEncoded());
        byte[] iv = new byte[algorithmBlockSizeInBytes()];
        mix(iv, 0, hash);
        return iv;
    }

    private byte[] generateIv(long pageIndex) {
        if (pageIndex == 0) {
            return mHeaderIv.clone();
        } else if (pageIndex == 1) {
            byte[] iv = mHeaderIv.clone();
            iv[0] ^= 1;
            return iv;
        } else {
            return generatePageIv(pageIndex);
        }
    }

    private byte[] generatePageIv(long pageIndex) {
        byte[] iv = mPageIv.clone();
        mix(iv, 0, pageIndex);
        return iv;
    }

    private Cipher unpaddedCipher() throws GeneralSecurityException {
        Cipher cipher = mUnpaddedCipher.get();
        if (cipher == null) {
            cipher = newUnpaddedCipher();
            mUnpaddedCipher.set(cipher);
        }
        return cipher;
    }

    private int mix(byte[] dst, int dstOffset, byte[] src) {
        int dstLength = dst.length;
        for (int i=0; i<src.length; i++) {
            dst[dstOffset++] ^= src[i];
            if (dstOffset >= dstLength) {
                dstOffset = 0;
            }
        }
        return dstOffset;
    }

    private int mix(byte[] dst, int dstOffset, long src) {
        int dstLength = dst.length;
        int i=0;
        while (true) {
            dst[dstOffset++] ^= (byte) src;
            if (++i >= 8) {
                break;
            }
            src >>= 8;
            if (dstOffset >= dstLength) {
                dstOffset = 0;
            }
        }
        return dstOffset;
    }
}
