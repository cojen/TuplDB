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

    private final ThreadLocal<Cipher> mBlockCipher = new ThreadLocal<Cipher>();
    private final SecretKey mKey;
    private final boolean mIsNewKey;

    private final byte[] mHeaderIv0;
    private final byte[] mHeaderIv1;

    private volatile MessageDigest mPageMd;

    /**
     * Construct with a new key, available from the {@link #secretKey
     * secretKey} method.
     */
    public CipherCrypto() throws GeneralSecurityException {
        mKey = generateKey();
        mIsNewKey = true;
        mHeaderIv0 = generateHeaderIv(false);
        mHeaderIv1 = generateHeaderIv(true);
    }

    /**
     * Construct with an existing key, which is wrapped with {@link SecretKeySpec}.
     */
    public CipherCrypto(byte[] encodedKey) throws GeneralSecurityException {
        mKey = keyFor(encodedKey);
        mIsNewKey = false;
        mHeaderIv0 = generateHeaderIv(false);
        mHeaderIv1 = generateHeaderIv(true);
    }

    /**
     * Construct with an existing key.
     */
    public CipherCrypto(SecretKey key) throws GeneralSecurityException {
        mKey = key;
        mIsNewKey = false;
        mHeaderIv0 = generateHeaderIv(false);
        mHeaderIv1 = generateHeaderIv(true);
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
        MessageDigest md = newMessageDigest();
        md.update(databaseId);
        md.update(mKey.getEncoded());
        mPageMd = md;
    }

    @Override
    public void encryptPage(long pageIndex, int pageSize, byte[] page, int pageOffset)
        throws GeneralSecurityException
    {
        Cipher cipher = blockCipher();
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
        Cipher cipher = blockCipher();
        initCipher(cipher, Cipher.ENCRYPT_MODE, mKey, generateIv(pageIndex));
        if (cipher.doFinal(src, srcOffset, pageSize, dst, dstOffset) != pageSize) {
            throw new GeneralSecurityException("Encrypted length does not match");
        }
    }

    @Override
    public void decryptPage(long pageIndex, int pageSize, byte[] page, int pageOffset)
        throws GeneralSecurityException
    {
        Cipher cipher = blockCipher();
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
        Cipher cipher = blockCipher();
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
        Cipher cipher = newStreamCipher();
        initCipher(cipher, Cipher.ENCRYPT_MODE, mKey, generatePageIv(id));
        return new CipherOutputStream(out, cipher);
    }

    @Override
    public InputStream newDecryptingStream(long id, InputStream in)
        throws GeneralSecurityException, IOException
    {
        Cipher cipher = newStreamCipher();
        initCipher(cipher, Cipher.DECRYPT_MODE, mKey, generatePageIv(id));
        return new CipherInputStream(in, cipher);
    }

    protected String algorithm() {
        return "AES";
    }

    protected int algorithmBlockSizeInBytes() {
        return 16;
    }

    protected MessageDigest newMessageDigest() throws GeneralSecurityException {
        return MessageDigest.getInstance("SHA-1");
    }

    protected SecretKey generateKey() throws GeneralSecurityException {
        KeyGenerator gen = KeyGenerator.getInstance(algorithm());
        gen.init(256);
        return gen.generateKey();
    }

    protected SecretKey keyFor(byte[] encodedKey) {
        return new SecretKeySpec(encodedKey, algorithm());
    }

    protected Cipher newBlockCipher() throws GeneralSecurityException {
        return Cipher.getInstance(algorithm() + "/CBC/NoPadding");
    }

    protected Cipher newStreamCipher() throws GeneralSecurityException {
        return Cipher.getInstance(algorithm() + "/CTR/NoPadding");
    }

    protected void initCipher(Cipher cipher, int opmode, SecretKey key, byte[] iv)
        throws GeneralSecurityException
    {
        cipher.init(opmode, key, new IvParameterSpec(iv));
    }

    private byte[] generateHeaderIv(boolean second) throws GeneralSecurityException {
        MessageDigest md = newMessageDigest();
        md.update(mKey.getEncoded());
        if (second) {
            md.update((byte) 85);
        }
        byte[] iv = new byte[algorithmBlockSizeInBytes()];
        digest(md, iv);
        return iv;
    }

    private byte[] generateIv(long pageIndex) throws GeneralSecurityException {
        if (pageIndex == 0) {
            return mHeaderIv0.clone();
        } else if (pageIndex == 1) {
            return mHeaderIv1.clone();
        } else {
            return generatePageIv(pageIndex);
        }
    }

    private byte[] generatePageIv(long pageIndex) throws GeneralSecurityException {
        try {
            MessageDigest md = (MessageDigest) mPageMd.clone();
            int size = algorithmBlockSizeInBytes();
            byte[] iv = new byte[size];
            byte[] pageBytes = size >= 8 ? iv : new byte[8];
            Utils.writeLongLE(pageBytes, 0, pageIndex);
            md.update(pageBytes);
            digest(md, iv);
            return iv;
        } catch (CloneNotSupportedException e) {
            throw new GeneralSecurityException(e);
        }
    }

    private Cipher blockCipher() throws GeneralSecurityException {
        Cipher cipher = mBlockCipher.get();
        if (cipher == null) {
            cipher = newBlockCipher();
            mBlockCipher.set(cipher);
        }
        return cipher;
    }

    private void digest(MessageDigest md, byte[] dst) {
        byte[] digest = md.digest();
        int dstOffset = 0;
        int dstLength = dst.length;
        for (int i=0; i<digest.length; i++) {
            dst[dstOffset++] ^= digest[i];
            if (dstOffset >= dstLength) {
                dstOffset = 0;
            }
        }
    }
}
