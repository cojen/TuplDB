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

import java.io.EOFException;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

import java.security.GeneralSecurityException;

import java.util.Objects;

import java.util.function.Supplier;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.ShortBufferException;

import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.cojen.tupl.core.CheckedSupplier;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.LocalPool;

/**
 * Crypto implementation which uses {@link Cipher} and defaults to the AES algorithm. An
 * encrypted salt-sector initialization vector <a
 * href="https://en.wikipedia.org/wiki/Disk_encryption_theory#ESSIV">(ESSIV)</a> scheme is
 * applied to all data pages in the main database file, where the initialization vector is
 * defined as {@code encrypt(iv=dataPageIndex, key=dataPageKey, data=dataPageSalt)}. The key
 * and salt values are randomly generated when the database is created, and they are stored in
 * the database header pages. The initialization vector for the header pages is randomly
 * generated each time and is stored cleartext in the header pages. The secret key given to the
 * constructor is used for encrypting header pages, and for any redo log files.
 *
 * @author Brian S O'Neill
 */
public class CipherCrypto implements Crypto {
    /**
     * Generates and prints a new 128-bit key. Pass an argument to specify an alternate key
     * size.
     */
    public static void main(String[] args) throws Exception {
        int keySize = 128;
        if (args.length != 0) {
            keySize = Integer.parseInt(args[0]);
        }
        System.out.println(toString(new CipherCrypto(null, keySize).secretKey()));
    }

    private final LocalPool<Cipher> mHeaderPageCipher = new LocalPool<>(this::newHpCipher, -4);
    private final LocalPool<Cipher> mDataPageCipher = new LocalPool<>(this::newDpCipher, -4);
    private final SecretKey mRootKey;
    private final int mKeySize;

    private volatile byte[] mDataIvSalt;
    private volatile SecretKey mDataKey;

    /**
     * Construct with an existing key, which is wrapped with {@link SecretKeySpec}, although
     * additional keys might need to be generated. The key size must be permitted by the
     * underlying algorithm, which for AES must be 128, 192, or 256 bits (16, 24, or 32 bytes).
     */
    public static Supplier<Crypto> factory(byte[] encodedKey) {
        return (CheckedSupplier<Crypto>) () -> new CipherCrypto(null, encodedKey);
    }

    /**
     * Construct with an existing key, although additional keys might need to be generated.
     *
     * @param keySize key size in bits for additional keys (with AES: 128, 192, or 256)
     */
    public static Supplier<Crypto> factory(SecretKey key, int keySize) {
        return (CheckedSupplier<Crypto>) () -> new CipherCrypto(null, key, keySize);
    }

    /**
     * Construct with a new key, available from the {@link #secretKey secretKey} method.
     *
     * @param keySize key size in bits (with AES: 128, 192, or 256)
     */
    public CipherCrypto(int keySize) throws GeneralSecurityException {
        this(CheckedSupplier.check(2), keySize);
    }

    /**
     * Construct with an existing key, which is wrapped with {@link SecretKeySpec}, although
     * additional keys might need to be generated. The key size must be permitted by the
     * underlying algorithm, which for AES must be 128, 192, or 256 bits (16, 24, or 32 bytes).
     */
    public CipherCrypto(byte[] encodedKey) {
        this(CheckedSupplier.check(2), encodedKey);
    }

    /**
     * Construct with an existing key, although additional keys might need to be generated.
     *
     * @param keySize key size in bits for additional keys (with AES: 128, 192, or 256)
     */
    public CipherCrypto(SecretKey key, int keySize) {
        this(CheckedSupplier.check(2), key, keySize);
    }

    private CipherCrypto(Object dummy, int keySize) throws GeneralSecurityException {
        mKeySize = check(keySize);
        mRootKey = generateKey(keySize);
    }

    private CipherCrypto(Object dummy, byte[] encodedKey) {
        mKeySize = check(encodedKey.length * 8) | 0x8000_0000;
        mRootKey = new SecretKeySpec(encodedKey, algorithm());
    }

    private CipherCrypto(Object dummy, SecretKey key, int keySize) {
        mKeySize = check(keySize) | 0x8000_0000;
        mRootKey = Objects.requireNonNull(key);
    }

    private static int check(int keySize) {
        if (keySize <= 0) {
            throw new IllegalArgumentException("Illegal key size: " + keySize);
        }
        return keySize;
    }

    /**
     * Provides access to the generated secret key.
     *
     * @throws IllegalStateException if key was passed into the constructor
     */
    public SecretKey secretKey() {
        if (mKeySize < 0) {
            throw new IllegalStateException("Unavailable");
        }
        return mRootKey;
    }

    @Override
    public final void encryptPage(long pageIndex, int pageSize,
                                  long srcAddr, int srcOffset, long dstAddr, int dstOffset)
        throws GeneralSecurityException
    {
        byte[] dataIvSalt = mDataIvSalt;
        SecretKey dataKey = mDataKey;

        if (dataIvSalt == null) {
            createDataKeyIfNecessary();
            dataIvSalt = mDataIvSalt;
            dataKey = mDataKey;
        }

        if (pageIndex <= 1) {
            LocalPool.Entry<Cipher> entry = mHeaderPageCipher.access();
            try {
                Cipher cipher = entry.get();
                initCipher(cipher, Cipher.ENCRYPT_MODE, mRootKey);

                // Store IV and additional keys at end of header page, which (presently) has at
                // least 196 bytes available. Max AES block size is 32 bytes, so required space
                // is 99 bytes. If block size is 64 bytes, required header space is 195 bytes.

                try (Arena a = Arena.ofConfined()) {
                    MemorySegment srcCopy = a.allocate(pageSize);
                    MemorySegment.copy
                        (MemorySegment.ofAddress(srcAddr + srcOffset).reinterpret(pageSize), 0,
                         srcCopy, 0, pageSize);
                    srcAddr = srcCopy.address();
                    srcOffset = 0;
                    int offset = pageSize;

                    byte[] headerIv = cipher.getIV();
                    checkBlockLength(headerIv);
                    offset = encodeBlock(srcAddr, offset, headerIv);
                    // Don't encrypt the IV.
                    encodeBlock(dstAddr, dstOffset + pageSize, headerIv);
                    pageSize = offset;

                    offset = encodeBlock(srcAddr, offset, dataIvSalt);
                    offset = encodeBlock(srcAddr, offset, dataKey.getEncoded());

                    if (cipherDoFinal(cipher, srcAddr, srcOffset, pageSize, dstAddr, dstOffset)
                        != pageSize)
                    {
                        throw new GeneralSecurityException("Encrypted length does not match");
                    }
                }
            } finally {
                entry.release();
            }
        } else {
            LocalPool.Entry<Cipher> entry = mDataPageCipher.access();
            try {
                Cipher cipher = entry.get();
                IvParameterSpec ivSpec = generateDataPageIv(cipher, pageIndex, dataIvSalt, dataKey);
                initCipher(cipher, Cipher.ENCRYPT_MODE, dataKey, ivSpec);

                if (cipherDoFinal(cipher, srcAddr, srcOffset, pageSize, dstAddr, dstOffset)
                    != pageSize)
                {
                    throw new GeneralSecurityException("Encrypted length does not match");
                }
            } finally {
                entry.release();
            }
        }
    }

    private void createDataKeyIfNecessary() throws GeneralSecurityException {
        synchronized (mRootKey) {
            if (mDataIvSalt == null) {
                int keySize = mKeySize & 0x7fff_ffff;
                byte[] dataIvSalt = generateKey(keySize).getEncoded();
                SecretKey dataKey = generateKey(keySize);
                checkBlockLength(dataIvSalt);
                checkBlockLength(dataKey.getEncoded());
                mDataIvSalt = dataIvSalt;
                mDataKey = dataKey;
            }
        }
    }

    @Override
    public final void decryptPage(long pageIndex, int pageSize,
                                  long srcAddr, int srcOffset, long dstAddr, int dstOffset)
        throws GeneralSecurityException
    {
        if (pageIndex <= 1) {
            byte[] headerIv = decodeBlock(srcAddr, srcOffset + pageSize);

            // Don't decrypt the IV.
            pageSize = pageSize - headerIv.length - 1;

            LocalPool.Entry<Cipher> entry = mHeaderPageCipher.access();
            try {
                Cipher cipher = entry.get();
                initCipher(cipher, Cipher.DECRYPT_MODE, mRootKey, new IvParameterSpec(headerIv));

                if (cipherDoFinal(cipher, srcAddr, srcOffset, pageSize, dstAddr, dstOffset)
                    != pageSize)
                {
                    throw new GeneralSecurityException("Decrypted length does not match");
                }
            } finally {
                entry.release();
            }

            if (mDataIvSalt == null) synchronized (mRootKey) {
                if (mDataIvSalt == null) {
                    int offset = dstOffset + pageSize;
                    byte[] dataIvSalt = decodeBlock(dstAddr, offset);
                    mDataIvSalt = dataIvSalt;
                    offset = offset - dataIvSalt.length - 1;
                    byte[] dataKeyValue = decodeBlock(dstAddr, offset);
                    mDataKey = new SecretKeySpec(dataKeyValue, algorithm());
                }
            }
        } else {
            LocalPool.Entry<Cipher> entry = mDataPageCipher.access();
            try {
                Cipher cipher = entry.get();
                SecretKey dataKey = mDataKey;
                IvParameterSpec ivSpec = generateDataPageIv
                    (cipher, pageIndex, mDataIvSalt, dataKey);
                initCipher(cipher, Cipher.DECRYPT_MODE, dataKey, ivSpec);

                if (cipherDoFinal(cipher, srcAddr, srcOffset, pageSize, dstAddr, dstOffset)
                    != pageSize)
                {
                    throw new GeneralSecurityException("Decrypted length does not match");
                }
            } finally {
                entry.release();
            }
        }
    }

    private IvParameterSpec generateDataPageIv(Cipher cipher, long pageIndex,
                                               byte[] salt, SecretKey dataKey)
        throws GeneralSecurityException
    {
        var iv = new byte[cipher.getBlockSize()];
        Utils.encodeLongLE(iv, 0, pageIndex);
        initCipher(cipher, Cipher.ENCRYPT_MODE, dataKey, new IvParameterSpec(iv));
        byte[] fin = cipher.doFinal(salt);
        // The length of the initialization vector is limited by the cipher block size.
        return new IvParameterSpec(fin, 0, iv.length);
    }

    @Override
    public final OutputStream newEncryptingStream(OutputStream out)
        throws GeneralSecurityException, IOException
    {
        Cipher cipher = newStreamCipher();
        initCipher(cipher, Cipher.ENCRYPT_MODE, mRootKey);
        byte[] iv = cipher.getIV();
        checkBlockLength(iv, 256);
        out.write((byte) (iv.length - 1));
        out.write(iv);
        return new Out(out, cipher);
    }

    @Override
    public final InputStream newDecryptingStream(InputStream in)
        throws GeneralSecurityException, IOException
    {
        int length = in.read();
        if (length < 0) {
            throw new EOFException();
        }
        var iv = new byte[length + 1];
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
        var b = new StringBuilder(200);
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
     * Called to generate a key, using the {@link #algorithm algorithm} and the given key size
     * (bits). In general, this method is only called when creating a new database. Afterwards,
     * the generated keys cannot change, and this method won't be called again.
     */
    protected SecretKey generateKey(int keySize) throws GeneralSecurityException {
        KeyGenerator gen = KeyGenerator.getInstance(algorithm());
        gen.init(keySize);
        return gen.generateKey();
    }

    /**
     * Called to instantiate all {@link Cipher} instances, with the given transformation.
     */
    protected Cipher newCipher(String transformation) throws GeneralSecurityException {
        return Cipher.getInstance(transformation);
    }

    /**
     * Called to instantiate a {@link Cipher} for encrypting and decrypting regular database
     * pages, using the fixed instance {@link #algorithm algorithm}. Default mode applied is
     * CTR, with no padding.
     */
    protected Cipher newPageCipher() throws GeneralSecurityException {
        return newCipher(algorithm() + "/CTR/NoPadding");
    }

    /**
     * Called to instantiate a {@link Cipher} for encrypting and decrypting header pages and
     * redo logs, using the fixed instance {@link #algorithm algorithm}. Default mode applied
     * is CTR, with no padding.
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

    /**
     * Returns a new Cipher for header pages.
     */
    private Cipher newHpCipher() {
        try {
            return newStreamCipher();
        } catch (GeneralSecurityException e) {
            throw Utils.rethrow(e);
        }
    }

    /**
     * Returns a new Cipher for data pages.
     */
    private Cipher newDpCipher() {
        try {
            return newPageCipher();
        } catch (GeneralSecurityException e) {
            throw Utils.rethrow(e);
        }
    }

    private static void checkBlockLength(byte[] bytes) throws GeneralSecurityException {
        checkBlockLength(bytes, 64); // header encoding limit; see encryptPage method
    }

    private static void checkBlockLength(byte[] bytes, int max) throws GeneralSecurityException {
        if (bytes.length == 0 || bytes.length > max) {
            throw new GeneralSecurityException
                ("Unsupported block length: " + bytes.length);
        }
    }

    private static int encodeBlock(long dstAddr, int offset, byte[] value) {
        MemorySegment dst = MemorySegment.ofAddress(dstAddr).reinterpret(offset);
        int length = value.length;
        dst.set(ValueLayout.JAVA_BYTE, --offset, (byte) (length - 1));
        MemorySegment.copy(value, 0, dst, ValueLayout.JAVA_BYTE, offset -= length, length);
        return offset;
    }

    private static byte[] decodeBlock(long srcAddr, int offset) {
        MemorySegment src = MemorySegment.ofAddress(srcAddr).reinterpret(offset);
        int length = (src.get(ValueLayout.JAVA_BYTE, --offset) & 0xff) + 1;
        var value = new byte[length];
        MemorySegment.copy(src, ValueLayout.JAVA_BYTE, offset - length, value, 0, length);
        return value;
    }

    private static int cipherDoFinal(Cipher cipher,
                                     long srcPage, int srcStart, int srcLen,
                                     long dstPage, int dstStart)
        throws GeneralSecurityException
    {
        return cipher.doFinal
            (MemorySegment.ofAddress(srcPage + srcStart).reinterpret(srcLen).asByteBuffer(),
             MemorySegment.ofAddress(dstPage + dstStart).reinterpret(srcLen).asByteBuffer());
    }

    /**
     * Alternative to CipherOutputStream which recycles the output buffer and doesn't suppress
     * exceptions.
     */
    private static class Out extends OutputStream {
        private final OutputStream mOut;
        private final Cipher mCipher;

        private byte[] mBuffer;

        Out(OutputStream out, Cipher cipher) {
            mOut = out;
            mCipher = cipher;
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[] {(byte) b});
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            byte[] buf = mBuffer;
            if (buf == null || buf.length < len) {
                mBuffer = buf = new byte[len];
            }

            int amt;
            try {
                amt = mCipher.update(b, off, len, buf);
            } catch (ShortBufferException e) {
                mBuffer = buf = mCipher.update(b, off, len);
                amt = buf.length;
            }

            mOut.write(buf, 0, amt);
        }

        @Override
        public void flush() throws IOException {
            mOut.flush();
        }

        @Override
        public void close() throws IOException {
            mBuffer = null;

            byte[] buf;
            try {
                buf = mCipher.doFinal();
            } catch (GeneralSecurityException e) {
                throw new IOException(e);
            }

            if (buf != null) {
                mOut.write(buf);
            }

            mOut.close();
        }
    }
}
