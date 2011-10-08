/*
 *  Copyright 2011 Brian S O'Neill
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

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import java.nio.channels.FileChannel;

import java.util.zip.Adler32;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RedoLog implements RedoLogVisitor {
    private static final long MAGIC_NUMBER = 431399725605778814L;
    private static final int ENCODING_VERSION = 20111001;

    private static final byte
        /** timestamp: long */
        OP_TIMESTAMP = 1,

        /** txnId: long */
        //OP_TXN_BEGIN = 2,

        /** txnId: long, parentTxnId: long */
        //OP_TXN_BEGIN_NESTED = 3,

        /** txnId: long */
        //OP_TXN_CONTINUE = 4,

        /** txnId: long, parentTxnId: long */
        //OP_TXN_CONTINUE_NESTED = 5,

        /** txnId: long */
        OP_TXN_ROLLBACK = 6,

        /** txnId: long, parentTxnId: long */
        OP_TXN_ROLLBACK_NESTED = 7,

        /** txnId: long, checksum: int */
        OP_TXN_COMMIT = 8,

        /** txnId: long, parentTxnId: long */
        OP_TXN_COMMIT_NESTED = 9,

        /** indexId: long, keyLength: varInt, key: bytes,
            valueLength: varInt, value: bytes,
            checksum: int */
        OP_STORE = 16,

        /** indexId: long, keyLength: varInt, key: bytes,
            checksum: int */
        OP_DELETE = 17,

        /** indexId: long, checksum: int */
        OP_CLEAR = 18,

        /** txnId: long, indexId: long, keyLength: varInt, key: bytes,
            valueLength: varInt, value: bytes */
        OP_TXN_STORE = 19,

        /** txnId: long, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_DELETE = 20,

        /** txnId: long, indexId: long */
        //OP_TXN_CLEAR = 21,

        /** length: varInt, data: bytes, checksum: int */
        OP_CUSTOM = (byte) 128,

        /** txnId: long, length: varInt, data: bytes */
        OP_TXN_CUSTOM = (byte) 129;

    public static RedoLog create(File file, long logNumber) throws IOException {
        if (file.exists()) {
            throw new FileNotFoundException("File already exists: " + file.getPath());
        }
        FileOutputStream out = new FileOutputStream(file);
        RedoLog log = new RedoLog(file, out);
        synchronized (log) {
            log.writeLong(MAGIC_NUMBER);
            log.writeInt(ENCODING_VERSION);
            log.writeLong(logNumber);
            log.timestamp();
            log.flush();
        }
        return log;
    }

    public static RedoLog open(File file, long number, boolean readOnly) throws IOException {
        // FIXME: only support read? no append? checksum becomes a mess if append is supported
        throw null;
    }

    private final File mFile;
    private final FileChannel mChannel;
    private final FileOutputStream mOut;
    private final byte[] mBuffer;
    private int mBufferPos;
    private final Adler32 mChecksum;

    RedoLog(File file, FileOutputStream out) throws IOException {
        mFile = file;
        mChannel = out == null ? null : out.getChannel();
        mOut = out;
        mBuffer = new byte[4096];
        mChecksum = new Adler32();
    }

    void replay(RedoLogVisitor visitor) throws IOException {
        // FIXME
        throw null;
    }

    public synchronized void store(long indexId, byte[] key, byte[] value) throws IOException {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }

        if (value == null) {
            writeOp(OP_DELETE, indexId);
            writeUnsignedVarInt(key.length);
            writeBytes(key);
        } else {
            writeOp(OP_STORE, indexId);
            writeUnsignedVarInt(key.length);
            writeBytes(key);
            writeUnsignedVarInt(value.length);
            writeBytes(value);
        }

        writeChecksumAndFlush();
    }

    public synchronized void clear(long indexId) throws IOException {
        writeOp(OP_CLEAR, indexId);
        writeChecksumAndFlush();
    }

    public void txnBegin(long txnId, long parentTxnId) throws IOException {
        // No-op.
    }

    public synchronized void txnRollback(long txnId, long parentTxnId) throws IOException {
        if (parentTxnId == 0) {
            writeOp(OP_TXN_ROLLBACK, txnId);
        } else {
            writeOp(OP_TXN_ROLLBACK_NESTED, txnId);
            writeLong(parentTxnId);
        }
    }

    public void txnCommit(long txnId, long parentTxnId, boolean durable)
        throws IOException
    {
        synchronized (this) {
            if (parentTxnId == 0) {
                writeOp(OP_TXN_COMMIT, txnId);
                writeChecksumAndFlush();
            } else {
                writeOp(OP_TXN_COMMIT_NESTED, txnId);
                writeLong(parentTxnId);
                // No need for nested transactions to be durable.
                return;
            }
        }

        if (durable) {
            mChannel.force(false);
        }
    }

    public synchronized void txnStore(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }

        if (value == null) {
            writeOp(OP_TXN_DELETE, txnId);
            writeLong(indexId);
            writeUnsignedVarInt(key.length);
            writeBytes(key);
        } else {
            writeOp(OP_TXN_STORE, txnId);
            writeLong(indexId);
            writeUnsignedVarInt(key.length);
            writeBytes(key);
            writeUnsignedVarInt(value.length);
            writeBytes(value);
        }
    }

    synchronized void timestamp() throws IOException {
        writeOp(OP_TIMESTAMP, System.currentTimeMillis());
    }

    // Caller must be synchronized.
    private void writeInt(int v) throws IOException {
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        if (pos > buffer.length - 4) {
            flush(buffer, pos);
            pos = 0;
        }
        DataIO.writeInt(buffer, pos, v);
        mBufferPos = pos + 4;
    }

    // Caller must be synchronized.
    private void writeLong(long v) throws IOException {
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        if (pos > buffer.length - 8) {
            flush(buffer, pos);
            pos = 0;
        }
        DataIO.writeLong(buffer, pos, v);
        mBufferPos = pos + 8;
    }

    // Caller must be synchronized.
    private void writeOp(byte op, long operand) throws IOException {
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        if (pos >= buffer.length - 9) {
            flush(buffer, pos);
            pos = 0;
        }
        buffer[pos] = op;
        DataIO.writeLong(buffer, pos + 1, operand);
        mBufferPos = pos + 9;
    }

    // Caller must be synchronized.
    private void writeUnsignedVarInt(int v) throws IOException {
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        if (pos > buffer.length - 5) {
            flush(buffer, pos);
            pos = 0;
        }
        mBufferPos = DataIO.writeUnsignedVarInt(buffer, pos, v);
    }

    // Caller must be synchronized.
    private void writeBytes(byte[] bytes) throws IOException {
        writeBytes(bytes, 0, bytes.length);
    }

    // Caller must be synchronized.
    private void writeBytes(byte[] bytes, int offset, int length) throws IOException {
        if (length == 0) {
            return;
        }
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        while (true) {
            if (pos <= buffer.length - length) {
                System.arraycopy(bytes, offset, buffer, pos, length);
                mBufferPos = pos + length;
                return;
            }
            int remaining = buffer.length - pos;
            System.arraycopy(bytes, offset, buffer, pos, remaining);
            flush(buffer, buffer.length);
            pos = 0;
            offset += remaining;
            length -= remaining;
        }
    }

    // Caller must be synchronized.
    private void writeChecksumAndFlush() throws IOException {
        writeInt((int) mChecksum.getValue());
        flush();
    }

    // Caller must be synchronized.
    private void flush() throws IOException {
        flush(mBuffer, mBufferPos);
    }

    // Caller must be synchronized.
    private void flush(byte[] buffer, int pos) throws IOException {
        mChecksum.update(buffer, 0, pos);
        mOut.write(buffer, 0, pos);
        mBufferPos = 0;
    }
}
