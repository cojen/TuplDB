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
import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import java.nio.channels.FileChannel;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class RedoLog implements Closeable, RedoLogVisitor {
    private static final long MAGIC_NUMBER = 431399725605778814L;
    private static final int ENCODING_VERSION = 20111001;

    private static final byte
        /** timestamp: long */
        OP_TIMESTAMP = 1,

        /** timestamp: long */
        OP_SHUTDOWN = 2,

        /** timestamp: long */
        OP_CLOSE = 3,

        /** txnId: long */
        //OP_TXN_BEGIN = 4,

        /** txnId: long, parentTxnId: long */
        //OP_TXN_BEGIN_NESTED = 5,

        /** txnId: long */
        //OP_TXN_CONTINUE = 6,

        /** txnId: long, parentTxnId: long */
        //OP_TXN_CONTINUE_NESTED = 7,

        /** txnId: long */
        OP_TXN_ROLLBACK = 8,

        /** txnId: long, parentTxnId: long */
        OP_TXN_ROLLBACK_NESTED = 9,

        /** txnId: long */
        OP_TXN_COMMIT = 10,

        /** txnId: long, parentTxnId: long */
        OP_TXN_COMMIT_NESTED = 11,

        /** indexId: long, keyLength: varInt, key: bytes, valueLength: varInt, value: bytes */
        OP_STORE = 16,

        /** indexId: long, keyLength: varInt, key: bytes */
        OP_DELETE = 17,

        /** indexId: long */
        OP_CLEAR = 18,

        /** txnId: long, indexId: long, keyLength: varInt, key: bytes,
            valueLength: varInt, value: bytes */
        OP_TXN_STORE = 19,

        /** txnId: long, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_DELETE = 20,

        /** txnId: long, indexId: long */
        //OP_TXN_CLEAR = 21,

        /** length: varInt, data: bytes */
        OP_CUSTOM = (byte) 128,

        /** txnId: long, length: varInt, data: bytes */
        OP_TXN_CUSTOM = (byte) 129;

    private File mBaseFile;

    private final byte[] mBuffer;
    private int mBufferPos;

    private FileOutputStream mOut;
    private FileChannel mChannel;

    private boolean mAlwaysFlush;

    RedoLog(File baseFile, long logNumber) throws IOException {
        mBaseFile = baseFile;
        mBuffer = new byte[4096];

        open(logNumber);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    shutdown(OP_SHUTDOWN);
                } catch (Throwable e) {
                    Utils.rethrow(e);
                }
            }
        });
    }

    void replay(RedoLogVisitor visitor) throws IOException {
        // FIXME
        throw null;
    }

    private synchronized void open(long logNumber) throws IOException {
        File file = new File(mBaseFile.getPath() + ".redo." + logNumber);
        if (file.exists()) {
            throw new FileNotFoundException("File already exists: " + file.getPath());
        }

        mOut = new FileOutputStream(file);
        mChannel = mOut.getChannel();

        writeLong(MAGIC_NUMBER);
        writeInt(ENCODING_VERSION);
        writeLong(logNumber);
        timestamp();
        flush();
    }

    public synchronized void close() throws IOException {
        shutdown(OP_CLOSE);
    }

    void shutdown(byte op) throws IOException {
        synchronized (this) {
            mAlwaysFlush = true;
            if (!mChannel.isOpen()) {
                return;
            }
            writeOp(op, System.currentTimeMillis());
            flush();
        }
        mChannel.force(true);
        if (op == OP_CLOSE) {
            mChannel.close();
        }
    }

    public void store(long indexId, byte[] key, byte[] value, DurabilityMode mode)
        throws IOException
    {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }

        boolean sync;
        synchronized (this) {
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

            sync = conditionalFlush(mode);
        }

        if (sync) {
            mChannel.force(false);
        }
    }

    public void clear(long indexId, DurabilityMode mode) throws IOException {
        boolean sync;
        synchronized (this) {
            writeOp(OP_CLEAR, indexId);
            sync = conditionalFlush(mode);
        }

        if (sync) {
            mChannel.force(false);
        }
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

    public void txnCommit(long txnId, long parentTxnId, DurabilityMode mode)
        throws IOException
    {
        boolean sync;
        synchronized (this) {
            if (parentTxnId == 0) {
                writeOp(OP_TXN_COMMIT, txnId);
                sync = conditionalFlush(mode);
            } else {
                writeOp(OP_TXN_COMMIT_NESTED, txnId);
                writeLong(parentTxnId);
                // No need for nested transactions to be durable.
                return;
            }
        }

        if (sync) {
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

    // Caller must be synchronized. Returns true if caller should sync.
    private boolean conditionalFlush(DurabilityMode mode) throws IOException {
        switch (mode) {
        default:
            return false;
        case NO_FLUSH:
            if (mAlwaysFlush) {
                flush();
            }
            return false;
        case SYNC:
            flush();
            return true;
        case NO_SYNC:
            flush();
            return false;
        }
    }

    // Caller must be synchronized.
    private void flush() throws IOException {
        flush(mBuffer, mBufferPos);
    }

    // Caller must be synchronized.
    private void flush(byte[] buffer, int pos) throws IOException {
        mOut.write(buffer, 0, pos);
        mBufferPos = 0;
    }
}
