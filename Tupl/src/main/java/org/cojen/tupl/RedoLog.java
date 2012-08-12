/*
 *  Copyright 2011-2012 Brian S O'Neill
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

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.util.Random;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

import java.security.GeneralSecurityException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class RedoLog extends CauseCloseable implements Checkpointer.Shutdown {
    private static final long MAGIC_NUMBER = 431399725605778814L;
    private static final int ENCODING_VERSION = 20120801;

    private static final byte
        /** timestamp: long */
        OP_TIMESTAMP = 1,

        /** timestamp: long */
        OP_SHUTDOWN = 2,

        /** timestamp: long */
        OP_CLOSE = 3,

        /** timestamp: long */
        OP_END_FILE = 4,

        /** txnId: long */
        //OP_TXN_BEGIN = 5,

        /** txnId: long, parentTxnId: long */
        //OP_TXN_BEGIN_CHILD = 6,

        /** txnId: long */
        //OP_TXN_CONTINUE = 7,

        /** txnId: long, parentTxnId: long */
        //OP_TXN_CONTINUE_CHILD = 8,

        /** txnId: long */
        OP_TXN_ROLLBACK = 9,

        /** txnId: long, parentTxnId: long */
        OP_TXN_ROLLBACK_CHILD = 10,

        /** txnId: long */
        OP_TXN_COMMIT = 11,

        /** txnId: long, parentTxnId: long */
        OP_TXN_COMMIT_CHILD = 12,

        /** indexId: long, keyLength: varInt, key: bytes, valueLength: varInt, value: bytes */
        OP_STORE = 16,

        /** indexId: long, keyLength: varInt, key: bytes */
        OP_DELETE = 17,

        /** indexId: long */
        //OP_CLEAR = 18,

        /** txnId: long, indexId: long, keyLength: varInt, key: bytes,
            valueLength: varInt, value: bytes */
        OP_TXN_STORE = 19,

        /** txnId: long, indexId: long, keyLength: varInt, key: bytes,
            valueLength: varInt, value: bytes */
        OP_TXN_STORE_COMMIT = 20,

        /** txnId: long, parentTxnId: long, indexId: long, keyLength: varInt, key: bytes,
            valueLength: varInt, value: bytes */
        OP_TXN_STORE_COMMIT_CHILD = 21,

        /** txnId: long, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_DELETE = 22,

        /** txnId: long, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_DELETE_COMMIT = 23,

        /** txnId: long, parentTxnId: long, indexId: long, keyLength: varInt, key: bytes */
        OP_TXN_DELETE_COMMIT_CHILD = 24;

        /** txnId: long, indexId: long */
        //OP_TXN_CLEAR = 25,

        /** txnId: long, indexId: long */
        //OP_TXN_CLEAR_COMMIT = 26,

        /** txnId: long, parentTxnId: long, indexId: long */
        //OP_TXN_CLEAR_COMMIT_CHILD = 27,

        /** length: varInt, data: bytes */
        //OP_CUSTOM = (byte) 128,

        /** txnId: long, length: varInt, data: bytes */
        //OP_TXN_CUSTOM = (byte) 129;

    private static int randomInt() {
        Random rnd = new Random();
        int x;
        // Cannot return zero, since it breaks Xorshift RNG.
        while ((x = rnd.nextInt()) == 0);
        return x;
    }

    private final Crypto mCrypto;
    private final File mBaseFile;

    private final byte[] mBuffer;
    private int mBufferPos;

    private long mLogId;
    private OutputStream mOut;
    private volatile FileChannel mChannel;

    private boolean mReplayMode;

    private boolean mAlwaysFlush;

    private int mTermRndSeed;

    private volatile Throwable mCause;

    /**
     * RedoLog starts in replay mode.
     */
    RedoLog(Crypto crypto, File baseFile, long logId) throws IOException {
        mCrypto = crypto;
        mBaseFile = baseFile;
        mBuffer = new byte[4096];

        synchronized (this) {
            mLogId = logId;
            mReplayMode = true;
        }
    }

    synchronized long logId() {
        return mLogId;
    }

    synchronized boolean isReplayMode() {
        return mReplayMode;
    }

    /**
     * @return false if file not found and replay mode is deactivated
     */
    synchronized boolean replay(RedoLogVisitor visitor) throws IOException {
        if (!mReplayMode) {
            throw new IllegalStateException();
        }

        File file = fileFor(mLogId);
        if (file == null) {
            return true;
        }

        try {
            DataIn in;
            try {
                InputStream fin = new FileInputStream(file);
                if (mCrypto != null) {
                    fin = mCrypto.newDecryptingStream(mLogId, fin);
                }
                in = new DataIn(fin);
            } catch (FileNotFoundException e) {
                mReplayMode = false;
                openFile(mLogId);
                return false;
            } catch (GeneralSecurityException e) {
                throw new DatabaseException(e);
            }

            try {
                replay(in, visitor);
            } catch (EOFException e) {
                // End of log didn't get completely flushed.
            } finally {
                Utils.closeQuietly(null, in);
            }

            return true;
        } catch (IOException e) {
            throw Utils.rethrow(e, mCause);
        }
    }

    /**
     * @return old log file id, which is one less than new one
     */
    synchronized long openNewFile() throws IOException {
        if (mOut != null) {
            writeOp(OP_END_FILE, System.currentTimeMillis());
            writeTerminator();
            doFlush();
        }
        long logId = mLogId;
        openFile(logId + 1);
        return logId;
    }

    void deleteOldFile(long logId) {
        fileFor(logId).delete();
    }

    private synchronized void openFile(long logId) throws IOException {
        final File file = fileFor(logId);
        if (file.exists()) {
            if (mReplayMode) {
                mLogId = logId;
                return;
            }
            throw new FileNotFoundException("Log file already exists: " + file.getPath());
        }

        mReplayMode = false;

        try {
            final OutputStream oldOut = mOut;
            final FileChannel oldChannel = mChannel;

            final OutputStream out;
            final FileChannel channel;
            {
                FileOutputStream fout = new FileOutputStream(file);
                channel = fout.getChannel();
                if (mCrypto == null) {
                    out = fout;
                } else {
                    try {
                        out = mCrypto.newEncryptingStream(logId, fout);
                    } catch (GeneralSecurityException e) {
                        throw new DatabaseException(e);
                    }
                }
            }

            try {
                mOut = out;
                mChannel = channel;

                writeLongLE(MAGIC_NUMBER);
                writeIntLE(ENCODING_VERSION);
                writeLongLE(logId);
                writeIntLE(mTermRndSeed = randomInt());
                timestamp();
                doFlush();
            } catch (IOException e) {
                mChannel = ((mOut = oldOut) == null) ? null : oldChannel;
                Utils.closeQuietly(null, out);
                file.delete();

                throw e;
            }

            Utils.closeQuietly(null, oldOut);

            mLogId = logId;
        } catch (IOException e) {
            throw Utils.rethrow(e, mCause);
        }
    }

    /**
     * @return null if non-durable
     */
    private File fileFor(long logId) {
        File base = mBaseFile;
        return base == null ? null :  new File(base.getPath() + ".redo." + logId);
    }

    public synchronized void flush() throws IOException {
        doFlush();
    }

    public void sync() throws IOException {
        flush();
        force(false);
    }

    private void force(boolean metadata) throws IOException {
        FileChannel channel = mChannel;
        if (channel != null) {
            try {
                channel.force(metadata);
            } catch (ClosedChannelException e) {
            } catch (IOException e) {
                throw Utils.rethrow(e, mCause);
            }
        }
    }

    @Override
    public synchronized void close() throws IOException {
        close(null);
    }

    @Override
    public synchronized void close(Throwable cause) throws IOException {
        if (cause != null) {
            mCause = cause;
        }
        shutdown(OP_CLOSE);
    }

    @Override
    public void shutdown() {
        try {
            shutdown(OP_SHUTDOWN);
        } catch (IOException e) {
            // Ignore.
        }
    }

    void shutdown(byte op) throws IOException {
        synchronized (this) {
            mAlwaysFlush = true;

            if (mChannel == null || !mChannel.isOpen()) {
                return;
            }

            writeOp(op, System.currentTimeMillis());
            writeTerminator();
            doFlush();

            if (op == OP_CLOSE) {
                mChannel.force(true);
                mChannel.close();
                return;
            }
        }

        force(true);
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
            writeTerminator();

            sync = conditionalFlush(mode);
        }

        if (sync) {
            force(false);
        }
    }

    public synchronized void txnRollback(long txnId, long parentTxnId) throws IOException {
        if (parentTxnId == 0) {
            writeOp(OP_TXN_ROLLBACK, txnId);
        } else {
            writeOp(OP_TXN_ROLLBACK_CHILD, txnId);
            writeLongLE(parentTxnId);
        }
        writeTerminator();
    }

    /**
     * @return true if caller should call txnCommitSync
     */
    public synchronized boolean txnCommitFull(long txnId, DurabilityMode mode) throws IOException {
        writeOp(OP_TXN_COMMIT, txnId);
        writeTerminator();
        return conditionalFlush(mode);
    }

    /**
     * Called after txnCommitFull.
     */
    public void txnCommitSync() throws IOException {
        force(false);
    }

    public synchronized void txnCommitScope(long txnId, long parentTxnId) throws IOException {
        writeOp(OP_TXN_COMMIT_CHILD, txnId);
        writeLongLE(parentTxnId);
        writeTerminator();
    }

    public synchronized void txnStore(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException
    {
        if (key == null) {
            throw new NullPointerException("Key is null");
        }

        if (value == null) {
            writeOp(OP_TXN_DELETE, txnId);
            writeLongLE(indexId);
            writeUnsignedVarInt(key.length);
            writeBytes(key);
        } else {
            writeOp(OP_TXN_STORE, txnId);
            writeLongLE(indexId);
            writeUnsignedVarInt(key.length);
            writeBytes(key);
            writeUnsignedVarInt(value.length);
            writeBytes(value);
        }

        writeTerminator();
    }

    synchronized void timestamp() throws IOException {
        writeOp(OP_TIMESTAMP, System.currentTimeMillis());
        writeTerminator();
    }

    // Caller must be synchronized.
    private void writeIntLE(int v) throws IOException {
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        if (pos > buffer.length - 4) {
            doFlush(buffer, pos);
            pos = 0;
        }
        Utils.writeIntLE(buffer, pos, v);
        mBufferPos = pos + 4;
    }

    // Caller must be synchronized.
    private void writeLongLE(long v) throws IOException {
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        if (pos > buffer.length - 8) {
            doFlush(buffer, pos);
            pos = 0;
        }
        Utils.writeLongLE(buffer, pos, v);
        mBufferPos = pos + 8;
    }

    // Caller must be synchronized.
    private void writeOp(byte op, long operand) throws IOException {
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        if (pos >= buffer.length - 9) {
            doFlush(buffer, pos);
            pos = 0;
        }
        buffer[pos] = op;
        Utils.writeLongLE(buffer, pos + 1, operand);
        mBufferPos = pos + 9;
    }

    // Caller must be synchronized.
    private void writeTerminator() throws IOException {
        writeIntLE(nextTermRnd());
    }

    // Caller must be synchronized (replay is exempt)
    private int nextTermRnd() throws IOException {
        // Xorshift RNG by George Marsaglia.
        int x = mTermRndSeed;
        x ^= x << 13;
        x ^= x >>> 17;
        x ^= x << 5;
        mTermRndSeed = x;
        return x;
    }

    // Caller must be synchronized.
    private void writeUnsignedVarInt(int v) throws IOException {
        byte[] buffer = mBuffer;
        int pos = mBufferPos;
        if (pos > buffer.length - 5) {
            doFlush(buffer, pos);
            pos = 0;
        }
        mBufferPos = Utils.writeUnsignedVarInt(buffer, pos, v);
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
            doFlush(buffer, buffer.length);
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
                doFlush();
            }
            return false;
        case SYNC:
            doFlush();
            return true;
        case NO_SYNC:
            doFlush();
            return false;
        }
    }

    // Caller must be synchronized.
    private void doFlush() throws IOException {
        doFlush(mBuffer, mBufferPos);
    }

    // Caller must be synchronized.
    private void doFlush(byte[] buffer, int pos) throws IOException {
        try {
            mOut.write(buffer, 0, pos);
            mBufferPos = 0;
        } catch (IOException e) {
            throw Utils.rethrow(e, mCause);
        }
    }

    private void replay(DataIn in, RedoLogVisitor visitor) throws IOException {
        long magic = in.readLongLE();
        if (magic != MAGIC_NUMBER) {
            if (magic == 0) {
                // Assume file was flushed improperly and discard it.
                return;
            }
            throw new DatabaseException("Incorrect magic number in redo log file");
        }

        int version = in.readIntLE();
        if (version != ENCODING_VERSION) {
            throw new DatabaseException("Unsupported redo log encoding version: " + version);
        }

        long id = in.readLongLE();
        if (id != mLogId) {
            throw new DatabaseException
                ("Expected redo log identifier of " + mLogId + ", but actual is: " + id);
        }

        mTermRndSeed = in.readIntLE();

        int op;
        while ((op = in.read()) >= 0) {
            long operand = in.readLongLE();

            switch (op &= 0xff) {
            default:
                throw new DatabaseException("Unknown redo log operation: " + op);

            case OP_TIMESTAMP:
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.timestamp(operand);
                break;

            case OP_SHUTDOWN:
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.shutdown(operand);
                break;

            case OP_CLOSE:
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.close(operand);
                break;

            case OP_END_FILE:
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.endFile(operand);
                break;

            case OP_TXN_ROLLBACK:
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnRollback(operand, 0);
                break;

            case OP_TXN_ROLLBACK_CHILD:
                long parentTxnId = in.readLongLE();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnRollback(operand, parentTxnId);
                break;

            case OP_TXN_COMMIT:
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnCommit(operand, 0);
                break;

            case OP_TXN_COMMIT_CHILD:
                parentTxnId = in.readLongLE();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnCommit(operand, parentTxnId);
                break;

            case OP_STORE:
                byte[] key = in.readBytes();
                byte[] value = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.store(operand, key, value);
                break;

            case OP_DELETE:
                key = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.store(operand, key, null);
                break;

            case OP_TXN_STORE:
                long indexId = in.readLongLE();
                key = in.readBytes();
                value = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnStore(operand, indexId, key, value);
                break;

            case OP_TXN_STORE_COMMIT:
                indexId = in.readLongLE();
                key = in.readBytes();
                value = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnStore(operand, indexId, key, value);
                visitor.txnCommit(operand, 0);
                break;

            case OP_TXN_STORE_COMMIT_CHILD:
                parentTxnId = in.readLongLE();
                indexId = in.readLongLE();
                key = in.readBytes();
                value = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnStore(operand, indexId, key, value);
                visitor.txnCommit(operand, parentTxnId);
                break;

            case OP_TXN_DELETE:
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnStore(operand, indexId, key, null);
                break;

            case OP_TXN_DELETE_COMMIT:
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnStore(operand, indexId, key, null);
                visitor.txnCommit(operand, 0);
                break;

            case OP_TXN_DELETE_COMMIT_CHILD:
                parentTxnId = in.readLongLE();
                indexId = in.readLongLE();
                key = in.readBytes();
                if (!verifyTerminator(in)) {
                    return;
                }
                visitor.txnStore(operand, indexId, key, null);
                visitor.txnCommit(operand, parentTxnId);
                break;
            }
        }
    }

    /**
     * If false is returned, assume rest of log file is corrupt.
     */
    private boolean verifyTerminator(DataIn in) throws IOException {
        try {
            return in.readIntLE() == nextTermRnd();
        } catch (EOFException e) {
            return false;
        }
    }
}
