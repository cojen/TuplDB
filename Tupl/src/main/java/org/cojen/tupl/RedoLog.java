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

import java.io.BufferedInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class RedoLog implements Closeable, Checkpointer.Shutdown {
    private static final long MAGIC_NUMBER = 431399725605778814L;
    private static final int ENCODING_VERSION = 20120105;

    private static final byte
        OP_NOP = 0,

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

    private final File mBaseFile;

    private final byte[] mBuffer;
    private int mBufferPos;

    private long mLogId;
    private FileOutputStream mOut;
    private volatile FileChannel mChannel;

    private boolean mReplayMode;

    private boolean mAlwaysFlush;

    /**
     * RedoLog starts in replay mode.
     */
    RedoLog(File baseFile, long logId) throws IOException {
        mBaseFile = baseFile;
        mBuffer = new byte[4096];

        synchronized (this) {
            mLogId = logId;
            mReplayMode = true;
        }
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

        DataIn in;
        try {
            in = new DataIn(new FileInputStream(file));
        } catch (FileNotFoundException e) {
            mReplayMode = false;
            openFile(mLogId);
            return false;
        }

        try {
            replay(in, visitor);
        } catch (EOFException e) {
            // End of log didn't get completely flushed.
        } finally {
            Utils.closeQuietly(null, in);
        }

        return true;
    }

    /**
     * @return old log file id, which is one less than new one
     */
    synchronized long openNewFile() throws IOException {
        if (mOut != null) {
            writeOp(OP_END_FILE, System.currentTimeMillis());
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

        final FileOutputStream oldOut = mOut;
        final FileOutputStream out = new FileOutputStream(file);

        try {
            mOut = out;
            mChannel = out.getChannel();

            writeLongLE(MAGIC_NUMBER);
            writeIntLE(ENCODING_VERSION);
            writeLongLE(logId);
            timestamp();
            doFlush();
        } catch (IOException e) {
            mChannel = ((mOut = oldOut) == null) ? null : oldOut.getChannel();
            Utils.closeQuietly(null, out);
            file.delete();

            throw e;
        }

        Utils.closeQuietly(null, oldOut);

        mLogId = logId;
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
            }
        }
    }

    public synchronized void close() throws IOException {
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
    }

    /**
     * @return true if caller should call txnCommitSync
     */
    public boolean txnCommitFull(long txnId, DurabilityMode mode)
        throws IOException
    {
        synchronized (this) {
            writeOp(OP_TXN_COMMIT, txnId);
            return conditionalFlush(mode);
        }
    }

    /**
     * Called after txnCommitFull.
     */
    public void txnCommitSync() throws IOException {
        force(false);
    }

    public void txnCommitScope(long txnId, long parentTxnId) throws IOException {
        synchronized (this) {
            writeOp(OP_TXN_COMMIT_CHILD, txnId);
            writeLongLE(parentTxnId);
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
    }

    synchronized void timestamp() throws IOException {
        writeOp(OP_TIMESTAMP, System.currentTimeMillis());
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
        mOut.write(buffer, 0, pos);
        mBufferPos = 0;
    }

    private void replay(DataIn in, RedoLogVisitor visitor) throws IOException {
        if (in.readLongLE() != MAGIC_NUMBER) {
            throw new DatabaseException("Incorrect magic number in log file");
        }
        int version = in.readIntLE();
        if (version != ENCODING_VERSION) {
            throw new DatabaseException("Unsupported encoding version: " + version);
        }
        long id = in.readLongLE();
        if (id != mLogId) {
            throw new DatabaseException
                ("Expected log identifier of " + mLogId + ", but actual is: " + id);
        }

        int op;
        while ((op = in.read()) >= 0) {
            op = op & 0xff;
            switch (op) {
            default:
                throw new DatabaseException("Unknown log operation: " + op);

            case OP_NOP:
                // Can be caused by recovered log file which was not flushed
                // properly by operating system.
                break;

            case OP_TIMESTAMP:
                visitor.timestamp(in.readLongLE());
                break;

            case OP_SHUTDOWN:
                visitor.shutdown(in.readLongLE());
                break;

            case OP_CLOSE:
                visitor.close(in.readLongLE());
                break;

            case OP_END_FILE:
                visitor.endFile(in.readLongLE());
                break;

            case OP_TXN_ROLLBACK:
                visitor.txnRollback(in.readLongLE(), 0);
                break;

            case OP_TXN_ROLLBACK_CHILD:
                visitor.txnRollback(in.readLongLE(), in.readLongLE());
                break;

            case OP_TXN_COMMIT:
                visitor.txnCommit(in.readLongLE(), 0);
                break;

            case OP_TXN_COMMIT_CHILD:
                visitor.txnCommit(in.readLongLE(), in.readLongLE());
                break;

            case OP_STORE:
                visitor.store(in.readLongLE(), in.readBytes(), in.readBytes());
                break;

            case OP_DELETE:
                visitor.store(in.readLongLE(), in.readBytes(), null);
                break;

            case OP_TXN_STORE:
                visitor.txnStore(in.readLongLE(), in.readLongLE(), in.readBytes(), in.readBytes());
                break;

            case OP_TXN_STORE_COMMIT:
                long txnId = in.readLongLE();
                visitor.txnStore(txnId, in.readLongLE(), in.readBytes(), in.readBytes());
                visitor.txnCommit(txnId, 0);
                break;

            case OP_TXN_STORE_COMMIT_CHILD:
                txnId = in.readLongLE();
                long parentTxnId = in.readLongLE();
                visitor.txnStore(txnId, in.readLongLE(), in.readBytes(), in.readBytes());
                visitor.txnCommit(txnId, parentTxnId);
                break;

            case OP_TXN_DELETE:
                visitor.txnStore(in.readLongLE(), in.readLongLE(), in.readBytes(), null);
                break;

            case OP_TXN_DELETE_COMMIT:
                txnId = in.readLongLE();
                visitor.txnStore(txnId, in.readLongLE(), in.readBytes(), null);
                visitor.txnCommit(txnId, 0);
                break;

            case OP_TXN_DELETE_COMMIT_CHILD:
                txnId = in.readLongLE();
                parentTxnId = in.readLongLE();
                visitor.txnStore(txnId, in.readLongLE(), in.readBytes(), null);
                visitor.txnCommit(txnId, parentTxnId);
                break;
            }
        }
    }
}
