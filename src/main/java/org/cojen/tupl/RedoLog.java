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
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.util.LinkedHashSet;
import java.util.Set;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

import java.security.GeneralSecurityException;

import org.cojen.tupl.io.FileFactory;
import org.cojen.tupl.io.FileIO;

/**
 * 
 *
 * @author Brian S O'Neill
 */
/*P*/
final class RedoLog extends RedoWriter {
    private static final long MAGIC_NUMBER = 431399725605778814L;
    private static final int ENCODING_VERSION = 20130106;

    private final Crypto mCrypto;
    private final File mBaseFile;
    private final FileFactory mFileFactory;

    private final boolean mReplayMode;

    private final byte[] mBuffer;
    private int mBufferPos;

    private boolean mAlwaysFlush;

    private long mLogId;
    private long mPosition;
    private OutputStream mOut;
    private volatile FileChannel mChannel;

    private int mTermRndSeed;

    private long mNextLogId;
    private long mNextPosition;
    private OutputStream mNextOut;
    private FileChannel mNextChannel;
    private int mNextTermRndSeed;

    private volatile OutputStream mOldOut;
    private volatile FileChannel mOldChannel;

    private long mDeleteLogId;

    /**
     * Open for replay.
     *
     * @param logId first log id to open
     */
    RedoLog(DatabaseConfig config, long logId, long redoPos) throws IOException {
        this(config.mCrypto, config.mBaseFile, config.mFileFactory, logId, redoPos, null);
    }

    /**
     * Open after replay.
     *
     * @param logId first log id to open
     * @param context used for creating next log file; must not be null
     */
    RedoLog(DatabaseConfig config, RedoLog replayed, TransactionContext context)
        throws IOException
    {
        this(config.mCrypto, config.mBaseFile, config.mFileFactory,
             replayed.mLogId, replayed.mPosition, context);
    }

    /**
     * @param crypto optional
     * @param factory optional
     * @param logId first log id to open
     * @param context used for creating next log file; pass null for replay mode
     */
    RedoLog(Crypto crypto, File baseFile, FileFactory factory,
            long logId, long redoPos, TransactionContext context)
        throws IOException
    {
        mCrypto = crypto;
        mBaseFile = baseFile;
        mFileFactory = factory;
        mReplayMode = context == null;

        mBuffer = new byte[8192];

        acquireExclusive();
        mLogId = logId;
        mPosition = redoPos;
        releaseExclusive();

        if (context != null) {
            openNextFile(logId);
            applyNextFile(context);
            // Log will be deleted after next checkpoint finishes.
            mDeleteLogId = logId;
        }
    }

    /**
     * @return all the files which were replayed
     */
    Set<File> replay(RedoVisitor visitor, EventListener listener, EventType type, String message)
        throws IOException
    {
        if (!mReplayMode || mBaseFile == null) {
            throw new IllegalStateException();
        }

        acquireExclusive();
        try {
            Set<File> files = new LinkedHashSet<>(2);

            while (true) {
                File file = fileFor(mBaseFile, mLogId);

                InputStream in;
                try {
                    in = new FileInputStream(file);
                } catch (FileNotFoundException e) {
                    break;
                }

                boolean finished;
                try {
                    if (mCrypto != null) {
                        try {
                            in = mCrypto.newDecryptingStream(mLogId, in);
                        } catch (IOException e) {
                            throw e;
                        } catch (Exception e) {
                            throw new DatabaseException(e);
                        }
                    }

                    if (listener != null) {
                        listener.notify(type, message, mLogId);
                    }

                    files.add(file);

                    DataIn din = new DataIn.Stream(mPosition, in);
                    finished = replay(din, visitor, listener);
                    mPosition = din.mPos;
                } finally {
                    Utils.closeQuietly(null, in);
                }

                mLogId++;

                if (!finished) {
                    // Last log file was truncated, so chuck the rest.
                    Utils.deleteNumberedFiles(mBaseFile, LocalDatabase.REDO_FILE_SUFFIX, mLogId);
                    break;
                }
            }

            return files;
        } catch (IOException e) {
            throw Utils.rethrow(e, mCloseCause);
        } finally {
            releaseExclusive();
        }
    }

    static void deleteOldFile(File baseFile, long logId) {
        fileFor(baseFile, logId).delete();
    }

    private void openNextFile(long logId) throws IOException {
        byte[] header = new byte[8 + 4 + 8 + 4];

        final File file = fileFor(mBaseFile, logId);
        if (file.exists() && file.length() > header.length) {
            throw new FileNotFoundException("Log file already exists: " + file.getPath());
        }

        if (mFileFactory != null) {
            mFileFactory.createFile(file);
        }

        FileOutputStream fout = null;
        OutputStream nextOut;
        FileChannel nextChannel;

        // Zero indicates that Xorshift random numbers aren't used for terminators anymore.
        int nextTermRndSeed = 0;

        try {
            fout = new FileOutputStream(file);
            nextChannel = fout.getChannel();

            if (mCrypto == null) {
                nextOut = fout;
            } else {
                try {
                    nextOut = mCrypto.newEncryptingStream(logId, fout);
                } catch (GeneralSecurityException e) {
                    throw new DatabaseException(e);
                }
            }

            int offset = 0;
            Utils.encodeLongLE(header, offset, MAGIC_NUMBER); offset += 8;
            Utils.encodeIntLE(header, offset, ENCODING_VERSION); offset += 4;
            Utils.encodeLongLE(header, offset, logId); offset += 8;
            Utils.encodeIntLE(header, offset, nextTermRndSeed); offset += 4;
            if (offset != header.length) {
                throw new AssertionError();
            }

            nextOut.write(header);

            // Make sure that parent directory durably records the new log file.
            FileIO.dirSync(file);
        } catch (IOException e) {
            Utils.closeQuietly(null, fout);
            file.delete();
            throw new WriteFailureException(e);
        }

        mNextLogId = logId;
        mNextOut = nextOut;
        mNextChannel = nextChannel;
        mNextTermRndSeed = nextTermRndSeed;
    }

    private void applyNextFile(TransactionContext... contexts) throws IOException {
        final OutputStream oldOut;
        final FileChannel oldChannel;

        TransactionContext context = contexts[0];
        for (int i = contexts.length; --i >= 1; ) {
            contexts[i].flush();
        }

        context.fullAcquireRedoLatch(this);
        try {
            oldOut = mOut;
            oldChannel = mChannel;

            if (oldOut != null) {
                context.doRedoTimestamp(this, RedoOps.OP_END_FILE);
                context.doFlush();
                doFlush();
            }

            mNextPosition = mPosition;

            mOut = mNextOut;
            mChannel = mNextChannel;
            mTermRndSeed = mNextTermRndSeed;
            mLogId = mNextLogId;

            mNextOut = null;
            mNextChannel = null;

            // Reset the transaction id early in order for terminators to be encoded correctly.
            // RedoLogDecoder always starts with an initial transaction id of 0.
            mLastTxnId = 0;

            context.doRedoTimestamp(this, RedoOps.OP_TIMESTAMP);
            context.doRedoReset(this);

            context.doFlush();
        } finally {
            context.releaseRedoLatch();
        }

        // Close old file if previous checkpoint aborted.
        Utils.closeQuietly(null, mOldOut);

        mOldOut = oldOut;
        mOldChannel = oldChannel;
    }

    /**
     * @return null if non-durable
     */
    private static File fileFor(File base, long logId) {
        return base == null ? null : new File(base.getPath() + ".redo." + logId);
    }

    @Override
    void txnCommitSync(LocalTransaction txn, long commitPos) throws IOException {
        try {
            force(false);
        } catch (IOException e) {
            throw Utils.rethrow(e, mCloseCause);
        }
    }

    @Override
    void txnCommitPending(PendingTxn pending) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    final long encoding() {
        return 0;
    }

    @Override
    final RedoWriter txnRedoWriter() {
        return this;
    }

    @Override
    boolean shouldCheckpoint(long size) {
        try {
            FileChannel channel = mChannel;
            return channel != null && channel.size() >= size;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    void checkpointPrepare() throws IOException {
        if (mReplayMode) {
            throw new IllegalStateException();
        }

        acquireShared();
        final long logId = mLogId;
        releaseShared();

        openNextFile(logId + 1);
    }

    @Override
    void checkpointSwitch(TransactionContext[] contexts) throws IOException {
        applyNextFile(contexts);
    }

    @Override
    long checkpointNumber() {
        return mNextLogId;
    }

    @Override
    long checkpointPosition() {
        return mNextPosition;
    }

    @Override
    long checkpointTransactionId() {
        // Log file always begins with a reset.
        return 0;
    }

    @Override
    void checkpointAborted() {
        if (mNextOut != null) {
            Utils.closeQuietly(null, mNextOut);
            mNextOut = null;
        }
    }

    @Override
    void checkpointStarted() throws IOException {
        /* Forcing the old redo log increases I/O and slows down the checkpoint. If the
           checkpoint completes, then durable persistence of the old redo log file was
           unnecessary. Applications which require stronger durability can select an
           appropriate mode or call sync periodically.

        FileChannel oldChannel = mOldChannel;

        if (oldChannel != null) {
            // Make sure any exception thrown by this call is not caught here,
            // because a checkpoint cannot complete successfully if the redo
            // log has not been durably written.
            oldChannel.force(true);
            mOldChannel = null;
        }

        Utils.closeQuietly(null, mOldOut);
        */
    }

    @Override
    void checkpointFlushed() throws IOException {
        // Nothing to do.
    }

    @Override
    void checkpointFinished() throws IOException {
        mOldChannel = null;
        Utils.closeQuietly(null, mOldOut);
        long id = mDeleteLogId;
        for (; id < mNextLogId; id++) {
            // Typically deletes one file, but more will accumulate if checkpoints abort.
            deleteOldFile(mBaseFile, id);
        }
        // Log will be deleted after next checkpoint finishes.
        mDeleteLogId = id;
    }

    @Override
    DurabilityMode opWriteCheck(DurabilityMode mode) throws IOException {
        // Mode stays the same when not replicated.
        return mode;
    }

    @Override
    boolean shouldWriteTerminators() {
        return true;
    }

    @Override
    long write(boolean commit, byte[] bytes, int offset, int length) throws IOException {
        try {
            byte[] buf = mBuffer;
            int avail = buf.length - mBufferPos;

            if (avail >= length) {
                if (mBufferPos == 0 && avail == length) {
                    mOut.write(bytes, offset, length);
                } else {
                    System.arraycopy(bytes, offset, buf, mBufferPos, length);
                    mBufferPos += length;
                    if (mBufferPos == buf.length || commit || mAlwaysFlush) {
                        mOut.write(buf, 0, mBufferPos);
                        mBufferPos = 0;
                    }
                }
            } else {
                // Fill remainder of buffer and flush it.
                System.arraycopy(bytes, offset, buf, mBufferPos, avail);
                mBufferPos = buf.length;
                mOut.write(buf, 0, mBufferPos);
                offset += avail;
                length -= avail;
                if (length >= buf.length || commit || mAlwaysFlush) {
                    mBufferPos = 0;
                    mOut.write(bytes, offset, length);
                } else {
                    System.arraycopy(bytes, offset, buf, 0, length);
                    mBufferPos = length;
                }
            }

            return mPosition += length;
        } catch (IOException e) {
            throw new WriteFailureException(e);
        }
    }

    @Override
    void alwaysFlush(boolean enable) throws IOException {
        acquireExclusive();
        try {
            mAlwaysFlush = enable;
            if (enable) {
                doFlush();
            }
        } finally {
            releaseExclusive();
        }
    }

    private void doFlush() throws IOException {
        try {
            if (mBufferPos > 0) {
                mOut.write(mBuffer, 0, mBufferPos);
                mBufferPos = 0;
            }
        } catch (IOException e) {
            throw new WriteFailureException(e);
        }
    }

    @Override
    void force(boolean metadata) throws IOException {
        FileChannel oldChannel = mOldChannel;
        if (oldChannel != null) {
            // Ensure old file is forced before current file. Proper ordering is critical.
            try {
                oldChannel.force(true);
            } catch (ClosedChannelException e) {
                // Ignore.
            }
            mOldChannel = null;
        }

        FileChannel channel = mChannel;
        if (channel != null) {
            try {
                channel.force(metadata);
            } catch (ClosedChannelException e) {
                // Ignore.
            }
        }
    }

    @Override
    public void close() throws IOException {
        Utils.closeQuietly(null, mOldOut);

        FileChannel channel = mChannel;
        if (channel != null) {
            try {
                channel.close();
            } catch (ClosedChannelException e) {
                // Ignore.
            }
        }

        Utils.closeQuietly(null, mOut);
    }

    // Caller must hold exclusive latch (replay is exempt)
    int nextTermRnd() {
        return mTermRndSeed = Utils.nextRandom(mTermRndSeed);
    }

    private boolean replay(DataIn in, RedoVisitor visitor, EventListener listener)
        throws IOException
    {
        try {
            long magic = in.readLongLE();
            if (magic != MAGIC_NUMBER) {
                if (magic == 0) {
                    // Assume file was flushed improperly and discard it.
                    return false;
                }
                throw new DatabaseException("Incorrect magic number in redo log file");
            }
        } catch (EOFException e) {
            // Assume file was flushed improperly and discard it.
            return false;
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

        try {
            return new RedoLogDecoder(this, in, listener).run(visitor);
        } catch (EOFException e) {
            if (listener != null) {
                listener.notify(EventType.RECOVERY_REDO_LOG_CORRUPTION, "Unexpected end of file");
            }
            return false;
        }
    }
}
