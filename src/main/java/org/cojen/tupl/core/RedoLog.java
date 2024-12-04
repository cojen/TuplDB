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

package org.cojen.tupl.core;

import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

import java.nio.channels.ClosedChannelException;

import java.util.TreeMap;

import java.security.GeneralSecurityException;

import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.WriteFailureException;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

import org.cojen.tupl.ext.Crypto;

import org.cojen.tupl.io.FileIO;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import static org.cojen.tupl.core.LocalDatabase.REDO_FILE_SUFFIX;
import static org.cojen.tupl.core.Utils.*;

/**
 * Implementation of the local (non-replicated) redo log. All redo operations are stored in a
 * single numbered file between checkpoints. A new file is generated when a checkpoint begins,
 * and the old files are deleted when the checkpoint finishes. More than two files can exist if
 * a checkpoint is interrupted before it finishes. The redo log files are only read by database
 * recovery, when the database is opened.
 *
 * @author Brian S O'Neill
 */
final class RedoLog extends RedoWriter {
    private static final long MAGIC_NUMBER = 431399725605778814L;
    private static final int ENCODING_VERSION = 20130106;

    private static final VarHandle cOldFileIOHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();
            cOldFileIOHandle = lookup.findVarHandle(RedoLog.class, "mOldFileIO", FileIO.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private final Crypto mCrypto;
    private final File mBaseFile;

    private final boolean mReplayMode;

    private final byte[] mBuffer;
    private int mBufferPos;

    private boolean mAlwaysFlush;

    private long mLogId;
    private long mPosition;
    private OutputStream mOut;
    private volatile FileIO mFileIO;

    private long mNextLogId;
    private long mNextPosition;
    private OutputStream mNextOut;
    private FileIO mNextFileIO;

    private volatile FileIO mOldFileIO;

    private long mDeleteLogId;

    /**
     * Open for replay.
     *
     * @param logId first log id to create
     */
    RedoLog(Launcher launcher, long logId, long redoPos) throws IOException {
        this(launcher.mRedoCrypto, launcher.mBaseFile, logId, redoPos, null);
    }

    /**
     * Open after replay.
     *
     * @param context used for creating next log file; must not be null
     */
    RedoLog(Launcher launcher, RedoLog replayed, TransactionContext context)
        throws IOException
    {
        this(launcher.mRedoCrypto, launcher.mBaseFile,
             replayed.mLogId, replayed.mPosition, context);
    }

    /**
     * @param crypto optional
     * @param logId first log id to create
     * @param context used for creating next log file; pass null for replay mode
     */
    RedoLog(Crypto crypto, File baseFile, long logId, long redoPos, TransactionContext context)
        throws IOException
    {
        mCrypto = crypto;
        mBaseFile = baseFile;
        mReplayMode = context == null;

        mBuffer = new byte[8192];

        acquireExclusive();
        mLogId = logId;
        mPosition = redoPos;
        releaseExclusive();

        if (context != null) {
            mNextLogId = -1;
            openNextFile(logId);
            applyNextFile(context);
            // Log will be deleted after next checkpoint finishes.
            mDeleteLogId = logId;
        }
    }

    /**
     * @return all the files which were replayed
     */
    TreeMap<Long, File> replay(boolean readOnly, RedoVisitor visitor,
                               EventListener listener, EventType type, String message)
        throws IOException
    {
        if (!mReplayMode || mBaseFile == null) {
            throw new IllegalStateException();
        }

        acquireExclusive();
        try {
            var files = new TreeMap<Long, File>();

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
                            in = mCrypto.newDecryptingStream(in);
                        } catch (IOException e) {
                            throw e;
                        } catch (Exception e) {
                            throw new DatabaseException(e);
                        }
                    }

                    if (listener != null) {
                        listener.notify(type, message, mLogId);
                    }

                    files.put(mLogId, file);

                    var din = new DataIn.Stream(mPosition, in);
                    finished = replay(din, visitor, listener);
                    mPosition = din.mPos;
                } finally {
                    closeQuietly(in);
                }

                mLogId++;

                if (!finished) {
                    if (!readOnly) {
                        // Last log file was truncated, so chuck the rest.
                        deleteNumberedFiles(mBaseFile, REDO_FILE_SUFFIX, mLogId, Long.MAX_VALUE);
                    }
                    break;
                }
            }

            return files;
        } catch (IOException e) {
            throw rethrow(e, mCloseCause);
        } finally {
            releaseExclusive();
        }
    }

    private void openNextFile(long logId) throws IOException {
        if (mNextLogId == logId) {
            // Already open.
            return;
        }

        var header = new byte[8 + 4 + 8 + 4];

        final File file = fileFor(mBaseFile, logId);
        if (file.exists() && file.length() > header.length) {
            throw new FileNotFoundException("Log file already exists: " + file.getPath());
        }

        FileOutputStream fout = null;
        OutputStream nextOut;
        FileIO nextFileIO = null;

        try {
            fout = new FileOutputStream(file);

            // The FileIO object is mainly used for sync'ng data with more control than what
            // the FileDescriptor class offers. Using the FileChannel of the FileOutputStream
            // isn't a good option because an interrupt during a channel operation closes both
            // the channel and the stream. The FileIO object handles interrupts properly,
            // keeping the object open and functional.
            nextFileIO = FileIO.open(file, null, 1);

            if (mCrypto == null) {
                nextOut = fout;
            } else {
                try {
                    nextOut = mCrypto.newEncryptingStream(fout);
                } catch (GeneralSecurityException e) {
                    throw new DatabaseException(e);
                }
            }

            int offset = 0;
            encodeLongLE(header, offset, MAGIC_NUMBER); offset += 8;
            encodeIntLE(header, offset, ENCODING_VERSION); offset += 4;
            encodeLongLE(header, offset, logId); offset += 8;
            encodeIntLE(header, offset, 0); offset += 4; // reserved field
            if (offset != header.length) {
                throw new AssertionError();
            }

            nextOut.write(header);

            // Make sure that parent directory durably records the new log file.
            FileIO.dirSync(file);
        } catch (IOException e) {
            close(fout, nextFileIO);
            try {
                Utils.delete(file);
            } catch (IOException e2) {
                e.addSuppressed(e2);
            }
            throw WriteFailureException.from(e);
        }

        mNextLogId = logId;
        mNextOut = nextOut;
        mNextFileIO = nextFileIO;
    }

    private void applyNextFile(TransactionContext... contexts) throws IOException {
        TransactionContext context = contexts[0];
        for (int i = contexts.length; --i >= 1; ) {
            contexts[i].flush();
        }

        final OutputStream oldOut;
        final FileIO oldFileIO;

        context.fullAcquireRedoLatch(this);
        try {
            // Keep the old FileIO around in case the sync method is called.
            oldFileIO = (FileIO) cOldFileIOHandle.getAndSet(this, mFileIO);

            oldOut = mOut;

            if (oldOut != null) {
                context.doRedoTimestamp(this, RedoOps.OP_END_FILE, DurabilityMode.NO_FLUSH);
                context.doFlush();
                doFlush();
            }

            mNextPosition = mPosition;

            mOut = mNextOut;
            mFileIO = mNextFileIO;
            mLogId = mNextLogId;

            mNextOut = null;
            mNextFileIO = null;

            // Reset the transaction id early in order for terminators to be encoded correctly.
            // RedoLogDecoder always starts with an initial transaction id of 0.
            mLastTxnId = 0;

            context.doRedoTimestamp(this, RedoOps.OP_TIMESTAMP, DurabilityMode.NO_FLUSH);
            context.doRedoReset(this);

            context.doFlush();
        } finally {
            context.releaseRedoLatch();
        }

        closeQuietly(oldOut);
        closeOldFileIO(oldFileIO);
    }

    /**
     * @return null if non-stored
     */
    private static File fileFor(File base, long logId) {
        return base == null ? null : new File(base.getPath() + REDO_FILE_SUFFIX + logId);
    }

    @Override
    void txnCommitSync(long commitPos) throws IOException {
        try {
            sync(false, -1);
        } catch (IOException e) {
            throw rethrow(e, mCloseCause);
        }
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
            FileIO fileIO = mFileIO;
            return fileIO != null && fileIO.length() >= size;
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
        if (mNextOut != null || mNextFileIO != null) {
            close(mNextOut, mNextFileIO);
            mNextOut = null;
            mNextFileIO = null;
        }
    }

    @Override
    void checkpointStarted() throws IOException {
        /* Sync'ng the old redo log increases I/O and slows down the checkpoint. If the
           checkpoint completes, then durable persistence of the old redo log file was
           unnecessary. Applications which require stronger durability can select an
           appropriate mode or call sync periodically.
        */
    }

    @Override
    void checkpointFlushed() throws IOException {
        // Nothing to do.
    }

    @Override
    void checkpointFinished() throws IOException {
        closeOldFileIO();

        for (long id = mNextLogId; --id >= mDeleteLogId; ) {
            // Typically deletes one file, but more will accumulate if checkpoints abort.
            delete(fileFor(mBaseFile, id));
        }

        // Log will be deleted after next checkpoint finishes.
        mDeleteLogId = mNextLogId;
    }

    /**
     * Deletes newly created redo log files that should be effectively empty.
     */
    void initialCheckpointFailed(Throwable cause) {
        try {
            close();
            for (long id = mNextLogId; id >= mDeleteLogId; id--) {
                delete(fileFor(mBaseFile, id));
            }
        } catch (Throwable e2) {
            suppress(cause, e2);
            rethrow(cause);
        }
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
    long write(boolean flush, byte[] bytes, int offset, final int length, int commitLen,
               PendingTxn pending)
        throws IOException
    {
        try {
            byte[] buf = mBuffer;
            int avail = buf.length - mBufferPos;

            if (avail >= length) {
                if (mBufferPos == 0 && avail == length) {
                    mOut.write(bytes, offset, length);
                } else {
                    System.arraycopy(bytes, offset, buf, mBufferPos, length);
                    mBufferPos += length;
                    if (mBufferPos == buf.length || flush || mAlwaysFlush) {
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
                int rem = length - avail;
                if (rem >= buf.length || flush || mAlwaysFlush) {
                    mBufferPos = 0;
                    mOut.write(bytes, offset, rem);
                } else {
                    System.arraycopy(bytes, offset, buf, 0, rem);
                    mBufferPos = rem;
                }
            }

            return mPosition += length;
        } catch (IOException e) {
            throw WriteFailureException.from(e);
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

    @Override
    public void flush() throws IOException {
        acquireExclusive();
        try {
            doFlush();
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
            throw WriteFailureException.from(e);
        }
    }

    @Override
    void sync(boolean metadata, long nanosTimeout) throws IOException {
        FileIO oldFileIO = mOldFileIO;
        if (oldFileIO != null) {
            // Ensure the old file is sync'd before current file. Proper ordering is critical.
            try {
                oldFileIO.sync(true);
                closeOldFileIO(oldFileIO);
            } catch (ClosedChannelException e) {
                // Assume sync was already called.
            } catch (IOException e) {
                if (!oldFileIO.isClosed()) {
                    throw e;
                }
                // Assume sync was already called.
            }
        }

        FileIO fileIO = mFileIO;
        if (fileIO != null) {
            fileIO.sync(metadata);
        }
    }

    @Override
    public void close() {
        close(mNextOut, mNextFileIO);
        close(mOut, mFileIO);
        closeOldFileIO();
    }

    private static void close(OutputStream out, FileIO fileIO) {
        closeQuietly(out);
        closeQuietly(fileIO);
    }

    private void closeOldFileIO() {
        closeOldFileIO(mOldFileIO);
    }

    private void closeOldFileIO(FileIO oldFileIO) {
        cOldFileIOHandle.compareAndSet(this, oldFileIO, null);
        closeQuietly(oldFileIO);
    }

    @Override
    void stashForRecovery(LocalTransaction txn) {
        // Recovery handler can only be invoked when restarting the database.
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

        // Skip a field which was originally used to initialize the terminator sequence.
        in.readIntLE();

        try {
            return new RedoLogDecoder(in, listener).run(visitor);
        } catch (EOFException e) {
            if (listener != null) {
                listener.notify(EventType.RECOVERY_REDO_LOG_CORRUPTION, "Unexpected end of file");
            }
            return false;
        }
    }
}
