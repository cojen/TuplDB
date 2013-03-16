/*
 *  Copyright 2011-2013 Brian S O'Neill
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

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.Random;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;

import java.security.GeneralSecurityException;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class RedoLog extends RedoWriter {
    private static final long MAGIC_NUMBER = 431399725605778814L;
    private static final int ENCODING_VERSION = 20130106;

    static int randomInt() {
        Random rnd = new Random();
        int x;
        // Cannot return zero, since it breaks Xorshift RNG.
        while ((x = rnd.nextInt()) == 0);
        return x;
    }

    private final Crypto mCrypto;
    private final File mBaseFile;

    private final boolean mReplayMode;

    private long mLogId;
    private OutputStream mOut;
    private volatile FileChannel mChannel;

    private int mTermRndSeed;

    private NextFile mCheckpointState;

    /**
     * @param logId first log id to open
     */
    RedoLog(DatabaseConfig config, long logId, boolean replay) throws IOException {
        this(config.mCrypto, config.mBaseFile, logId, replay);
    }

    /**
     * @param logId first log id to open
     */
    RedoLog(Crypto crypto, File baseFile, long logId, boolean replay) throws IOException {
        super(4096, 0);

        mCrypto = crypto;
        mBaseFile = baseFile;
        mReplayMode = replay;

        synchronized (this) {
            mLogId = logId;
            if (!replay) {
                applyNextFile(openNextFile(logId));
            }
        }
    }

    /**
     * @return all the files which were replayed
     */
    synchronized Set<File> replay(RedoVisitor visitor,
                                  EventListener listener, EventType type, String message)
        throws IOException
    {
        if (!mReplayMode || mBaseFile == null) {
            throw new IllegalStateException();
        }

        try {
            Set<File> files = new LinkedHashSet<File>(2);

            while (true) {
                File file = fileFor(mBaseFile, mLogId);

                InputStream in;
                try {
                    in = new FileInputStream(file);
                } catch (FileNotFoundException e) {
                    break;
                }

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

                    replay(new DataIn(in), visitor, listener);
                } catch (EOFException e) {
                    // End of log didn't get completely flushed.
                } finally {
                    Utils.closeQuietly(null, in);
                }

                mLogId++;
            }

            return files;
        } catch (IOException e) {
            throw Utils.rethrow(e, mCause);
        }
    }

    synchronized long currentLogId() {
        return mLogId;
    }

    static void deleteOldFile(File baseFile, long logId) {
        fileFor(baseFile, logId).delete();
    }

    static class NextFile {
        final long logId;
        final OutputStream out;
        final FileChannel channel;
        final int termRndSeed;

        NextFile(long logId, File file, Crypto crypto) throws IOException {
            this.logId = logId;

            FileOutputStream fout = new FileOutputStream(file);
            channel = fout.getChannel();
            if (crypto == null) {
                out = fout;
            } else {
                try {
                    out = crypto.newEncryptingStream(logId, fout);
                } catch (GeneralSecurityException e) {
                    throw new DatabaseException(e);
                }
            }

            termRndSeed = randomInt();

            byte[] buf = new byte[8 + 4 + 8 + 4];
            int offset = 0;
            Utils.writeLongLE(buf, offset, MAGIC_NUMBER); offset += 8;
            Utils.writeIntLE(buf, offset, ENCODING_VERSION); offset += 4;
            Utils.writeLongLE(buf, offset, logId); offset += 8;
            Utils.writeIntLE(buf, offset, termRndSeed); offset += 4;
            if (offset != buf.length) {
                throw new AssertionError();
            }

            try {
                out.write(buf);
            } catch (IOException e) {
                Utils.closeQuietly(null, out);
                file.delete();
                throw e;
            }
        }
    }

    private NextFile openNextFile(long logId) throws IOException {
        final File file = fileFor(mBaseFile, logId);
        if (file.exists()) {
            throw new FileNotFoundException("Log file already exists: " + file.getPath());
        }
        return new NextFile(logId, file, mCrypto);
    }

    private void applyNextFile(NextFile nf) throws IOException {
        final OutputStream oldOut;
        final FileChannel oldChannel;
        synchronized (this) {
            oldOut = mOut;
            oldChannel = mChannel;

            if (oldOut != null) {
                endFile();
            }

            mOut = nf.out;
            mChannel = nf.channel;
            mTermRndSeed = nf.termRndSeed;
            mLogId = nf.logId;

            timestamp();
            reset();
        }

        if (oldChannel != null) {
            // Make sure any exception thrown by this call is not caught here,
            // because a checkpoint cannot complete successfully if the redo
            // log has not been durably written.
            oldChannel.force(true);
        }

        Utils.closeQuietly(null, oldOut);
    }

    /**
     * @return null if non-durable
     */
    private static File fileFor(File base, long logId) {
        return base == null ? null : new File(base.getPath() + ".redo." + logId);
    }

    @Override
    boolean isOpen() {
        FileChannel channel = mChannel;
        return channel != null && channel.isOpen();
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
        final long logId;
        synchronized (this) {
            logId = mLogId;
        }
        mCheckpointState = openNextFile(logId + 1);
    }

    @Override
    void checkpointSwitch() throws IOException {
        applyNextFile(mCheckpointState);
    }

    @Override
    long checkpointPosition() {
        return mCheckpointState.logId;
    }

    @Override
    long checkpointTransactionId() {
        // Log file always begins with a reset.
        return 0;
    }

    @Override
    void checkpointed() throws IOException {
        deleteOldFile(mBaseFile, mCheckpointState.logId - 1);
        mCheckpointState = null;
    }

    @Override
    void write(byte[] buffer, int len) throws IOException {
        mOut.write(buffer, 0, len);
    }

    @Override
    void force(boolean metadata) throws IOException {
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
    void forceAndClose() throws IOException {
        FileChannel channel = mChannel;
        if (channel != null) {
            try {
                channel.force(true);
                try {
                    channel.close();
                } catch (IOException e) {
                    // Ignore.
                }
            } catch (ClosedChannelException e) {
                // Ignore.
            }
        }
    }

    @Override
    void writeTerminator() throws IOException {
        writeIntLE(nextTermRnd());
    }

    // Caller must be synchronized (replay is exempt)
    int nextTermRnd() throws IOException {
        // Xorshift RNG by George Marsaglia.
        int x = mTermRndSeed;
        x ^= x << 13;
        x ^= x >>> 17;
        x ^= x << 5;
        mTermRndSeed = x;
        return x;
    }

    private void replay(DataIn in, RedoVisitor visitor, final EventListener listener)
        throws IOException
    {
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

        RedoDecoder decoder = new RedoDecoder(in, true, 0) {
            @Override
            protected boolean verifyTerminator(DataIn in) throws IOException {
                try {
                    if (in.readIntLE() == nextTermRnd()) {
                        return true;
                    }
                    if (listener != null) {
                        listener.notify
                            (EventType.RECOVERY_REDO_LOG_CORRUPTION, "Invalid message terminator");
                    }
                    return false;
                } catch (EOFException e) {
                    listener.notify
                        (EventType.RECOVERY_REDO_LOG_CORRUPTION, "Unexpected end of file");
                    return false;
                }
            }
        };

        try {
            decoder.run(visitor);
        } catch (EOFException e) {
            listener.notify(EventType.RECOVERY_REDO_LOG_CORRUPTION, "Unexpected end of file");
        }
    }
}
