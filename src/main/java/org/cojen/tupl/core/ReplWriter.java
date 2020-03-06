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

import java.io.InterruptedIOException;
import java.io.IOException;

import org.cojen.tupl.ConfirmationFailureException;
import org.cojen.tupl.ConfirmationInterruptedException;
import org.cojen.tupl.ConfirmationTimeoutException;
import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.UnmodifiableReplicaException;
import org.cojen.tupl.WriteFailureException;

import org.cojen.tupl.repl.StreamReplicator;

import org.cojen.tupl.util.Latch;
import org.cojen.tupl.util.Parker;


/**
 * Implementation of a replicated redo log, used by {@link ReplController}.
 *
 * @author Brian S O'Neill
 */
/*P*/
class ReplWriter extends RedoWriter {
    final ReplEngine mEngine;

    // Is non-null if writes are allowed.
    final StreamReplicator.Writer mReplWriter;

    // These fields capture the state of the last produced commit, but not yet confirmed.
    // Access is guarded by RedoWriter latch and mBufferLatch. Both latches must be held to
    // modify these fields, and so either latch must be held for reading the fields.
    long mLastCommitPos;
    long mLastCommitTxnId;

    // These fields are guarded by mBufferLatch.
    private final Latch mBufferLatch;
    private Thread mProducer;
    private Thread mConsumer;
    private boolean mConsumerParked;
    // Circular buffer; empty when mBufferTail < 0, full when mBufferHead == mBufferTail.
    private byte[] mBuffer;
    // If mBufferTail is in the range of [0, buffer.length] then this is the first used byte in
    // the buffer. If mBufferTail is negative, there is no used byte in the buffer.
    private int mBufferHead;
    // In the range of [0, buffer.length), mBufferTail represents the first free byte in the
    // buffer, unless it equals mBufferHead (in which case there are no free bytes in the
    // buffer). It can also be negative, which means the buffer is empty.
    private int mBufferTail = -1;
    // Absolute log position.
    private long mWritePos;
    // Set if the consumer failed to write to the ReplManager.
    private Throwable mConsumerException;

    volatile boolean mUnmodifiable;

    // Last ones added with mBufferLatch held, but first ones are removed without any latch.
    // Assumes that the commit stream from Replicator holds a latch of its own.
    private volatile PendingTxn mFirstPending;
    private volatile PendingTxn mLastPending;

    /**
     * Caller must call start if a writer is supplied.
     */
    ReplWriter(ReplEngine engine, StreamReplicator.Writer writer) {
        mEngine = engine;
        mReplWriter = writer;
        if (writer == null) {
            mBufferLatch = null;
        } else {
            mBufferLatch = new Latch();
            writer.addCommitListener(this::finishPending);
        }
    }

    void start() {
        mBufferLatch.acquireExclusive();
        try {
            if (mEngine.mDatabase.isClosed()) {
                return;
            }

            mWritePos = mReplWriter.position();
            mBuffer = new byte[65536];

            mConsumer = new Thread(this::consume);
            mConsumer.setName("WriteConsumer-" + mConsumer.getId());
            mConsumer.setDaemon(true);
            mConsumer.start();
        } finally {
            mBufferLatch.releaseExclusive();
        }
    }

    @Override
    final boolean failover() throws IOException {
        return mEngine.mRepl.failover();
    }

    @Override
    public final void txnCommitSync(long commitPos) throws IOException {
        StreamReplicator.Writer writer = mReplWriter;
        if (writer == null) {
            throw mEngine.unmodifiable();
        }
        if (!confirm(writer, commitPos)) {
            throw nowUnmodifiable();
        }
    }

    @Override
    public final long encoding() {
        return mEngine.mRepl.encoding();
    }

    @Override
    public RedoWriter txnRedoWriter() {
        return this;
    }

    @Override
    boolean shouldCheckpoint(long sizeThreshold) {
        return false;
    }

    @Override
    void checkpointPrepare() throws IOException {
        throw fail();
    }

    @Override
    void checkpointSwitch(TransactionContext[] contexts) throws IOException {
        throw fail();
    }

    @Override
    long checkpointNumber() {
        throw fail();
    }

    @Override
    long checkpointPosition() {
        throw fail();
    }

    @Override
    long checkpointTransactionId() {
        throw fail();
    }

    @Override
    void checkpointAborted() {
    }

    @Override
    void checkpointStarted() throws IOException {
        throw fail();
    }

    @Override
    void checkpointFlushed() throws IOException {
        throw fail();
    }

    @Override
    void checkpointFinished() throws IOException {
        throw fail();
    }

    @Override
    DurabilityMode opWriteCheck(DurabilityMode mode) throws IOException {
        // All redo methods which accept a DurabilityMode must always use SYNC mode. This
        // ensures that write commit option is true, for capturing the log position. If
        // Transaction.commit sees that DurabilityMode wasn't actually SYNC, it prepares a
        // PendingTxn instead of immediately calling txnCommitSync. Replication makes no
        // distinction between NO_FLUSH and NO_SYNC mode.
        return DurabilityMode.SYNC;
    }

    @Override
    boolean shouldWriteTerminators() {
        return false;
    }

    @Override
    final long write(boolean flush, byte[] bytes, int offset, int length, int commitLen,
                     PendingTxn pending)
        throws IOException
    {
        if (mReplWriter == null) {
            throw mEngine.unmodifiable();
        }

        mBufferLatch.acquireExclusive();
        try {
            byte[] buffer = mBuffer;
            if (buffer == null) {
                throw nowUnmodifiable();
            }

            if (commitLen > 0) {
                // Store the last commit info early, before the position is adjusted when
                // looping over large messages. There's no harm in doing this early, even if an
                // exception is thrown due to replica mode switchover. The commit position must
                // always be confirmed later.
                mLastCommitPos = mWritePos + commitLen;
                mLastCommitTxnId = mLastTxnId;

                if (pending != null) {
                    // Set position before writing to volatile fields.
                    pending.mCommitPos = mLastCommitPos;
                    PendingTxn last = mLastPending;
                    if (last == null) {
                        mFirstPending = pending;
                    } else {
                        last.setNext(pending);
                        if (mFirstPending == null) {
                            mFirstPending = pending;
                        }
                    }
                    mLastPending = pending;
                }
            }

            while (true) {
                if (mBufferHead == mBufferTail) {
                    mProducer = Thread.currentThread();
                    try {
                        Thread consumer = mConsumer;
                        do {
                            boolean parked = mConsumerParked;
                            if (parked) {
                                mConsumerParked = false;
                            }
                            mBufferLatch.releaseExclusive();
                            if (parked) {
                                Parker.unpark(consumer);
                            }
                            Parker.park(mBufferLatch);
                            mBufferLatch.acquireExclusive();
                            buffer = mBuffer;
                            if (buffer == null) {
                                throw nowUnmodifiable();
                            }
                            checkConsumerException();
                        } while (mBufferHead == mBufferTail);
                    } finally {
                        mProducer = null;
                    }
                }

                int amt;
                //assert mBufferHead != mBufferTail;
                if (mBufferHead < mBufferTail) {
                    // Allow filling up to the end of the buffer without wrapping around. The
                    // next iteration of this loop will wrap around in the buffer if necessary.
                    amt = buffer.length - mBufferTail;
                } else if (mBufferTail >= 0) {
                    // The tail has wrapped around, but the head has not. Allow filling up to
                    // the head.
                    amt = mBufferHead - mBufferTail;
                } else {
                    // The buffer is empty, so allow filling the whole thing. Note that this is
                    // an intermediate state, which implies that the buffer is full. After the
                    // arraycopy, the tail is set correctly.
                    if (length != 0) {
                        mBufferHead = 0;
                        mBufferTail = 0;
                    }
                    amt = buffer.length;
                }

                if (length <= amt) {
                    try {
                        System.arraycopy(bytes, offset, buffer, mBufferTail, length);
                    } catch (Throwable e) {
                        // Fix any intermediate state.
                        if (mBufferHead == mBufferTail) {
                            mBufferTail = -1;
                        }
                        throw e;
                    }

                    mWritePos += length;

                    if ((mBufferTail += length) >= buffer.length) {
                        mBufferTail = 0;
                    }

                    if (mConsumerParked) {
                        mConsumerParked = false;
                        Parker.unpark(mConsumer);
                    }

                    return mWritePos;
                }

                try {
                    System.arraycopy(bytes, offset, buffer, mBufferTail, amt);
                } catch (Throwable e) {
                    // Fix any intermediate state.
                    if (mBufferHead == mBufferTail) {
                        mBufferTail = -1;
                    }
                    throw e;
                }

                mWritePos += amt;
                length -= amt;
                offset += amt;

                if ((mBufferTail += amt) >= buffer.length) {
                    mBufferTail = 0;
                }
            }
        } finally {
            mBufferLatch.releaseExclusive();
        }
    }

    @Override
    final void alwaysFlush(boolean enable) {
        // Always flushes already.
    }

    @Override
    public final void flush() {
        // Nothing to flush.
    }

    @Override
    void force(boolean metadata) throws IOException {
        // Wait for consumer to finish writing to the ReplManager.
        acquireExclusive();
        mBufferLatch.acquireExclusive();
        try {
            if (mBufferTail >= 0 && mBuffer != null) {
                while (true) {
                    mProducer = Thread.currentThread();
                    try {
                        mBufferLatch.releaseExclusive();
                        Parker.park(mBufferLatch);
                        mBufferLatch.acquireExclusive();
                    } finally {
                        mProducer = null;
                    }
                    if (mBufferTail < 0 || mBuffer == null) {
                        break;
                    }
                    checkConsumerException();
                }
            }
        } finally {
            mBufferLatch.releaseExclusive();
            releaseExclusive();
        }

        mEngine.mRepl.sync();
    }

    @Override
    public void close() throws IOException {
        mEngine.mRepl.close();

        if (mBufferLatch == null) {
            return;
        }

        closeConsumerThread();
    }

    @Override
    void stashForRecovery(LocalTransaction txn) {
        mEngine.stashForRecovery(txn);
    }

    private void finishPending(long commitPos) {
        PendingTxn pending = mFirstPending;
        if (pending == null) {
            return;
        }

        final PendingTxn first = pending;

        // Read volatile field first to get the correct position.
        PendingTxn next = pending.mNext;

        if (commitPos < 0) {
            pending.mCommitPos = -1; // signal rollback
        } else if (commitPos < pending.mCommitPos) {
            return;
        }

        int count = 1;

        while (true) {
            if (next == null) {
                // Removing the last node requires special attention.
                next = pending.tryLockLast();
                mFirstPending = next;
                if (next == null) {
                    mLastPending = null;
                    pending.unlockLast();
                    break;
                }
            }

            PendingTxn prev = pending;
            pending = next;
            next = pending.mNext;

            if (commitPos < 0) {
                pending.mCommitPos = -1; // signal rollback
            } else if (commitPos < pending.mCommitPos) {
                mFirstPending = pending;
                pending = prev;
                break;
            }

            count++;
        }

        pending.mNext = null;

        mEngine.mFinisher.enqueue(count, first, pending);
    }

    void closeConsumerThread() {
        mBufferLatch.acquireExclusive();
        Thread consumer = mConsumer;
        mConsumer = null;
        mConsumerParked = false;
        mBufferLatch.releaseExclusive();

        if (consumer != null) {
            Parker.unpark(consumer);
            try {
                consumer.join();
            } catch (InterruptedException e) {
                // Ignore.
            }
        }
    }

    private void checkConsumerException() throws WriteFailureException {
        if (mConsumerException != null) {
            var e = new WriteFailureException(mConsumerException);
            mConsumerException = null;
            throw e;
        }
    }

    private UnsupportedOperationException fail() {
        // ReplController subclass supports checkpoint operations.
        return new UnsupportedOperationException();
    }

    private UnmodifiableReplicaException nowUnmodifiable() throws DatabaseException {
        mUnmodifiable = true;
        return mEngine.mController.nowUnmodifiable(mReplWriter);
    }

    /**
     * Blocks until all data up to the given log position is confirmed.
     *
     * @param commitPos commit position which was passed to the write method
     * @return false if not leader at the given position
     */
    static boolean confirm(StreamReplicator.Writer writer, long commitPos)
        throws ConfirmationFailureException
    {
        long pos;
        try {
            pos = writer.waitForCommit(commitPos, -1);
        } catch (InterruptedIOException e) {
            throw new ConfirmationInterruptedException();
        }
        if (pos >= commitPos) {
            return true;
        }
        if (pos == -1) {
            return false;
        }
        throw unexpectedConfirmation(pos);
    }

    /**
     * Blocks until the leadership end is confirmed. This method must be called before
     * switching to replica mode.
     *
     * @return the end commit position; same as next read position
     */
    static long confirmEnd(StreamReplicator.Writer writer) throws ConfirmationFailureException {
        long pos;
        try {
            pos = writer.waitForEndCommit(-1);
        } catch (InterruptedIOException e) {
            throw new ConfirmationInterruptedException();
        }
        if (pos >= 0) {
            return pos;
        }
        if (pos == -1) {
            throw new ConfirmationFailureException("Closed");
        }
        throw unexpectedConfirmation(pos);
    }

    static ConfirmationFailureException unexpectedConfirmation(long pos) {
        if (pos == -2) {
            return new ConfirmationTimeoutException(-1);
        }
        return new ConfirmationFailureException("Unexpected result: " + pos);
    }

    /**
     * Consumes data from the circular buffer and writes into the replication log. Method doesn't
     * exit until leadership is revoked.
     */
    private void consume() {
        mBufferLatch.acquireExclusive();

        final byte[] buffer = mBuffer;

        while (mConsumer != null) {
            int head = mBufferHead;
            int tail = mBufferTail;

            try {
                if (head == tail) {
                    // Buffer is full, so consume everything with the latch held.

                    // Write the head section.
                    if (replWrite(buffer, head, buffer.length - head) <= 0) {
                        break;
                    }

                    if (head > 0) {
                        // Write the tail section.
                        mBufferHead = 0;
                        if (replWrite(buffer, 0, tail) <= 0) {
                            break;
                        }
                    }

                    // Buffer is now empty.
                    mBufferTail = -1;
                } else if (tail >= 0) {
                    // Buffer is partially full. Consume it with the latch released, to
                    // allow a producer to fill in a bit more.
                    mBufferLatch.releaseExclusive();
                    try {
                        if (head < tail) {
                            // No circular wraparound.
                            if (replWrite(buffer, head, tail - head) <= 0) {
                                break;
                            }
                            head = tail;
                        } else {
                            // Write only the head section.
                            int len = buffer.length - head;
                            if (replWrite(buffer, head, len) <= 0) {
                                break;
                            }
                            head = 0;
                        }
                    } finally {
                        mBufferLatch.acquireExclusive();
                    }

                    if (head != mBufferTail) {
                        // More data to consume.
                        mBufferHead = head;
                        continue;
                    }

                    // Buffer is now empty.
                    mBufferTail = -1;
                }
            } catch (Throwable e) {
                if (mConsumerException == null) {
                    mConsumerException = e;
                }
                // Keep consuming until an official leadership change is observed.
                if (mProducer == null) {
                    mBufferLatch.releaseExclusive();
                    Thread.yield();
                    mBufferLatch.acquireExclusive();
                    continue;
                }
            }

            // Wait for producer and loop back.
            mConsumerParked = true;
            Thread producer = mProducer;
            mBufferLatch.releaseExclusive();
            Parker.unpark(producer);
            Parker.park(mBufferLatch);
            mBufferLatch.acquireExclusive();
        }

        if (mConsumerException != null) {
            if (!(mConsumerException instanceof IOException)) {
                Utils.uncaught(mConsumerException);
            }
            mConsumerException = null;
        }

        mConsumer = null;
        mBuffer = null;
        Parker.unpark(mProducer);
        mBufferLatch.releaseExclusive();

        mEngine.mController.switchToReplica(mReplWriter);
    }

    /**
     * @return 1 if successful, -1 if fully deactivated, or 0 if should flush the buffer
     */
    private int replWrite(byte[] buf, int off, int len) throws IOException {
        return mReplWriter.write(buf, off, len, mLastCommitPos);
    }
}
