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

import java.io.IOException;

import java.util.concurrent.locks.LockSupport;

import org.cojen.tupl.ext.ReplicationManager;

import org.cojen.tupl.util.Latch;

/**
 * 
 *
 * @author Brian S O'Neill
 */
/*P*/
class ReplRedoWriter extends RedoWriter {
    final ReplRedoEngine mEngine;

    // Is non-null if writes are allowed.
    final ReplicationManager.Writer mReplWriter;

    // These fields capture the state of the last produced commit, but not yet confirmed.
    // Access is guarded by RedoWriter latch and mBufferLatch. Both latches must be held to
    // modify these fields, and so either latch must be held for reading the fields.
    long mLastCommitPos;
    long mLastCommitTxnId;

    private volatile PendingTxnWaiter mPendingWaiter;

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

    /**
     * Caller must call start if a writer is supplied.
     */
    ReplRedoWriter(ReplRedoEngine engine, ReplicationManager.Writer writer) {
        mEngine = engine;
        mReplWriter = writer;
        mBufferLatch = writer == null ? null : new Latch();
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
    public final void commitSync(TransactionContext context, long commitPos) throws IOException {
        ReplicationManager.Writer writer = mReplWriter;
        if (writer == null) {
            throw mEngine.unmodifiable();
        }
        if (writer.confirm(commitPos)) {
            context.confirmed(commitPos);
        } else {
            throw nowUnmodifiable();
        }
    }

    @Override
    public final void txnCommitSync(LocalTransaction txn, long commitPos) throws IOException {
        ReplicationManager.Writer writer = mReplWriter;
        if (writer == null) {
            throw mEngine.unmodifiable();
        }
        if (writer.confirm(commitPos)) {
            txn.mContext.confirmed(commitPos, txn.txnId());
        } else {
            throw nowUnmodifiable();
        }
    }

    @Override
    public final void txnCommitPending(PendingTxn pending) throws IOException {
        PendingTxnWaiter waiter = mPendingWaiter;
        int action;
        if (waiter == null || (action = waiter.add(pending)) == PendingTxnWaiter.EXITED) {
            acquireExclusive();
            try {
                waiter = mPendingWaiter;
                if (waiter == null || (action = waiter.add(pending)) == PendingTxnWaiter.EXITED) {
                    waiter = new PendingTxnWaiter(this);
                    mPendingWaiter = waiter;
                    action = waiter.add(pending);
                    if (action == PendingTxnWaiter.PENDING) {
                        waiter.setName("PendingTxnWaiter-" + waiter.getId());
                        waiter.setDaemon(true);
                        waiter.start();
                    }
                }
            } finally {
                releaseExclusive();
            }
        }

        if (action != PendingTxnWaiter.PENDING) {
            LocalDatabase db = mEngine.mDatabase;
            if (action == PendingTxnWaiter.DO_COMMIT) {
                pending.commit(db);
            } else if (action == PendingTxnWaiter.DO_ROLLBACK) {
                pending.rollback(db);
            }
        }
    }

    protected final void flipped(long commitPos) {
        closeConsumerThread();

        PendingTxnWaiter waiter;
        acquireExclusive();
        try {
            waiter = mPendingWaiter;
            if (waiter == null) {
                waiter = new PendingTxnWaiter(this);
                mPendingWaiter = waiter;
                // Don't start it.
            }
            waiter.flipped(commitPos);
        } finally {
            releaseExclusive();
        }

        waiter.finishAll();
    }

    /**
     * Block waiting for the given committed position to be confirmed. Returns false if not the
     * leader.
     */
    final boolean confirm(PendingTxn pending) {
        // Note: Similar to txnCommitSync.

        ReplicationManager.Writer writer = mReplWriter;
        if (writer == null) {
            return false;
        }

        long commitPos = pending.mCommitPos;

        try {
            if (writer.confirm(commitPos)) {
                pending.mContext.confirmed(commitPos, pending.mTxnId);
                return true;
            }
        } catch (IOException e) {
            // Treat as leader switch.
        }

        mEngine.mController.switchToReplica(mReplWriter);

        return false;
    }

    @Override
    public final long encoding() {
        return mEngine.mManager.encoding();
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
    final long write(boolean commit, byte[] bytes, int offset, int length) throws IOException {
        if (mReplWriter == null) {
            throw mEngine.unmodifiable();
        }

        mBufferLatch.acquireExclusive();
        try {
            byte[] buffer = mBuffer;
            if (buffer == null) {
                throw nowUnmodifiable();
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
                                LockSupport.unpark(consumer);
                            }
                            LockSupport.park(mBufferLatch);
                            mBufferLatch.acquireExclusive();
                            buffer = mBuffer;
                            if (buffer == null) {
                                throw nowUnmodifiable();
                            }
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

                    long pos = mWritePos += length;

                    if ((mBufferTail += length) >= buffer.length) {
                        mBufferTail = 0;
                    }

                    if (commit) {
                        mLastCommitPos = pos;
                        mLastCommitTxnId = mLastTxnId;
                    }

                    // FIXME: If consumer is parked, attempt to do the write immediately.
                    // Still do the arraycopy, to support auto-tuning. Release the latch and
                    // then do the write. This creates a race condition with the consumer
                    // thread, and so somethig extra is needed.
                    if (mConsumerParked) {
                        mConsumerParked = false;
                        LockSupport.unpark(mConsumer);
                    }
                    return pos;
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
        mEngine.mManager.sync();
    }

    @Override
    public void close() throws IOException {
        mEngine.mManager.close();

        if (mBufferLatch == null) {
            return;
        }

        closeConsumerThread();
    }

    private void closeConsumerThread() {
        mBufferLatch.acquireExclusive();
        Thread consumer = mConsumer;
        mConsumer = null;
        mConsumerParked = false;
        mBufferLatch.releaseExclusive();

        if (consumer != null) {
            LockSupport.unpark(consumer);
            try {
                consumer.join();
            } catch (InterruptedException e) {
                // Ignore.
            }
        }
    }

    private UnsupportedOperationException fail() {
        // ReplRedoController subclass supports checkpoint operations.
        return new UnsupportedOperationException();
    }

    private UnmodifiableReplicaException nowUnmodifiable() throws DatabaseException {
        return mEngine.mController.nowUnmodifiable(mReplWriter);
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
            long commitPos = mLastCommitPos;

            try {
                if (head == tail) {
                    // Buffer is full, so consume everything with the latch held.

                    // Write the head section.
                    if (!mReplWriter.write(buffer, head, buffer.length - head, commitPos)) {
                        break;
                    }

                    if (head > 0) {
                        // Write the tail section.
                        mBufferHead = 0;
                        if (!mReplWriter.write(buffer, 0, tail, commitPos)) {
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
                            if (!mReplWriter.write(buffer, head, tail - head, commitPos)) {
                                break;
                            }
                            head = tail;
                        } else {
                            // Write only the head section.
                            int len = buffer.length - head;
                            if (!mReplWriter.write(buffer, head, len, commitPos)) {
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
                if (!(e instanceof IOException)) {
                    Utils.uncaught(e);
                }
                // Keep consuming until an official leadership change is observed.
                mBufferLatch.releaseExclusive();
                Thread.yield();
                mBufferLatch.acquireExclusive();
                continue;
            }

            // Wait for producer and loop back.
            mConsumerParked = true;
            Thread producer = mProducer;
            mBufferLatch.releaseExclusive();
            LockSupport.unpark(producer);
            LockSupport.park(mBufferLatch);
            mBufferLatch.acquireExclusive();
        }

        mConsumer = null;
        mBuffer = null;
        LockSupport.unpark(mProducer);
        mBufferLatch.releaseExclusive();

        mEngine.mController.switchToReplica(mReplWriter);
    }
}
