/*
 *  Copyright (C) 2021 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.table;

import java.io.DataOutput;
import java.io.IOException;

import java.lang.ref.Cleaner;
import java.lang.ref.Reference;
import java.lang.ref.SoftReference;

import java.util.concurrent.TimeUnit;

import java.util.random.RandomGenerator;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Index;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.core.RowPredicateLock;

import org.cojen.tupl.util.LocalPool;

/**
 * Stores entries into an index using automatically generated keys.
 *
 * @author Brian S O'Neill
 */
public abstract class AutomaticKeyGenerator<R> {
    private final Index mIndex;
    private final LocalPool<SoftReference<KeyState>> mStatePool;

    AutomaticKeyGenerator(Index index) {
        mIndex = index;
        mStatePool = new LocalPool<>(null);
    }

    public static class OfInt<R> extends AutomaticKeyGenerator<R> {
        private final int mMin, mMax;
        private final Applier<R> mApplier;

        /**
         * @param min inclusive
         * @param max inclusive
         */
        public OfInt(Index index, int min, int max, Applier<R> applier) {
            super(index);
            mMin = min;
            mMax = max;
            mApplier = applier;
        }

        @Override
        public void writeTail(byte[] key, DataOutput out) throws IOException {
            out.writeInt(decode(key));
        }

        @Override
        protected void randomKey(byte[] key, RandomGenerator rnd) {
            int colValue;
            do {
                if (mMax != Integer.MAX_VALUE) {
                    colValue = rnd.nextInt(mMin, mMax + 1);
                } else if (mMin != Integer.MIN_VALUE) {
                    colValue = rnd.nextInt(mMin - 1, mMax) + 1;
                } else {
                    colValue = rnd.nextInt();
                }
            } while (colValue == 0);

            encode(key, colValue);
        }

        @Override
        protected RowPredicateLock.Closer incrementKey(Transaction txn, R row,
                                                       byte[] srcKey, byte[] dstKey, byte[] value)
            throws IOException
        {
            int colValue = decode(srcKey) + 1;
            if (colValue == 0) {
                colValue = 1;
            }
            if (colValue == Integer.MIN_VALUE || colValue > mMax) {
                colValue = mMin;
            }

            encode(dstKey, colValue);

            var applier = mApplier;

            if (applier == null) {
                return RowPredicateLock.NonCloser.THE;
            } else if (row != null) {
                return applier.applyToRow(txn, row, colValue);
            } else {
                return applier.tryOpenAcquire(txn, dstKey, value);
            }
        }

        protected int decode(byte[] key) {
            return RowUtils.decodeIntBE(key, key.length - 4) ^ (1 << 31);
        }

        protected void encode(byte[] key, int colValue) {
            RowUtils.encodeIntBE(key, key.length - 4, colValue ^ (1 << 31));
        }

        public static interface Applier<R> {
            /**
             * Store the given column value into the row and then call tryOpenAcquire.
             *
             * @see RowPredicateLock#tryOpenAcquire
             */
            public RowPredicateLock.Closer applyToRow(Transaction txn, R row, int colValue)
                throws IOException;

            /**
             * Is called when no row object was provided, and so the predicate lock must be
             * acquired by examining the encoded key and value.
             *
             * @see RowPredicateLock#tryOpenAcquire
             */
            public RowPredicateLock.Closer tryOpenAcquire(Transaction txn, byte[] key, byte[] value)
                throws IOException;
        }
    }

    public static class OfUInt<R> extends OfInt<R> {
        /**
         * @param min inclusive
         * @param max inclusive
         */
        public OfUInt(Index index, int min, int max, Applier<R> applier) {
            super(index, min, max, applier);
        }

        protected int decode(byte[] key) {
            return RowUtils.decodeIntBE(key, key.length - 4);
        }

        protected void encode(byte[] key, int colValue) {
            RowUtils.encodeIntBE(key, key.length - 4, colValue);
        }
    }

    public static class OfLong<R> extends AutomaticKeyGenerator<R> {
        private final long mMin, mMax;
        private final Applier<R> mApplier;

        /**
         * @param min inclusive
         * @param max inclusive
         */
        public OfLong(Index index, long min, long max, Applier<R> applier) {
            super(index);
            mMin = min;
            mMax = max;
            mApplier = applier;
        }

        @Override
        public void writeTail(byte[] key, DataOutput out) throws IOException {
            out.writeLong(decode(key));
        }

        @Override
        protected void randomKey(byte[] key, RandomGenerator rnd) {
            long colValue;
            do {
                if (mMax != Long.MAX_VALUE) {
                    colValue = rnd.nextLong(mMin, mMax + 1);
                } else if (mMin != Long.MIN_VALUE) {
                    colValue = rnd.nextLong(mMin - 1, mMax) + 1;
                } else {
                    colValue = rnd.nextLong();
                }
            } while (colValue == 0);

            encode(key, colValue);
        }

        @Override
        protected RowPredicateLock.Closer incrementKey(Transaction txn, R row,
                                                       byte[] srcKey, byte[] dstKey, byte[] value)
            throws IOException
        {
            long colValue = decode(srcKey) + 1;
            if (colValue == 0) {
                colValue = 1;
            }
            if (colValue == Long.MIN_VALUE || colValue > mMax) {
                colValue = mMin;
            }

            encode(dstKey, colValue);

            var applier = mApplier;

            if (applier == null) {
                return RowPredicateLock.NonCloser.THE;
            } else if (row != null) {
                return applier.applyToRow(txn, row, colValue);
            } else {
                return applier.tryOpenAcquire(txn, dstKey, value);
            }
        }

        protected long decode(byte[] key) {
            return RowUtils.decodeLongBE(key, key.length - 8) ^ (1L << 63);
        }

        protected void encode(byte[] key, long colValue) {
            RowUtils.encodeLongBE(key, key.length - 8, colValue ^ (1L << 63));
        }

        public static interface Applier<R> {
            /**
             * Store the given column value into the row and then call tryOpenAcquire.
             *
             * @see RowPredicateLock#tryOpenAcquire
             */
            public RowPredicateLock.Closer applyToRow(Transaction txn, R row, long colValue)
                throws IOException;

            /**
             * Is called when no row object was provided, and so the predicate lock must be
             * acquired by examining the encoded key and value.
             *
             * @see RowPredicateLock#tryOpenAcquire
             */
            public RowPredicateLock.Closer tryOpenAcquire(Transaction txn, byte[] key, byte[] value)
                throws IOException;
        }
    }

    public static class OfULong<R> extends OfLong<R> {
        /**
         * @param min inclusive
         * @param max inclusive
         */
        public OfULong(Index index, long min, long max, Applier<R> applier) {
            super(index, min, max, applier);
        }

        protected long decode(byte[] key) {
            return RowUtils.decodeLongBE(key, key.length - 8);
        }

        protected void encode(byte[] key, long colValue) {
            RowUtils.encodeLongBE(key, key.length - 8, colValue);
        }
    }

    /**
     * Stores an entry against the given non-null transaction.
     *
     * @param row is passed to the Applier; can be null
     * @param key partially specified key; the tail is updated with the generated portion
     * @return actual key that was stored
     */
    public byte[] store(Transaction txn, R row, byte[] key, byte[] value)
        throws IOException
    {
        LocalPool.Entry<SoftReference<KeyState>> entry = mStatePool.access();
        try {
            SoftReference<KeyState> stateRef = entry.get();
            KeyState state;
            Cursor c;
            if (stateRef != null && (state = stateRef.get()) != null) {
                c = state.mCursor;
            } else {
                c = mIndex.newCursor(Transaction.BOGUS);
                c.autoload(false);
                var rnd = RandomGenerator.of("L64X128MixRandom");
                randomPosition(key, rnd, c);
                state = register(c, rnd);
                entry.replace(new SoftReference<>(state));
            }

            try {
                long endNanos = 0;
                byte[] srcKey = c.key();

                while (true) {
                    RowPredicateLock.Closer closer = incrementKey(txn, row, srcKey, key, value);

                    if (closer != null) {
                        LockResult result;
                        try {
                            result = mIndex.tryLockUpgradable(txn, key, 0);
                        } catch (Throwable e) {
                            if (!(closer instanceof RowPredicateLock.NonCloser)) {
                                txn.unlock();
                                closer.close();
                            }
                            throw e;
                        }

                        if (result != LockResult.ACQUIRED) {
                            if (!(closer instanceof RowPredicateLock.NonCloser)) {
                                txn.unlock();
                                closer.close();
                            }
                        } else {
                            if (!(closer instanceof RowPredicateLock.NonCloser)) {
                                txn.unlockCombine();
                            }

                            try {
                                c.findNearby(key);
                            } catch (Throwable e) {
                                txn.unlock();
                                closer.close();
                                throw e;
                            }

                            if (c.value() == null) {
                                c.link(txn);
                                try {
                                    c.store(value);
                                } finally {
                                    closer.close();
                                    c.link(Transaction.BOGUS);
                                }
                                return key;
                            }

                            txn.unlock();
                            closer.close();

                            // Even though the key is unlocked, the byte array cannot be
                            // modified again because the LockManager now owns it.
                            key = key.clone();
                        }
                    }

                    srcKey = key;

                    if (endNanos == 0) {
                        long nanosTimeout = txn.lockTimeout(TimeUnit.NANOSECONDS);
                        if (nanosTimeout >= 0) {
                            if (nanosTimeout == 0) {
                                throw failed(txn);
                            }
                            endNanos = System.nanoTime() + nanosTimeout;
                            if (endNanos == 0) {
                                endNanos++;
                            }
                        }
                    } else if ((endNanos - System.nanoTime()) <= 0) {
                        throw failed(txn);
                    } else {
                        Thread.yield();
                    }

                    randomPosition(key, state.mRandom, c);
                }
            } finally {
                // Prevent the cursor from being closed prematurely due to GC. The loop isn't
                // sufficient to guard against this because the compiler might copy the
                // state.mRandom field access to a local variable.
                Reference.reachabilityFence(state);
            }
        } finally {
            entry.release();
        }
    }

    public abstract void writeTail(byte[] key, DataOutput out) throws IOException;

    /**
     * Closes all the cursors, but doesn't prevent them from being replaced.
     */
    // TODO: Try to call this when table or database is closed.
    public void clear() {
        mStatePool.clear(ref -> {
            var state = ref.get();
            if (state != null) {
                ref.clear();
                state.mCursor.reset();
            }
        });
    }

    protected abstract void randomKey(byte[] key, RandomGenerator rnd);

    protected abstract RowPredicateLock.Closer incrementKey(Transaction txn, R row,
                                                            byte[] srcKey, byte[] dstKey,
                                                            byte[] value)
        throws IOException;

    private void randomPosition(byte[] key, RandomGenerator rnd, Cursor c) throws IOException {
        randomKey(key, rnd);
        c.find(key);
        c.register();
    }

    private static LockFailureException failed(Transaction txn) {
        long nanosTimeout = txn.lockTimeout(TimeUnit.NANOSECONDS);

        String message;
        if (nanosTimeout == 0) {
            message = "Unable to immediately generate a unique identifier";
        } else {
            TimeUnit unit = RowUtils.inferUnit(TimeUnit.NANOSECONDS, nanosTimeout);
            long timeout = unit.convert(nanosTimeout, TimeUnit.NANOSECONDS);
            var b = new StringBuilder("Unable to generate a unique identifier within ");
            RowUtils.appendTimeout(b, timeout, unit);
            message = b.toString();
        }

        return new LockFailureException(message);
    }

    private static KeyState register(Cursor c, RandomGenerator rnd) {
        try {
            Cleaner cleaner = CommonCleaner.access();
            var state = new KeyState(c, rnd);
            cleaner.register(state, c::reset);
            return state;
        } catch (Throwable e) {
            c.reset();
            throw e;
        }
    }

    private static class KeyState {
        final Cursor mCursor;
        final RandomGenerator mRandom;

        KeyState(Cursor c, RandomGenerator rnd) {
            mCursor = c;
            mRandom = rnd;
        }
    }
}
