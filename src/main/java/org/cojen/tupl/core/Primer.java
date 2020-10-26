/*
 *  Copyright 2011-2019 Cojen.org
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

import java.io.DataInput;
import java.io.InterruptedIOException;
import java.io.IOException;

import org.cojen.tupl.Cursor;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.View;

import org.cojen.tupl.util.Runner;

/**
 * Decodes a cache primer stream and prefetches keys for an index (or view).
 *
 * @author Brian S O'Neill
 */
final class Primer {
    private final View mView;
    private final DataInput mDin;
    private final int mTaskLimit;

    private int mTaskCount;
    private boolean mFinished;
    private IOException mEx;

    Primer(View view, DataInput din) {
        mView = view;
        mDin = din;
        // TODO: Limit should be based on the concurrency level of the I/O system.
        // TODO: Cache primer order should be scrambled, to improve concurrent priming.
        mTaskLimit = Runtime.getRuntime().availableProcessors() * 8;
    }

    void run() throws IOException {
        synchronized (this) {
            mTaskCount++;
        }

        prime();

        // Wait for other task threads to finish.
        synchronized (this) {
            while (true) {
                if (mEx != null) {
                    throw mEx;
                }
                if (mTaskCount <= 0) {
                    break;
                }
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }
        }
    }

    private void prime() {
        try {
            Cursor c = mView.newCursor(Transaction.BOGUS);

            try {
                c.autoload(false);

                while (true) {
                    byte[] key;

                    synchronized (this) {
                        if (mFinished) {
                            return;
                        }

                        int len = mDin.readUnsignedShort();

                        if (len == 0xffff) {
                            mFinished = true;
                            return;
                        }

                        key = new byte[len];
                        mDin.readFully(key);

                        if (mTaskCount < mTaskLimit) spawn: {
                            try {
                                Runner.start(this::prime);
                            } catch (Throwable e) {
                                break spawn;
                            }
                            mTaskCount++;
                        }
                    }

                    c.findNearby(key);
                }
            } catch (IOException e) {
                synchronized (this) {
                    if (mEx == null) {
                        mEx = e;
                    }
                }
            } finally {
                c.reset();
            }
        } finally {
            synchronized (this) {
                mTaskCount--;
                notifyAll();
            }
        }
    }
}
