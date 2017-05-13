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

import org.junit.*;

/**
 * Tests multiple threads inserting interleaving records adjacent to each other. This is a
 * regression test, which exersizes concurrent cursor frame binding, unbinding, and rebinding.
 *
 * @author Brian S O'Neill
 */
public class ContentionTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ContentionTest.class.getName());
    }

    protected DatabaseConfig decorate(DatabaseConfig config) throws Exception {
        config.directPageAccess(false);
        return config;
    }

    @Test
    public void noContention() throws Throwable {
        contention(1_000_000, 1);
    }

    @Test
    public void twoThreads() throws Throwable {
        contention(1_000_000, 2);
    }

    @Test
    public void fourThreads() throws Throwable {
        contention(1_000_000, 4);
    }

    private void contention(int insertCount, int threadCount) throws Throwable {
        Database db = Database.open(decorate(new DatabaseConfig().minCacheSize(100_000_000)));
        Index ix = db.openIndex("test");

        class Runner extends Thread {
            private final int start;
            private final int inc;

            private volatile Throwable fail;
            private boolean finished;

            Runner(int start, int inc) {
                this.start = start;
                this.inc = inc;
            }

            @Override
            public void run() {
                try {
                    byte[] key = new byte[4];
                    byte[] value = new byte[0];
                    int inc = threadCount;
                    int end = insertCount;
                    for (int k=start; k<end; k+=inc) {
                        Utils.encodeIntBE(key, 0, k);
                        ix.insert(Transaction.BOGUS, key, value);
                    }
                } catch (Throwable e) {
                    fail = e;
                }

                synchronized (this) {
                    finished = true;
                    notifyAll();
                }
            }

            synchronized void waitToFinish() throws Throwable {
                while (!finished) {
                    wait();
                }
                Throwable e = fail;
                if (e != null) {
                    throw e;
                }
            }
        }

        Runner[] runners = new Runner[threadCount];

        for (int i=0; i<threadCount; i++) {
            runners[i] = new Runner(i, threadCount);
        }

        for (Runner r : runners) {
            r.start();
        }

        for (Runner r : runners) {
            r.waitToFinish();
        }

        db.close();
    }
}
