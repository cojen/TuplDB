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

import java.io.*;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SnapshotMappedTest extends SnapshotTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SnapshotMappedTest.class.getName());
    }

    @BeforeClass
    public static void isSupported() {
        TestUtils.assume64bit();
    }

    @After
    public void cleanup() throws Exception {
        deleteTempDatabases(getClass());
    }

    @Override
    public void decorate(DatabaseConfig config) throws Exception {
        config.mapDataFiles(true);
    }

    @Test
    public void fullyMapped() throws Exception {
        Database db = newTempDatabase(getClass(), 50_000_000, TestUtils.OpenMode.MAPPED, 0);

        Index ix = db.openIndex("test");
        
        for (int i=0; i<800_000; i++) {
            ix.store(null, key(i), value(i));
        }

        db.checkpoint();

        // Create an undo log which spans a checkpoint and a snapshot.

        Transaction txn = db.newTransaction();
        for (int i=0; i<10_000; i++) {
            ix.delete(txn, key(i));
            if (i % 1000 == 0) {
                db.checkpoint();
            }
        }

        db.checkpoint();

        Snapshot s = db.beginSnapshot();
        long fullLength = s.length();
        assertTrue(fullLength > 20_000_000);

        File snapshotBase = newTempBaseFile(getClass());
        var snapshot = new File(snapshotBase.getParentFile(), snapshotBase.getName() + ".db");

        final var out = new FileOutputStream(snapshot);

        var sout = new OutputStream() {
            private long mLimit;
            private long mTotal;
            private boolean mSuspended;

            synchronized void setLimit(long limit) {
                mLimit = limit;
                notifyAll();
            }

            synchronized void waitUntilSuspended() throws Exception {
                while (!mSuspended) {
                    wait();
                }
            }

            @Override
            public void write(int b) throws IOException {
                throw new IOException();
            }

            @Override
            public synchronized void write(byte[] b, int off, int len) throws IOException {
                out.write(b, off, len);

                mTotal += len;

                try {
                    while (mTotal >= mLimit) {
                        mSuspended = true;
                        notifyAll();
                        wait();
                    }
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
                mSuspended = false;
                notifyAll();
            }

            @Override
            public synchronized void close() throws IOException {
                setLimit(Long.MAX_VALUE);
                out.close();
            }
        };

        // Copy half the snapshot for now.
        sout.setLimit(fullLength / 2);

        var writer = new Thread(() -> {
            try {
                s.writeTo(sout);
                sout.close();
            } catch (IOException e) {
            }
        });

        writer.start();

        sout.waitUntilSuspended();

        // Commit the long transaction.
        txn.commit();
        db.checkpoint();

        // Delete everything.
        db.deleteIndex(ix).run();
        db.checkpoint();

        // Finish the snapshot.
        sout.setLimit(Long.MAX_VALUE);
        writer.join();

        var restoredConfig = new DatabaseConfig()
            .baseFile(snapshotBase)
            .minCacheSize(50_000_000)
            .checkpointRate(0, null)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        Database restored = Database.open(restoredConfig);

        assertTrue(restored.verify(null, 2));

        ix = restored.findIndex("test");
        assertNotNull(ix);
        
        for (int i=0; i<800_000; i++) {
            fastAssertArrayEquals(value(i), ix.load(null, key(i)));
        }

        restored.close();
        db.close();
    }

    private static byte[] key(int i) {
        return ("key-" + i).getBytes();
    }

    private static byte[] value(int i) {
        return ("value-" + i).getBytes();
    }
}
