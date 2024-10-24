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
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.cojen.tupl.io.MappedPageArray;
import org.cojen.tupl.io.OpenOption;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SnapshotTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SnapshotTest.class.getName());
    }

    protected void decorate(DatabaseConfig config) throws Exception {
    }

    @Test
    public void suspend() throws Exception {
        final int rateMillis = 500;

        final var listener = new EventListener() {
            private int mCheckpointCount;

            @Override
            public void notify(EventType type, String message, Object... args) {
                if (type == EventType.CHECKPOINT_COMPLETE) {
                    synchronized (this) {
                        mCheckpointCount++;
                        notify();
                    }
                }
            }

            public synchronized int checkpointCount() {
                return mCheckpointCount;
            }

            public synchronized void waitForNextCheckpoint(int initialCount) throws Exception {
                if (mCheckpointCount > initialCount) {
                    return;
                }
                long end = System.currentTimeMillis() + 60_000;
                while (true) {
                    wait(60_000);
                    if (mCheckpointCount > initialCount) {
                        return;
                    }
                    if (System.currentTimeMillis() >= end) {
                        throw new Exception("Timed out");
                    }
                }
            }
        };

        var config = new DatabaseConfig()
            .directPageAccess(false)
            .checkpointRate(rateMillis, TimeUnit.MILLISECONDS)
            .checkpointSizeThreshold(0)
            .checkpointDelayThreshold(0, null)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .eventListener(listener);
        decorate(config);
        Database db = newTempDatabase(getClass(), config);

        Index ix = db.openIndex("suspend");

        db.suspendCheckpoints();
        ix.store(Transaction.BOGUS, "hello".getBytes(), "world".getBytes());
        sleep(rateMillis * 2);
        db.close();

        db = reopenTempDatabase(getClass(), db, config);

        ix = db.openIndex("suspend");
        assertNull(ix.load(null, "hello".getBytes()));

        db.suspendCheckpoints();
        ix.store(Transaction.BOGUS, "hello".getBytes(), "world".getBytes());
        sleep(rateMillis * 2);
        db.checkpoint();
        db.close();
        
        db = reopenTempDatabase(getClass(), db, config);

        ix = db.openIndex("suspend");
        fastAssertArrayEquals("world".getBytes(), ix.load(null, "hello".getBytes()));

        try {
            db.resumeCheckpoints();
            fail();
        } catch (IllegalStateException e) {
        }

        db.suspendCheckpoints();
        db.suspendCheckpoints();
        ix.store(Transaction.BOGUS, "hello".getBytes(), "universe".getBytes());
        sleep(rateMillis * 2);
        db.resumeCheckpoints();
        sleep(rateMillis * 2);
        db.close();

        db = reopenTempDatabase(getClass(), db, config);

        ix = db.openIndex("suspend");
        fastAssertArrayEquals("world".getBytes(), ix.load(null, "hello".getBytes()));

        db.suspendCheckpoints();
        db.suspendCheckpoints();
        ix.store(Transaction.BOGUS, "hello".getBytes(), "universe".getBytes());
        sleep(rateMillis * 2);
        int initialCount = listener.checkpointCount();
        db.resumeCheckpoints();
        db.resumeCheckpoints();
        listener.waitForNextCheckpoint(initialCount);
        db.close();

        db = reopenTempDatabase(getClass(), db, config);

        ix = db.openIndex("suspend");
        fastAssertArrayEquals("universe".getBytes(), ix.load(null, "hello".getBytes()));

        deleteTempDatabase(getClass(), db);
    }

    @Test
    public void snapshot() throws Exception {
        snapshot(0);
    }

    @Test
    public void snapshotExtraCheckpointThreads() throws Exception {
        snapshot(4);
    }

    private void snapshot(int extraThreads) throws Exception {
        File base = newTempBaseFile(getClass());
        File snapshotBase = newTempBaseFile(getClass());
        snapshot(base, snapshotBase, extraThreads);
        deleteTempDatabases(getClass());
    }

    private void snapshot(File base, File snapshotBase, int extraThreads) throws Exception {
        var snapshot = new File(snapshotBase.getParentFile(), snapshotBase.getName() + ".db");

        var config = new DatabaseConfig()
            .directPageAccess(false)
            .baseFile(base)
            .minCacheSize(10_000_000).maxCacheSize(100_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .maxCheckpointThreads(extraThreads);

        decorate(config);

        final Database db = Database.open(config);
        final Index index = db.openIndex("test1");

        for (int i=0; i<10000000; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            index.store(null, key.getBytes(), value.getBytes());
        }

        final var out = new FileOutputStream(snapshot);

        final var slow = new OutputStream() {
            volatile boolean fast;

            public void write(int b) throws IOException {
                throw new IOException();
            }

            public void write(byte[] b, int off, int len) throws IOException {
                if (!fast) {
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        throw new InterruptedIOException();
                    }
                }
                out.write(b, off, len);
            }

            public void close() throws IOException {
                out.close();
            }
        };

        var t = new Thread(() -> {
            try {
                var rnd = new Random(5198473);
                for (int i=0; i<1000000; i++) {
                    int k = rnd.nextInt(1000000);
                    String key = "key-" + k;
                    String value = "rnd-" + k;
                    index.store(null, key.getBytes(), value.getBytes());
                }
                slow.fast = true;
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        t.setDaemon(true);
        t.start();

        Snapshot s = db.beginSnapshot();
        long expectedLength = s.length();
        s.writeTo(slow);
        out.close();
        s.close();

        t.join();
        assertTrue(db.verify(null));
        db.close();

        assertEquals(expectedLength, snapshot.length());

        var restoredConfig = new DatabaseConfig()
            .directPageAccess(false)
            .baseFile(snapshotBase)
            .minCacheSize(10_000_000).maxCacheSize(100_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        decorate(restoredConfig);

        final Database restored = Database.open(restoredConfig);
        assertTrue(restored.verify(null));
        final Index restoredIx = restored.openIndex("test1");

        for (int i=0; i<10000000; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = restoredIx.load(null, key);
            if (value == null) {
                break;
            }
            byte[] expectedValue = ("value-" + i).getBytes();
            fastAssertArrayEquals(expectedValue, value);
        }

        restored.close();
    }

    @Test
    public void incompleteRestore() throws Exception {
        File base = newTempBaseFile(getClass());
        File snapshotFile = newTempBaseFile(getClass());
        File restoredBase = newTempBaseFile(getClass());

        var config = new DatabaseConfig()
            .directPageAccess(false)
            .baseFile(base)
            .minCacheSize(10_000_000).maxCacheSize(100_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        decorate(config);

        Database db = Database.open(config);
        Index index = db.openIndex("test1");

        for (int i=0; i<1_000_000; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            index.store(null, key.getBytes(), value.getBytes());
        }

        db.checkpoint();

        var out = new FileOutputStream(snapshotFile);

        Snapshot s = db.beginSnapshot();
        long expectedLength = s.length();
        s.writeTo(out);
        out.close();
        s.close();
        db.close();
        db = null;
        index = null;

        assertEquals(expectedLength, snapshotFile.length());

        // Throw an exception before the restore can complete.
        long limit = expectedLength / 2;

        var broken = new FilterInputStream(new FileInputStream(snapshotFile)) {
            private long mTotal = 0;

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                if (mTotal >= limit) {
                    close();
                    throw new IOException("Broken");
                } else {
                    int amt = super.read(b, off, len);
                    if (amt > 0) {
                        mTotal += amt;
                    }
                    return amt;
                }
            }
        };

        var restoredConfig = new DatabaseConfig()
            .directPageAccess(false)
            .baseFile(restoredBase)
            .minCacheSize(10_000_000).maxCacheSize(100_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        decorate(restoredConfig);

        try {
            Database.restoreFromSnapshot(restoredConfig, broken);
            fail();
        } catch (IOException e) {
            assertEquals("Broken", e.getMessage());
        }

        try {
            Database.open(restoredConfig);
            fail();
        } catch (IncompleteRestoreException e) {
            // Expected.
        }

        deleteTempFiles(getClass());
    }

    @Test
    public void restoreToMappedFile() throws Exception {
        org.junit.Assume.assumeTrue(MappedPageArray.isSupported());

        File base = newTempBaseFile(getClass());
        File snapshotFile = newTempBaseFile(getClass());
        File restoredBase = newTempBaseFile(getClass());

        var config = new DatabaseConfig()
            .directPageAccess(false)
            .pageSize(4096)
            .baseFile(base)
            .minCacheSize(10_000_000).maxCacheSize(100_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH);

        decorate(config);

        Database db = Database.open(config);
        Index index = db.openIndex("test1");
        final long indexId = index.id();

        for (int i=0; i<1_000_000; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            index.store(null, key.getBytes(), value.getBytes());
        }

        db.checkpoint();

        var out = new FileOutputStream(snapshotFile);

        Snapshot s = db.beginSnapshot();
        long expectedLength = s.length();
        s.writeTo(out);
        out.close();
        s.close();
        db.close();
        db = null;
        index = null;

        assertEquals(expectedLength, snapshotFile.length());

        Supplier<MappedPageArray> openMap = () -> {
            try {
                return MappedPageArray.open
                    (4096, 25_000, restoredBase, EnumSet.of(OpenOption.CREATE));
            } catch (IOException e) {
                throw Utils.rethrow(e);
            }
        };

        // Note that no base file is provided. This means no redo logging.
        var restoredConfig = new DatabaseConfig()
            .directPageAccess(false)
            .pageSize(4096)
            .dataPageArray(openMap.get())
            .minCacheSize(10_000_000).maxCacheSize(100_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .checkpointRate(-1, null);

        decorate(restoredConfig);
        
        Database restored = Database.restoreFromSnapshot
            (restoredConfig, new FileInputStream(snapshotFile));

        assertTrue(restored.verify(null));
        Index restoredIx = restored.openIndex("test1");
        assertEquals(indexId, restoredIx.id());

        for (int i=0; i<1_000_000; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = restoredIx.load(null, key);
            byte[] expectedValue = ("value-" + i).getBytes();
            fastAssertArrayEquals(expectedValue, value);
        }

        restoredIx.store(null, "hello".getBytes(), "world".getBytes());
        fastAssertArrayEquals("world".getBytes(), restoredIx.load(null, "hello".getBytes()));

        restored = reopenTempDatabase
            (getClass(), restored,
             (file) -> restoredConfig.baseFile(file).dataPageArray(openMap.get()));

        restoredIx = restored.openIndex("test1");
        assertEquals(indexId, restoredIx.id());

        for (int i=0; i<1_000_000; i++) {
            byte[] key = ("key-" + i).getBytes();
            byte[] value = restoredIx.load(null, key);
            byte[] expectedValue = ("value-" + i).getBytes();
            fastAssertArrayEquals(expectedValue, value);
        }

        // No redo, so it's lost.
        assertNull(restoredIx.load(null, "hello".getBytes()));

        restored.close();

        deleteTempFiles(getClass());
    }
}
