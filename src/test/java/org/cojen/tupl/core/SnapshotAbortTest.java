/*
 *  Copyright 2020 Cojen.org
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

import java.util.concurrent.atomic.AtomicReference;

import org.cojen.tupl.*;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * Test various snapshot abort modes.
 *
 * @author Brian S O'Neill
 */
public class SnapshotAbortTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SnapshotAbortTest.class.getName());
    }

    @After
    public void teardown() throws Exception {
        deleteTempDatabases(getClass());
    }

    @Test
    public void doubleWrite() throws Exception {
        Database db = newTempDatabase(getClass());
        Index index = db.openIndex("test");

        for (int i=0; i<1000; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            index.store(null, key.getBytes(), value.getBytes());
        }

        db.checkpoint();

        final var slow = new OutputStream() {
            public void write(int b) throws IOException {
                throw new IOException();
            }

            public void write(byte[] b, int off, int len) throws IOException {
                try {
                    Thread.sleep(60_000);
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }
        };

        Snapshot snap = db.beginSnapshot();

        var ex = new AtomicReference<Throwable>();

        Thread first = startAndWaitUntilBlocked(new Thread(() -> {
            try {
                snap.writeTo(slow);
            } catch (Throwable e) {
                ex.set(e);
            }
        }));

        try {
            snap.writeTo(slow);
            fail();
        } catch (IllegalStateException e) {
            assertEquals("Snapshot already started", e.getMessage());
        }

        first.interrupt();
        first.join();

        assertTrue(ex.get() instanceof InterruptedIOException);

        try {
            snap.writeTo(slow);
        } catch (IOException e) {
            assertEquals("Snapshot closed", e.getMessage());
        }

        snap.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void explicitClose() throws Exception {
        Database db = newTempDatabase(getClass());
        Index index = db.openIndex("test");

        for (int i=0; i<1000; i++) {
            String key = "key-" + i;
            String value = "value-" + i;
            index.store(null, key.getBytes(), value.getBytes());
        }

        db.checkpoint();

        final var slow = new OutputStream() {
            public void write(int b) throws IOException {
                throw new IOException();
            }

            public void write(byte[] b, int off, int len) throws IOException {
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                }
            }
        };

        Snapshot[] snaps = new Snapshot[3];
        var exRefs = new AtomicReference[snaps.length];
        Thread[] threads = new Thread[3];

        for (int i=0; i<snaps.length; i++) {
            snaps[i] = db.beginSnapshot();

            exRefs[i] = new AtomicReference();

            final int fi = i;
            threads[i] = startAndWaitUntilBlocked(new Thread(() -> {
                try {
                    snaps[fi].writeTo(slow);
                } catch (Throwable e) {
                    exRefs[fi].set(e);
                }
            }));
        }

        for (Snapshot snap : snaps) {
            snap.close();
        }

        for (Thread t : threads) {
            t.join();
        }

        for (var exRef : exRefs) {
            var ex = (Throwable) exRef.get();
            assertTrue(ex instanceof IOException);
            assertEquals("Snapshot closed", ex.getMessage());
        }
    }
}
