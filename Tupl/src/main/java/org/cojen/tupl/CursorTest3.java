/*
 *  Copyright 2011 Brian S O'Neill
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

import java.util.concurrent.atomic.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CursorTest3 {
    public static void main(String[] args) throws Exception {
        java.io.File file = new java.io.File(args[0]);

        final Database db = new Database
            (DatabaseConfig.newConfig().setBaseFile(file).setMinCachedNodes(1000));
        final OrderedView view = db.openOrderedView("test3");

        final int threadCount = Integer.parseInt(args[1]);

        final int count = 1000000;
        final AtomicInteger next = new AtomicInteger();

        Thread committer = new Thread() {
            public void run() {
                try {
                    long lastDuration = 0;

                    while (true) {
                        long delay = 1000L - lastDuration;
                        if (delay > 0) {
                            Thread.sleep(delay);
                        }

                        System.out.println("commit...");
                        long start = System.currentTimeMillis();
                        db.commit();
                        long end = System.currentTimeMillis();
                        //System.out.println("...done: " + pstore.stats());
                        System.out.println("...done");

                        //Thread.sleep(500);
                        //System.exit(0);

                        lastDuration = end - start;
                    }
                } catch (Throwable e) {
                    synchronized (System.err) {
                        e.printStackTrace();
                    }
                    System.exit(1);
                }
            }
        };

        committer.setDaemon(true);
        //committer.start();

        Thread monitor = new Thread() {
            public void run() {
                try {
                    Cursor c = view.newCursor();
                    Entry e = new Entry();
                    while (true) {
                        if (c.last()) {
                            c.getEntry(e);
                            System.out.println("last:     " + CursorTest.string(e));
                            if (c.previous()) {
                                c.getEntry(e);
                                System.out.println("previous: " + CursorTest.string(e));
                                if (c.next()) {
                                    c.getEntry(e);
                                    System.out.println("next:     " + CursorTest.string(e));
                                }
                            }
                        }
                        Thread.sleep(100);
                    }
                } catch (Throwable e) {
                    synchronized (System.err) {
                        e.printStackTrace();
                    }
                }
            }
        };

        monitor.setDaemon(true);
        //monitor.start();

        Cursor c1 = view.newCursor();
        System.out.println("initial find: " + c1.find("k1000".getBytes()));

        Thread[] threads = new Thread[threadCount];
        for (int i=0; i<threadCount; i++) {
            threads[i] = new Thread() {
                public void run() {
                    try {
                        Cursor c = null;
                        for (int n; (n = next.getAndIncrement()) < count; ) {
                            if (c == null) {
                                c = view.newCursor();
                            } else {
                                //c.verify();
                                //Cursor copy = c.copy();
                                //copy.reset();
                            }
                            byte[] key = ("k" + n).getBytes();
                            byte[] value = ("v" + n).getBytes();
                            c.findNearby(key);
                            c.store(null);//value);
                            if (n % 10000 == 0) {
                                System.out.println(n);
                            }
                        }
                    } catch (Throwable e) {
                        synchronized (System.err) {
                            System.err.println("at " + next);
                            e.printStackTrace();
                        }
                    }
                }
            };
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }

        /*
        Cursor c = new Cursor(store);
        c.first();
        do {
            System.out.println(CursorTest.string(c.getEntry()));
        } while (c.next());
        */

        //store.root().dump(store, "");

        System.out.println("find value now: " + CursorTest.string(c1.getEntry()));

        Cursor c2 = view.newCursor();
        for (int i=0; i<count; i++) {
            byte[] key = ("k" + i).getBytes();
            byte[] value = ("x" + i).getBytes();
            c2.find(key);
            c2.store(value);
            if (i % 10000 == 0) {
                System.out.println(i);
            }
        }

        System.out.println("find value now: " + CursorTest.string(c1.getEntry()));
        //System.out.println("c1: " + c1.verify());
        //System.out.println("c2: " + c2.verify());

        if (committer.isAlive()) {
            System.out.println("last commit...");
            long start = System.currentTimeMillis();
            db.commit();
            long end = System.currentTimeMillis();
            //System.out.println("...done: " + pstore.stats());
            System.out.println("...last commit done");
        }
    }
}
