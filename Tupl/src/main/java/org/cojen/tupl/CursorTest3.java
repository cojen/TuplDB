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

        final Database db = Database.open
            (new DatabaseConfig().baseFile(file));
        final Index index = db.openIndex("test3");

        final int threadCount = Integer.parseInt(args[1]);

        final int count = 1000000;
        final AtomicInteger next = new AtomicInteger();

        Thread monitor = new Thread() {
            public void run() {
                try {
                    Cursor c = index.newCursor(null);
                    while (true) {
                        c.last();
                        if (c.value() != null) {
                            System.out.println("last:     " + CursorTest.string(c));
                            c.previous();
                            if (c.value() != null) {
                                System.out.println("previous: " + CursorTest.string(c));
                                c.next();
                                if (c.value() != null) {
                                    System.out.println("next:     " + CursorTest.string(c));
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

        Cursor c1 = index.newCursor(null);
        c1.find("k1000".getBytes());
        System.out.println("initial find: " + CursorTest.string(c1));

        Thread[] threads = new Thread[threadCount];
        for (int i=0; i<threadCount; i++) {
            threads[i] = new Thread() {
                public void run() {
                    try {
                        Cursor c = null;
                        for (int n; (n = next.getAndIncrement()) < count; ) {
                            if (c == null) {
                                c = index.newCursor(null);
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

        //System.out.println("find value now: " + CursorTest.string(c));

        Cursor c2 = index.newCursor(null);
        for (int i=0; i<count; i++) {
            byte[] key = ("k" + i).getBytes();
            byte[] value = ("x" + i).getBytes();
            c2.find(key);
            c2.store(value);
            if (i % 10000 == 0) {
                System.out.println(i);
            }
        }

        //System.out.println("find value now: " + CursorTest.string(e));
        //System.out.println("c1: " + c1.verify());
        //System.out.println("c2: " + c2.verify());
    }
}
