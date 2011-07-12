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
        java.io.File file0, file1;
        file0 = new java.io.File(args[0] + ".0");
        file1 = new java.io.File(args[0] + ".1");

        final PageStore pstore = new DualFilePageStore(file0, file1);
        final int cachedNodes = 1000;
        final TreeNodeStore store = new TreeNodeStore(pstore, cachedNodes, cachedNodes);

        final int threadCount = Integer.parseInt(args[1]);

        final int count = 1000000;
        final AtomicInteger next = new AtomicInteger();

        Thread[] threads = new Thread[threadCount];
        for (int i=0; i<threadCount; i++) {
            threads[i] = new Thread() {
                public void run() {
                    try {
                        Cursor c = null;
                        for (int n; (n = next.getAndIncrement()) < count; ) {
                            if (c == null) {
                                c = new Cursor(store);
                            } else {
                                c.verify();
                            }
                            byte[] key = ("k" + n).getBytes();
                            byte[] value = ("v" + n).getBytes();
                            c.find(key);
                            c.store(value);
                        }
                    } catch (Exception e) {
                        System.out.println("at " + next);
                        e.printStackTrace(System.out);
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
    }
}
