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

import java.util.concurrent.Semaphore;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CursorTest4 {
    public static void main(String[] args) throws Exception {
        java.io.File file = new java.io.File(args[0]);

        final Database db = new Database
            (DatabaseConfig.newConfig().setBaseFile(file).setMinCachedNodes(10000));
        final Index index = db.openIndex("test4");

        final int count = 1000000;

        // Fill with even keys, never deleted.
        {
            Cursor c = index.newCursor();
            for (int i=0; i<count; i+=2) {
                byte[] key = toKey(i);
                byte[] value = ("v" + i).getBytes();
                c.findNearby(key);
                c.store(value);
            }
            c.reset();
        }

        System.out.println("commit");
        db.commit();

        // FIXME: Testing with no concurrent deletes.
        //final Semaphore sem = new Semaphore(1, true);

        Thread scanner = new Thread() {
            public void run() {
                try {
                    doRun();
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    System.exit(1);
                }
            }

            private void doRun() throws Exception {
                while (true) {
                    //sem.acquire();

                    Cursor c = index.newCursor();
                    Entry e = new Entry();
                    c.first();

                    int i = 0;
                    do {
                        try {
                            c.getEntry(e);
                        } catch (NullPointerException ex) {
                            ex.printStackTrace(System.out);
                            break;
                        }

                        int key = DataIO.readInt(e.key, 0);

                        if ((key & 1) != 0) {
                            // Skip odd keys.
                            continue;
                        }

                        if (key < i) {
                            System.out.println("backwards: " + key + " < " + i);
                            break;
                        }

                        String value = CursorTest.string(e.value);
                        if (key != i || !value.equals("v" + i)) {
                            System.out.println("skipped: " + i + ", " + key);
                            break;
                        }

                        i += 2;
                    } while (c.next());

                    c.reset();

                    if (i != count) {
                        System.out.println("not reached end: " + i);
                    }

                    //sem.release();
                }
            }
        };

        scanner.start();

        while (true) {
            // Concurrently insert odd keys...
            {
                Cursor c = index.newCursor();
                for (int i=1; i<count; i+=2) {
                    byte[] key = toKey(i);
                    byte[] value = ("v" + i).getBytes();
                    c.findNearby(key);
                    c.store(value);
                }
                c.reset();
            }

            System.out.println("inserted odd");

            // Concurrently delete odd keys...
            {
                //sem.acquire();
                Cursor c = index.newCursor();
                for (int i=1; i<count; i+=2) {
                    byte[] key = toKey(i);
                    byte[] value = ("v" + i).getBytes();
                    c.findNearby(key);
                    c.store(null);
                }
                c.reset();
                //sem.release();
            }

            System.out.println("deleted odd");
        }
    }

    static byte[] toKey(int i) {
        byte[] k = new byte[4];
        DataIO.writeInt(k, 0, i);
        return k;
    }

    static String string(Entry entry) {
        return DataIO.readInt(entry.key, 0) + " = " + CursorTest.string(entry.value);
    }
}
