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

        final Database db = Database.open
            (new DatabaseConfig().baseFile(file).minCacheSize(40000000));
        final Index index = db.openIndex("test4");

        final int count = 1000000;

        // Fill with even keys, never deleted.
        {
            Cursor c = index.newCursor(null);
            for (int i=0; i<count; i+=2) {
                byte[] key = toKey(i);
                byte[] value = ("v" + i).getBytes();
                c.findNearby(key);
                c.store(value);
            }
            c.reset();
        }

        System.out.println("checkpoint");
        db.checkpoint();

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

                    Cursor c = index.newCursor(null);
                    c.last();

                    int i = count - 2;
                    do {
                        int key = DataUtils.readInt(c.key(), 0);

                        if ((key & 1) != 0) {
                            // Skip odd keys.
                            continue;
                        }

                        if (key > i) {
                            System.out.println("forwards: " + key + " > " + i);
                            break;
                        }

                        String value = CursorTest.string(c.value());
                        if (key != i || !value.equals("v" + i)) {
                            System.out.println("skipped: " + i + ", " + key);
                            break;
                        }

                        i -= 2;
                        c.previous();
                    } while (c.value() != null);

                    c.reset();

                    if (i != -2) {
                        System.out.println("not reached start: " + i);
                    }

                    //sem.release();
                }
            }
        };

        scanner.start();

        while (true) {
            // Concurrently insert odd keys...
            {
                Cursor c = index.newCursor(null);
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
                Cursor c = index.newCursor(null);
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
        DataUtils.writeInt(k, 0, i);
        return k;
    }

    static String string(Cursor cursor) {
        return DataUtils.readInt(cursor.key(), 0) + " = " + CursorTest.string(cursor.value());
    }
}
