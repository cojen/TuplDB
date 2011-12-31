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

import java.io.IOException;

import java.util.*;

import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class TreeNodeTest {
    public static void main(String[] args) throws Exception {
        java.io.File file = new java.io.File(args[0]);

        final Database db = Database.open
            (new DatabaseConfig()
             .setBaseFile(file)
             .setMinCacheSize(400_000_000)
             .setDurabilityMode(DurabilityMode.NO_FLUSH));
        //db.preallocate(500_000_000L);
        final Index index = db.openIndex("test1");

        if (false) {
            db.trace();
            System.exit(0);
        }

        /* FIXME: observed defect, after ctrl-break: (spurious wakeup problem?)
         java.lang.AssertionError: Split child is not already marked dirty
          at org.cojen.tupl.TreeNode.insertSplitChildRef(TreeNode.java:1178)
          at org.cojen.tupl.TreeCursor.finishSplit(TreeCursor.java:1647)
          at org.cojen.tupl.TreeCursor.finishSplit(TreeCursor.java:1640)
          at org.cojen.tupl.TreeCursor.store(TreeCursor.java:969)
          at org.cojen.tupl.TreeCursor.store(TreeCursor.java:746)
          at org.cojen.tupl.FullCursor.store(FullCursor.java:114)
          at org.cojen.tupl.TreeNodeTest.testInsert(TreeNodeTest.java:135)
          at org.cojen.tupl.TreeNodeTest.main(TreeNodeTest.java:107)

         ...followed by this after re-opening: (page is blank!)
         java.lang.ArrayIndexOutOfBoundsException: -2
          at org.cojen.tupl.DataIO.readUnsignedShort(DataIO.java:26)
          at org.cojen.tupl.TreeNode.splitLeafAndCreateEntry(TreeNode.java:2098)
          at org.cojen.tupl.TreeNode.insertLeafEntry(TreeNode.java:1072)
          at org.cojen.tupl.TreeCursor.store(TreeCursor.java:929)
          at org.cojen.tupl.TreeCursor.store(TreeCursor.java:746)
          at org.cojen.tupl.FullCursor.store(FullCursor.java:114)
          at org.cojen.tupl.TreeNodeTest.testInsert(TreeNodeTest.java:135)
          at org.cojen.tupl.TreeNodeTest.main(TreeNodeTest.java:54)

          Ctrl-break was hit during a checkpoint, and so cause might be a checkpoint
          race condition, made worse by the thread dump.

          Problem seems to occur just from killing the process. It might be a
          bug with file length in FilePageArray.

          Is it possible that dirty nodes got written out after fsync? Is there
          a race between the sorted sweep and eviction? Race with splits?
        */

        byte[] value = index.get(null, "hello".getBytes());
        System.out.println(value == null ? null : new String(value));

        Map<String, String> map = new TreeMap<String, String>();
        map = null;

        testInsert(map, true, index, "hello", "world");

        Thread t = new Thread() {
            public void run() {
                try {
                    long lastDuration = 0;

                    while (true) {
                        long delay = 1000L - lastDuration;
                        if (delay > 0) {
                            Thread.sleep(delay);
                        }

                        System.out.println("checkpoint...");
                        long start = System.currentTimeMillis();
                        db.checkpoint();
                        long end = System.currentTimeMillis();
                        //System.out.println("...done: " + pstore.stats());
                        System.out.println("...done");

                        //Thread.sleep(500);
                        //System.exit(0);

                        lastDuration = end - start;
                    }
                } catch (Exception e) {
                    e.printStackTrace(System.out);
                    System.exit(1);
                }
            }
        };

        t.setDaemon(true);
        t.start();

        Random rnd = new Random(89234723);

        long start = System.currentTimeMillis();
        for (int i=1; i<10_000_000; i++) {
            /*
            if (i % 100000 == 0) {
                db.checkpoint();
            }
            */
            if (i % 10000 == 0) {
                //System.out.println("" + i + ": " + pstore.stats());
                System.out.println(i);
            }
            boolean fullTest = i >= 99999999;
            fullTest = false;
            if (fullTest) {
                System.out.println(i);
            }
            long k = rnd.nextLong() & Long.MAX_VALUE;
            testInsert(map, fullTest, index,
                       "key-".concat(String.valueOf(k)), "value-".concat(String.valueOf(i)));
            /*
            if (i % 10 == 0) {
                Thread.sleep(1);
            }
            */
        }

        //root.dump(store, "");

        db.checkpoint();

        long end = System.currentTimeMillis();

        System.out.println("duration: " + new org.joda.time.Duration(end - start));
    }

    private static void testInsert(Map<String, String> map, boolean fullTest,
                                   Index index, String key, String value)
        throws IOException
    {
        if (map != null) {
            map.put(key, value);
        }

        byte[] bkey = key.getBytes();
        byte[] bvalue = value.getBytes();

        Cursor c = index.newCursor(Transaction.BOGUS);
        c.find(bkey);
        boolean exists = c.value() != null;
        if (!exists) {
            c.store(bvalue);
        }
        c.close();

        /*
        byte[] fvalue = index.get(null, bkey);
        try {
            compareArrays(bvalue, fvalue);
        } catch (AssertionError e) {
            System.out.println(CursorTest.string(bkey));
            System.out.println(CursorTest.string(bvalue));
            System.out.println(CursorTest.string(fvalue));
            //root.dump(store, "");
            throw e;
        }

        if (fullTest && map != null) {
            for (Map.Entry<String, String> entry : map.entrySet()) {
                //System.out.println(entry);
                bkey = entry.getKey().getBytes();
                bvalue = entry.getValue().getBytes();
                fvalue = index.get(null, bkey);
                try {
                    compareArrays(bvalue, fvalue);
                } catch (AssertionError e) {
                    System.out.println("not equals: " + entry);
                    //root.dump(store, "");
                    throw e;
                }
            }
        }
        */
    }

    private static void compareArrays(byte[] a, byte[] b) {
        // Faster than assertArrayEquals method.
        assertTrue(Arrays.equals(a, b));
    }
}
