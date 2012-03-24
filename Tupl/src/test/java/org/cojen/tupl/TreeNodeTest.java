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
@org.junit.Ignore
public class TreeNodeTest {
    public static void main(String[] args) throws Exception {
        java.io.File file = new java.io.File(args[0]);

        final Database db = Database.open
            (new DatabaseConfig()
             .baseFile(file)
             .minCacheSize(400000000)
             //.syncWrites(true)
             .durabilityMode(DurabilityMode.NO_FLUSH));
        //db.preallocate(500000000L);
        final Index index = db.openIndex("test1");

        if (false) {
            db.trace();
            System.exit(0);
        }

        byte[] value = index.load(null, "hello".getBytes());
        System.out.println(value == null ? null : new String(value));

        Map<String, String> map = new TreeMap<String, String>();
        map = null;

        testInsert(map, true, index, "hello", "world");

        Random rnd = new Random(89234723);

        long start = System.currentTimeMillis();
        for (int i=1; i<10000000; i++) {
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

        //((Tree) index).mRoot.dump(db, "");

        db.checkpoint();

        long end = System.currentTimeMillis();

        System.out.println("duration: " + (end - start));
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

        /*
        Cursor c = index.newCursor(Transaction.BOGUS);
        c.find(bkey);
        boolean exists = c.value() != null;
        if (!exists) {
            c.store(bvalue);
        }
        c.close();
        */

        index.store(Transaction.BOGUS, bkey, bvalue);

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
