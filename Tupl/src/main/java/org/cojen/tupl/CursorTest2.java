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

import java.util.Random;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CursorTest2 {
    public static void main(String[] args) throws Exception {
        java.io.File file0, file1;
        file0 = new java.io.File(args[0] + ".0");
        file1 = new java.io.File(args[0] + ".1");

        final PageStore pstore = new DualFilePageStore(file0, file1);
        final int cachedNodes = 1000;
        final TreeNodeStore store = new TreeNodeStore(pstore, 0, cachedNodes);

        Cursor c = new Cursor(store);
        Cursor c2 = new Cursor(store);
        Cursor c3 = new Cursor(store);
        Cursor c4 = new Cursor(store);

        System.out.println(c.find("hello".getBytes()));
        System.out.println(c2.find("hello".getBytes()));
        System.out.println(c3.find("hellox".getBytes()));
        System.out.println(c4.find("hell".getBytes()));

        c.store("world".getBytes());

        CursorTest.printEntry(c);
        CursorTest.printEntry(c2);
        CursorTest.printEntry(c3);
        CursorTest.printEntry(c4);

        System.out.println("---");
        System.out.println(c3.previous());
        CursorTest.printEntry(c3);

        System.out.println("---");
        System.out.println(c4.next());
        CursorTest.printEntry(c4);

        Cursor c5 = new Cursor(store);
        c5.find("z".getBytes());
        c5.store("zzz".getBytes());

        final long seed = 892347;
        final int count = 318;

        Random rnd = new Random(seed);
        int lowest = Integer.MAX_VALUE;
        int highest = 0;
        for (int i=0; i<count; i++) {
            int r = rnd.nextInt() & Integer.MAX_VALUE;
            if (r == 0 || r >= 1000000000 && r < lowest) {
                lowest = r;
            }
            if (r < 1000000000 && r > highest) {
                highest = r;
            }
            byte[] key = ("k" + r).getBytes();
            byte[] value = ("v" + r).getBytes();
            c.find(key);
            c.store(value);
        }

        System.out.println("lowest:  " + lowest);
        System.out.println("highest: " + highest);

        rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int r = rnd.nextInt() & Integer.MAX_VALUE;
            byte[] key = ("k" + r).getBytes();
            byte[] value = ("v" + r).getBytes();
            if (!c.find(key)) {
                System.out.println("not found: " + i);
            }
        }

        System.out.println("----------------------");

        System.out.println(c5.previous());
        CursorTest.printEntry(c5);

        System.out.println(c2.next());
        CursorTest.printEntry(c2);

        System.out.println(c3.next());
        CursorTest.printEntry(c3);

        System.out.println(c4.next());
        CursorTest.printEntry(c4);

        //store.commit();
    }
}
