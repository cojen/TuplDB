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
        java.io.File file = new java.io.File(args[0]);

        final Database db = Database.open
            (new DatabaseConfig().setBaseFile(file).setMinCacheSize(400000000L));
        final Index index = db.openIndex("test2");

        Cursor c = index.newCursor(null);
        Cursor c2 = index.newCursor(null);
        Cursor c3 = index.newCursor(null);
        Cursor c4 = index.newCursor(null);

        c.find("hello".getBytes());
        CursorTest.printEntry(c);

        c2.find("hello".getBytes());
        CursorTest.printEntry(c2);

        c3.find("hellox".getBytes());
        CursorTest.printEntry(c3);

        c4.find("hell".getBytes());
        CursorTest.printEntry(c4);

        c.store("world".getBytes());

        CursorTest.printEntry(c);
        CursorTest.printEntry(c2);
        CursorTest.printEntry(c3);
        CursorTest.printEntry(c4);

        System.out.println("---");
        c3.previous();
        CursorTest.printEntry(c3);

        System.out.println("---");
        c4.next();
        CursorTest.printEntry(c4);

        Cursor c5 = index.newCursor(null);
        c5.find("z".getBytes());
        c5.store("zzz".getBytes());

        final long seed = 892347;
        final int count = 100000;

        Random rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int r = rnd.nextInt() & Integer.MAX_VALUE;

            byte[] key = ("k" + r).getBytes();
            byte[] value = ("v" + r).getBytes();
            c.find(key);
            if (c.value() == null) {
                c.store(value);
            }

            /*
            try {
                c.verify();
                c2.verify();
                c3.verify();
                c4.verify();
                c5.verify();
            } catch (IllegalStateException e) {
                System.out.println("i: " + i);
                throw e;
            }
            */
        }

        rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int r = rnd.nextInt() & Integer.MAX_VALUE;
            byte[] key = ("k" + r).getBytes();
            byte[] value = ("v" + r).getBytes();
            c.find(key);
            if (c.value() == null) {
                System.out.println("not found: " + i);
            }
        }

        db.checkpoint();

        c5.last();
        do {
            CursorTest.printEntry(c5);
            c5.previous();
        } while (c5.value() != null);
    }
}
