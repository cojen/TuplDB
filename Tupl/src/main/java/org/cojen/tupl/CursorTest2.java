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

        final Database db = new Database
            (DatabaseConfig.newConfig().setBaseFile(file).setMinCachedNodes(100000));
        final View view = db.openView("test2");

        Cursor c = view.newCursor();
        Cursor c2 = view.newCursor();
        Cursor c3 = view.newCursor();
        Cursor c4 = view.newCursor();

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

        Cursor c5 = view.newCursor();
        c5.find("z".getBytes());
        c5.store("zzz".getBytes());

        final long seed = 892347;
        final int count = 100000;

        Random rnd = new Random(seed);
        for (int i=0; i<count; i++) {
            int r = rnd.nextInt() & Integer.MAX_VALUE;

            byte[] key = ("k" + r).getBytes();
            byte[] value = ("v" + r).getBytes();
            if (!c.find(key)) {
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
            if (!c.find(key)) {
                System.out.println("not found: " + i);
            }
        }

        db.commit();

        c5.last();
        do {
            CursorTest.printEntry(c5);
        } while (c5.previous());
    }
}
