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

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class CursorTest {
    public static void main(String[] args) throws Exception {
        java.io.File file0, file1;
        file0 = new java.io.File(args[0] + ".0");
        file1 = new java.io.File(args[0] + ".1");

        final PageStore pstore = new DualFilePageStore(file0, file1);
        final int cachedNodes = 1000;
        final TreeNodeStore store = new TreeNodeStore(pstore, 0, cachedNodes);
        final TreeNode root = store.root();

        //root.dump(store, "");

        Cursor c = new Cursor(store, root);

        System.out.println(c.find("key-5".getBytes()));
        printEntry(c);
        c.next();
        printEntry(c);
        c.previous();
        printEntry(c);
        c.reset();

        /*
        if (c.first()) {
            do {
                printEntry(c);
            } while (c.next());
        }
        */
    }

    private static void printEntry(Cursor c) throws Exception {
        Entry entry = new Entry();
        c.getEntry(entry);
        System.out.println(string(entry.key) + " = " + string(entry.value));
    }

    private static String string(byte[] b) {
        return b == null ? "null" : new String(b);
    }
}
