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
        java.io.File file = new java.io.File(args[0]);

        final Database db = Database.open
            (new DatabaseConfig().setBaseFile(file));
        final Index index = db.openIndex("test1");

        Cursor c = index.newCursor(null);

        c.find("key-5".getBytes());
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

    static void printEntry(Cursor cursor) throws Exception {
        System.out.println(string(cursor));
    }

    static String string(byte[] b) {
        return b == null ? "null" : new String(b);
    }

    static String string(byte[] b, int off, int len) {
        return b == null ? "null" : new String(b, off, len);
    }

    static String string(Cursor cursor) {
        return string(cursor.key()) + " = " + string(cursor.value());
    }
}
