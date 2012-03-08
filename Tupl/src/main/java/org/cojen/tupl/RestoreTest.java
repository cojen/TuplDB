/*
 *  Copyright 2012 Brian S O'Neill
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

import java.io.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RestoreTest {
    public static void main(String[] args) throws Exception {
        File base = new File(args[0]);
        FileInputStream in = new FileInputStream(args[1]);

        final Database db = Database.restoreFromSnapshot
            (new DatabaseConfig()
             .baseFile(base)
             .minCacheSize(100000000)
             .durabilityMode(DurabilityMode.NO_FLUSH)
             .checkpointRate(-1, null),
             in);
        in.close();

        final Index index = db.openIndex("test1");

        Cursor c = index.newCursor(null);
        c.first();
        while (c.key() != null) {
            String key = new String(c.key());
            String value = new String(c.value());
            System.out.println(key + " -> " + value);
            c.next();
        }
    }
}
