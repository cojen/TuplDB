/*
 *  Copyright 2014 Brian S O'Neill
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

import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

import static org.cojen.tupl.TestUtils.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class LargeKeyTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(LargeKeyTest.class.getName());
    }

    @Test
    public void largeBlanks() throws Exception {
        Database db = Database.open(new DatabaseConfig());
        Index ix = db.openIndex("test");

        byte[] value = new byte[0];

        byte[][] keys = {
            new byte[2000], new byte[2001], new byte[2002], new byte[2003], new byte[2004]
        };

        for (byte[] key : keys) {
            ix.store(null, key, value);
            assertTrue(ix.verify(null));
        }

        for (byte[] key : keys) {
            byte[] v = ix.load(null, key);
            assertArrayEquals(value, v);
        }
    }
}
