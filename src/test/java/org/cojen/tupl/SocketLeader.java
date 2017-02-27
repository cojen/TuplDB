/*
 *  Copyright 2017 Cojen.org
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
class SocketLeader {
    public static void main(String[] args) throws Exception {
        SocketReplicationManager man = new SocketReplicationManager("localhost", 3456);

        DatabaseConfig config = new DatabaseConfig()
            .minCacheSize(1_000_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .replicate(man);

        Database db = TestUtils.newTempDatabase(config);
        System.out.println(db);

        man.waitForLeadership();

        Index ix = db.openIndex("test");
        System.out.println(ix);

        Random rnd = new Random();

        while (true) {
            byte[] key = TestUtils.randomStr(rnd, 16);
            byte[] value = TestUtils.randomStr(rnd, 10, 100);
            ix.store(null, key, value);
        }
    }
}
