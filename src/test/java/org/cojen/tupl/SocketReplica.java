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

/**
 * 
 *
 * @author Brian S O'Neill
 */
class SocketReplica {
    public static void main(String[] args) throws Exception {
        SocketReplicationManager man = new SocketReplicationManager(null, 3456);

        DatabaseConfig config = new DatabaseConfig()
            .minCacheSize(1_000_000_000)
            .durabilityMode(DurabilityMode.NO_FLUSH)
            .replicate(man)
            .maxReplicaThreads(1);

        Database db = TestUtils.newTempDatabase(config);
        System.out.println(db);

        Index ix;
        while (true) {
            try {
                ix = db.openIndex("test");
                break;
            } catch (UnmodifiableReplicaException e) {
                Thread.sleep(100);
                continue;
            }
        }

        System.out.println(ix);

        while (true) {
            System.out.println(ix.count(null, null));
            Thread.sleep(1000);
        }
    }
}
