/*
 *  Copyright 2014-2015 Cojen.org
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

import static org.cojen.tupl.PageOps.p_calloc;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class NodeMapTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(NodeMapTest.class.getName());
    }

    @Test
    public void fill() throws Exception {
        final int page  = 512;
        final int size  = 10000;
        final int count = 100000;

        final int idOffset = 1000;

        LocalDatabase db = LocalDatabase.open
            (new DatabaseConfig()
             .directPageAccess(false).pageSize(page).maxCacheSize(size * page));

        for (int i=0; i<count; i++) {
            Node n = new Node(null, p_calloc(page));
            long id = idOffset + i;
            n.mId = id;
            db.nodeMapPut(n, Long.hashCode(id));
        }

        for (int i=0; i<count; i++) {
            long id = idOffset + i;
            Node n = db.nodeMapGet(id, Long.hashCode(id));
            assertEquals(id, n.mId);
        }

        for (int i=0; i<count; i++) {
            long id = idOffset + i;
            int hash = Long.hashCode(id);
            Node n = db.nodeMapGet(id, hash);
            assertEquals(id, n.mId);
            db.nodeMapRemove(n, hash);
            n.delete(db);
            assertNull(db.nodeMapGet(id, hash));
        }
    }

    @Test
    public void clear() throws Exception {
        final int page  = 512;
        final int size  = 10000;
        final int count = 100000;

        final int idOffset = 1000;

        LocalDatabase db = LocalDatabase.open
            (new DatabaseConfig()
             .directPageAccess(false).pageSize(page).maxCacheSize(size * page));

        Node[] nodes = new Node[count];
        for (int i=0; i<count; i++) {
            Node n = new Node(null, p_calloc(page));
            long id = idOffset + i;
            n.mId = id;
            db.nodeMapPut(n, Long.hashCode(id));
            nodes[i] = n;
        }

        db.nodeMapDeleteAll();

        for (int i=0; i<count; i++) {
            long id = idOffset + i;
            assertNull(db.nodeMapGet(id, Long.hashCode(id)));
        }

        for (Node n : nodes) {
            assertNull(n.mNodeMapNext);
        }
    }
}
