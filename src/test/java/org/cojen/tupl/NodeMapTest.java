/*
 *  Copyright (C) 2011-2017 Cojen.org
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
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
            Node n = new Node(null, p_calloc(page, false));
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
            Node n = new Node(null, p_calloc(page, false));
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
