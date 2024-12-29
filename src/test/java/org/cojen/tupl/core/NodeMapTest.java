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

package org.cojen.tupl.core;

import static org.cojen.tupl.core.PageOps.p_callocPage;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

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

        LocalDatabase db = (LocalDatabase) Database.open
            (new DatabaseConfig()
             .pageSize(page).maxCacheSize(size * page));

        for (int i=0; i<count; i++) {
            var n = new Node(null, p_callocPage(page));
            long id = idOffset + i;
            n.id(id);
            db.nodeMapPut(n, Long.hashCode(id));
        }

        for (int i=0; i<count; i++) {
            long id = idOffset + i;
            Node n = db.nodeMapGet(id, Long.hashCode(id));
            assertEquals(id, n.id());
        }

        for (int i=0; i<count; i++) {
            long id = idOffset + i;
            int hash = Long.hashCode(id);
            Node n = db.nodeMapGet(id, hash);
            assertEquals(id, n.id());
            db.nodeMapRemove(n, hash);
            n.delete(db);
            assertNull(db.nodeMapGet(id, hash));
        }

        db.close();
    }

    @Test
    public void clear() throws Exception {
        final int page  = 512;
        final int size  = 10000;
        final int count = 100000;

        final int idOffset = 1000;

        LocalDatabase db = (LocalDatabase) Database.open
            (new DatabaseConfig()
             .pageSize(page).maxCacheSize(size * page));

        var nodes = new Node[count];
        for (int i=0; i<count; i++) {
            var n = new Node(null, p_callocPage(page));
            long id = idOffset + i;
            n.id(id);
            db.nodeMapPut(n, Long.hashCode(id));
            nodes[i] = n;
        }

        db.nodeMapDeleteAll();

        /*
        for (int i=0; i<count; i++) {
            long id = idOffset + i;
            assertNull(db.nodeMapGet(id, Long.hashCode(id)));
        }
        */

        for (Node n : nodes) {
            assertNull(n.mNodeMapNext);
        }

        db.close();
    }
}
