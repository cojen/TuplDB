/*
 *  Copyright 2019 Cojen.org
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

import java.util.stream.Stream;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.*;

/**
 * Tests against the Split class itself.
 *
 * @author Brian S O'Neill
 */
public class SplitTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SplitTest.class.getName());
    }

    @Test
    public void rebindAndUnrebindLeftLowPos() throws Exception {
        rebindAndUnrebind(false, false);
    }

    @Test
    public void rebindAndUnrebindLeftHighPos() throws Exception {
        rebindAndUnrebind(false, true);
    }

    @Test
    public void rebindAndUnrebindRightLowPos() throws Exception {
        rebindAndUnrebind(true, false);
    }

    @Test
    public void rebindAndUnrebindRightHighPos() throws Exception {
        rebindAndUnrebind(true, true);
    }

    private void rebindAndUnrebind(boolean splitRight, boolean highPos) throws Exception {
        LocalDatabase db = (LocalDatabase) Database.open(new DatabaseConfig());
        BTree ix = (BTree) db.openIndex("test");

        // Fill up a leaf node, just before it has to split, with all possible cursor bindings.

        final int count = 340;
        var keys = new byte[count][];
        var found = new BTreeCursor[count];

        for (int i=0; i<count; i++) {
            byte[] key = key((i + 1) << 1);
            BTreeCursor c = ix.newCursor(null);
            c.find(key);
            c.store(key);
            keys[i] = key;
            found[i] = c;
        }

        Node root = ix.mRoot;
        assertTrue(root.isLeaf());
        assertEquals(4, root.availableBytes());

        var notFound = new BTreeCursor[count + 1];

        for (int i=0; i<=count; i++) {
            byte[] key = key((i << 1) + 1);
            BTreeCursor c = ix.newCursor(null);
            c.find(key);
            assertNull(c.value());
            notFound[i] = c;
        }

        // Verify the cursor bindings.
        int pos = 0;
        for (BTreeCursor c : found) {
            assertEquals(pos, c.mFrame.mNodePos);
            pos += 2;
        }
        pos = 0;
        for (BTreeCursor c : notFound) {
            assertEquals(~pos, c.mFrame.mNodePos);
            pos += 2;
        }

        // Now construct the split state, for a new key in the middle.

        byte[] splitKey = key(count + 1);
        assertFalse(ix.exists(null, splitKey));
        Node sibling = db.allocDirtyNode(NodeGroup.MODE_UNEVICTABLE);
        var split = new Split(splitRight, sibling);
        split.setKey(db, splitKey);

        int splitPos = highPos ? ((count << 1) - 50) : 50;

        if (splitRight) {
            sibling.searchVecStart(root.searchVecStart() + splitPos);
            sibling.searchVecEnd(root.searchVecEnd());
            root.searchVecEnd(root.searchVecEnd() - splitPos);
        } else {
            sibling.searchVecEnd(splitPos);
            root.searchVecStart(root.searchVecStart() + splitPos);
        }

        // Rebind the frames...
        Stream.concat(Stream.of(found), Stream.of(notFound)).forEach(c -> {
            split.rebindFrame(c.mFrame, sibling);
        });

        // Now unrebind them...

        Stream.concat(Stream.of(found), Stream.of(notFound)).forEach(c -> {
            CursorFrame frame = c.mFrame;
            if (frame.mNode == root) {
                split.unrebindOriginalFrame(frame);
            } else {
                split.unrebindSiblingFrame(frame, root);
            }
        });

        // Verify the cursor bindings.
        pos = 0;
        for (BTreeCursor c : found) {
            assertEquals(root, c.mFrame.mNode);
            assertEquals(pos, c.mFrame.mNodePos);
            pos += 2;
        }
        pos = 0;
        for (BTreeCursor c : notFound) {
            assertEquals(root, c.mFrame.mNode);
            assertEquals(~pos, c.mFrame.mNodePos);
            pos += 2;
        }

        sibling.releaseExclusive();
        db.close();
    }

    private static byte[] key(int i) {
        var key = new byte[4];
        Utils.encodeIntBE(key, 0, i);
        return key;
    }
}
