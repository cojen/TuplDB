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
        final int size  = 10000;
        final int count = 100000;

        NodeMap map = new NodeMap(size);

        for (int i=0; i<count; i++) {
            Node n = new Node(null, 10);
            n.mId = i;
            map.put(n, NodeMap.hash(i));
        }

        for (int i=0; i<count; i++) {
            Node n = map.get(i, NodeMap.hash(i));
            assertEquals(i, n.mId);
        }

        for (int i=0; i<count; i++) {
            int hash = NodeMap.hash(i);
            Node n = map.get(i, hash);
            assertEquals(i, n.mId);
            map.remove(n, hash);
            assertNull(map.get(i, hash));
        }

        map.delete();
    }

    @Test
    public void clear() throws Exception {
        final int size  = 10000;
        final int count = 100000;

        NodeMap map = new NodeMap(size);

        Node[] nodes = new Node[count];
        for (int i=0; i<count; i++) {
            Node n = new Node(null, 10);
            n.mId = i;
            map.put(n, NodeMap.hash(i));
            nodes[i] = n;
        }

        map.delete();

        for (int i=0; i<count; i++) {
            assertNull(map.get(i, NodeMap.hash(i)));
        }

        for (Node n : nodes) {
            assertNull(n.mNodeChainNext);
        }
    }
}
