/*
 *  Copyright (C) 2018 Cojen.org
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

import java.util.ArrayList;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * Tests for ReserveQueue class.
 *
 * @author Brian S O'Neill
 */
public class ReserveQueueTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(ReserveQueueTest.class.getName());
    }

    @Test
    public void basic() throws Exception {
        ReserveQueue<Integer> queue = new ReserveQueue<>(4);

        queue.add(1);
        queue.add(2);
        assertEquals(2, queue.size());
        assertEquals(1, (Object) queue.poll());
        assertEquals(1, queue.size());
        assertEquals(2, (Object) queue.poll());
        assertEquals(0, queue.size());
        assertNull(queue.poll());
        assertEquals(0, queue.size());

        for (int i=0; i<100; i++) {
            queue.add(i);
            if ((i & 1) == 0) {
                int removed = queue.poll();
                assertEquals(i >> 1, removed);
            }
        }
    }

    @Test
    public void reserve() throws Exception {
        ReserveQueue<Integer> queue = new ReserveQueue<>(4);

        queue.add(1);
        int slot = queue.reserve();
        queue.add(3);
        assertEquals(3, queue.size());
        assertEquals(1, (Object) queue.poll());
        assertNull(queue.poll());
        assertEquals(2, queue.size());
        queue.set(slot, 2);
        assertEquals(2, queue.size());
        assertEquals(2, (Object) queue.poll());
        assertEquals(3, (Object) queue.poll());
        assertEquals(0, queue.size());

        queue.clear();
        queue.add(1);
        int slot2 = queue.reserve();
        queue.add(3);
        int slot4 = queue.reserve();
        assertEquals(4, queue.size());
        queue.add(5);
        assertEquals(5, queue.size());
        queue.set(slot4, 4);
        assertEquals(1, (Object) queue.poll());
        assertEquals(4, queue.size());
        assertNull(queue.poll());
        queue.set(slot2, 2);
        for (int i=2; i<=5; i++) {
            assertEquals(i, (Object) queue.poll());
        }
        assertEquals(0, queue.size());
    }

    @Test
    public void scan() throws Exception {
        ReserveQueue<Integer> queue = new ReserveQueue<>(4);

        for (int i=0; i<50; i++) {
            if ((i & 1) == 0) {
                queue.add(i);
            } else {
                queue.reserve();
            }
        }

        int[] lastRef = {-1};
        int[] totalRef = {0};

        queue.scan(i -> {
            assertTrue((i & 1) == 0);
            assertTrue(i > lastRef[0]);
            lastRef[0] = i;
            totalRef[0]++;
        });

        assertEquals(50 >> 1, totalRef[0]);
    }

    @Test
    public void pollIntoArray() throws Exception {
        ReserveQueue<Integer> queue = new ReserveQueue<>(4);

        assertEquals(0, queue.available());

        ArrayList<Integer> slots = new ArrayList<>();

        for (int i=0; i<50; i++) {
            if ((i & 1) != 0) {
                queue.add(i);
            } else {
                slots.add(queue.reserve());
                assertEquals(0, queue.available());
            }
        }

        for (int i=0; i<5; i++) {
            int slot = slots.get(i);
            queue.set(slot, slot);
        }

        assertEquals(10, queue.available());
        Integer[] removed = new Integer[10];
        assertEquals(removed.length, queue.poll(removed, 0, removed.length));
        for (int i=0; i<removed.length; i++) {
            assertEquals(i, (Object) removed[i]);
        }
        assertEquals(40, queue.size());

        for (int i=5; i<slots.size(); i++) {
            int slot = slots.get(i);
            queue.set(slot, slot);
        }

        assertEquals(40, queue.available());
        removed = new Integer[100];
        assertEquals(40, queue.poll(removed, 0, removed.length));
        for (int i=0; i<40; i++) {
            assertEquals(i + 10, (Object) removed[i]);
        }
        assertEquals(0, queue.size());

        assertEquals(0, queue.available());
    }
}
