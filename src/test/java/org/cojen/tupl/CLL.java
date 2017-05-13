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

import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import java.util.function.Consumer;

/**
 * Simple concurrent linked list example.
 *
 * @author Brian S O'Neill
 */
@org.junit.Ignore
public class CLL {
    public static void main(String[] args) throws Exception {
        final int size = 10_000_000;

        for (int i=1; i<=32; i += 1) {
            System.out.println("----------");
            System.out.println("thread count: " + i);
            long start = System.currentTimeMillis();
            CLL queue = test(size, i);
            long end = System.currentTimeMillis();
            System.out.println("duration: " + (end - start));

            int expect = (size / i) * i;

            int removed = (size / i / 2) * i;
            expect -= removed;

            int actual = queue.verify();
            System.out.println("verify: " + actual + ", " + expect);

            if (actual != expect) {
                throw new AssertionError();
            }

            //queue.iterate((n) -> System.out.println(n));

            queue = null;
            System.gc();
            Thread.sleep(1000);
        }
    }

    private static CLL test(int size, int numThreads) throws Exception {
        CLL queue = new CLL();

        {
            Node n1 = new Node();
            Node n2 = new Node();
            queue.add(n1);
            queue.add(n2);
            queue.remove(n1);
            queue.remove(n2);
        }

        Thread[] threads = new Thread[numThreads];
        int count = size / numThreads;

        for (int i=0; i<threads.length; i++) {
            threads[i] = new Thread(() -> {
                for (int j=0; j<count; j++) {
                    Node n = new Node();
                    queue.add(n);
                    if ((j & 1) == 1) {
                        //Thread.yield();
                        queue.remove(n);
                    }
                }
            });
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            //System.out.println(t);
            t.join();
        }

        return queue;
    }

    private static final AtomicReferenceFieldUpdater<CLL, Node> cLastUpdater =
        AtomicReferenceFieldUpdater.newUpdater(CLL.class, Node.class, "mLast");

    private static final AtomicReferenceFieldUpdater<Node, Node> cPrevUpdater =
        AtomicReferenceFieldUpdater.newUpdater(Node.class, Node.class, "mPrev");

    private static final int SPIN_LIMIT = Runtime.getRuntime().availableProcessors();

    private volatile Node mLast;

    public final void add(Node node) {
        // Next is set to self to indicate that the node is the last.
        node.lazySet(node);

        int trials = 0;
        while (true) {
            Node last = mLast;
            cPrevUpdater.lazySet(node, last);
            if (last == null) {
                if (cLastUpdater.compareAndSet(this, null, node)) {
                    return;
                }
            } else if (last.get() == last) {
                if (last.compareAndSet(last, node)) {
                    // Catch up before replacing last node reference.
                    while (mLast != last);
                    cLastUpdater.lazySet(this, node);
                    return;
                }
            }

            trials++;

            if (trials >= SPIN_LIMIT) {
                // Spinning too much due to high contention. Back off a tad.
                Thread.yield();
                trials = 0;
            }
        }
    }

    /**
     * Removes the given node, which is expected to be in the queue. After removal, the node
     * next pointer is null, but the previous pointer is unmodified. Concurrent iteration works
     * as long as the node isn't recycled back into the queue.
     */
    public final void remove(Node node) {
        // Design note: The node to remove is effectively locked by cas'ng the next reference
        // to null. Null means locked. The previous node is then updated to skip over the node
        // being removed. It can only do so when its next reference matches the node to be
        // removed. Because the next reference of the last node is itself, this state can be
        // distinguished from the locked state.

        int trials = 0;
        while (true) {
            Node n = node.get();

            if (n == null) {
                // Not in the queue.
                return;
            }

            if (n == node) {
                // Removing the last node.
                if (node.compareAndSet(n, null)) {
                    // Update previous node to be the new last node.
                    Node p;
                    do {
                        p = node.mPrev;
                    } while  (p != null && (p.get() != node || !p.compareAndSet(node, p)));
                    // Catch up before replacing last node reference.
                    while (mLast != node);
                    cLastUpdater.lazySet(this, p);
                    return;
                }
            } else {
                // Removing an interior or first node.
                if (n.mPrev == node && node.compareAndSet(n, null)) {
                    // Update next reference chain to skip over the removed node.
                    Node p;
                    do {
                        p = node.mPrev;
                    } while (p != null && (p.get() != node || !p.compareAndSet(node, n)));
                    // Update previous reference chain to skip over the removed node.
                    cPrevUpdater.lazySet(n, p);
                    return;
                }
            }

            trials++;

            if (trials >= SPIN_LIMIT) {
                // Spinning too much due to high contention. Back off a tad.
                Thread.yield();
                trials = 0;
            }
        }
    }

    public final void iterate(Consumer<Node> consumer) {
        Node node = mLast;
        while (node != null) {
            consumer.accept(node);
            node = node.mPrev;
        }
    }

    public int verify() {
        int count = 0;

        Node node = mLast;

        if (node != null) {
            if (node.get() != node) {
                throw new AssertionError();
            }

            while (true) {
                count++;
                Node prev = node.mPrev;
                if (prev == null) {
                    break;
                }
                if (prev.get() != node) {
                    System.out.println("count: " + count);
                    System.out.println("node:  " + node);
                    System.out.println("n:     " + node.get());
                    System.out.println("n.p:   " + node.get().mPrev);
                    //System.out.println("p.removed: " + prev.mRemoved);
                    System.out.println("p:     " + prev);
                    System.out.println("p.n:   " + prev.get());
                    //System.out.println("p.n.p: " + prev.get().mPrev);
                    System.out.println("p.p:   " + prev.mPrev.mPrev);
                    System.out.println("p.p.n: " + prev.mPrev.get());
                    throw new AssertionError();
                }
                node = prev;
            }
        }

        return count;
    }

    /**
     * Atomic reference is to the next node in the chain.
     */
    public static final class Node extends AtomicReference<Node> {
        volatile Node mPrev;

        @Override
        public String toString() {
            return getClass().getName() + '@' + Integer.toHexString(hashCode());
        }
    }
}
