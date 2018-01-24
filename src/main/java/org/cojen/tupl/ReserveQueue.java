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

import java.util.Arrays;
import java.util.Objects;

import java.util.function.Consumer;

/**
 * A growable, array backed queue, which supports slot reservations. Elements at unset reserved
 * slots cannot be removed from the queue.
 *
 * @author Brian S O'Neill
 */
class ReserveQueue<E> {
    private E[] mElements;
    private int[] mReservations;
    private int mHead; // inclusive
    private int mTail; // exclusive
    private int mSize;

    @SuppressWarnings("unchecked")
    ReserveQueue(int initialCapacity) {
        mElements = (E[]) new Object[initialCapacity];
        mReservations = new int[initialCapacity];
    }

    /**
     * @return total size, including reserved slots
     */
    public final int size() {
        return mSize;
    }

    /**
     * @return reserved slot; to be passed to the set method
     */
    @SuppressWarnings("unchecked")
    public final int reserve() {
        E[] elements = mElements;
        int size = mSize;

        if (size >= elements.length) {
            // Expand the capacity.

            E[] newElements = (E[]) new Object[elements.length << 1];
            int[] newReservations = new int[newElements.length];

            // Store reverse lookups in the new array, for convenience. These entries will be
            // discarded as new reservations are made.
            for (int i=0; i<mReservations.length; i++) {
                newReservations[mReservations.length + mReservations[i]] = i;
            }

            int head = mHead;
            int i = 0;
            for (; i<elements.length; i++) {
                E obj = elements[head];
                newElements[i] = obj;
                // Use the reverse lookup to remap unfilled reservations.
                newReservations[newReservations[mReservations.length + head]] = i;
                head++;
                if (head >= elements.length) {
                    head = 0;
                }
            }

            mHead = 0;
            mTail = elements.length;
            mElements = elements = newElements;
            mReservations = newReservations;
        }

        mSize = size + 1;
        int slot = mTail;
        int tail = slot + 1;
        if (tail >= elements.length) {
            tail = 0;
        }
        mTail = tail;

        // Store the reservation in case the capacity needs to be increased later.
        mReservations[slot] = slot;

        return slot;
    }

    /**
     * @param slot provided by the reserve method
     * @param obj cannot be null
     */
    public final void set(int slot, E obj) {
        mElements[mReservations[slot]] = Objects.requireNonNull(obj);
    }

    /**
     * @param obj cannot be null
     */
    public final void add(E obj) {
        Objects.requireNonNull(obj);
        set(reserve(), obj);
    }

    /**
     * @return the amount of elements that can be removed
     */
    public final int available() {
        int count = 0;
        E[] elements = mElements;
        for (int i=mSize, head=mHead; --i>=0; ) {
            if (elements[head] == null) {
                break;
            }
            count++;
            head++;
            if (head >= elements.length) {
                head = 0;
            }
        }
        return count;
    }

    /**
     * @return null if nothing can be removed
     */
    public final E poll() {
        int size = mSize;
        if (size > 0) {
            E[] elements = mElements;
            int head = mHead;
            E obj = elements[head];
            if (obj != null) {
                elements[head] = null;
                mSize = size - 1;
                head++;
                if (head >= elements.length) {
                    head = 0;
                }
                mHead = head;
                return obj;
            }
        }
        return null;
    }

    /**
     * Remove into the given array, limited by the amount available.
     *
     * @return actual count removed
     */
    public final int poll(E[] removed, int offset, int count) {
        E[] elements = mElements;
        int head = mHead;
        for (int i=0; i<count; i++) {
            E element = elements[head];
            if (element == null) {
                count = i;
                break;
            }
            elements[head] = null;
            removed[offset++] = element;
            head++;
            if (head >= elements.length) {
                head = 0;
            }
        }
        mSize -= count;
        mHead = head;
        return count;
    }

    public final void clear() {
        Arrays.fill(mElements, mHead, mTail, null);
        mHead = 0;
        mTail = 0;
        mSize = 0;
    }

    /**
     * Scan over all of the filled-in slots.
     */
    public final void scan(Consumer<? super E> consumer) {
        E[] elements = mElements;
        int head = mHead;
        for (int i=mSize; --i>=0; ) {
            E obj = elements[head];
            if (obj != null) {
                consumer.accept(obj);
            }
            head++;
            if (head >= elements.length) {
                head = 0;
            }
        }
    }

    @Override
    public final String  toString() {
        StringBuilder b = new StringBuilder().append('[');

        scan(obj -> {
            if (b.length() != 1) {
                b.append(", ");
            }
            b.append(obj);
        });

        return b.append(']').toString();
    }
}
