/*
 *  Copyright (C) 2023 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.util;

import java.lang.invoke.VarHandle;

import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;

/**
 * Implements a specialized cache for supporting object canonicalization. It behaves like the
 * {@link String#intern String.intern} method, but this class supports any kind of object.
 *
 * <p>Objects that do not customize the {@code hashCode} and {@code equals} methods don't make
 * sense to be canonicalized because each instance will be considered unique. The object
 * returned from the {@code apply} method will always be the same as the one passed in.
 *
 * @author Brian S O'Neill
 */
public final class Canonicalizer {
    private final ReferenceQueue<Object> mQueue;

    private Entry[] mEntries;
    private int mSize;

    public Canonicalizer() {
        mQueue = new ReferenceQueue<Object>();

        // Initial capacity must be a power of 2.
        mEntries = new Entry[2];
    }

    /**
     * Returns the original object or another one which is equal.
     */
    @SuppressWarnings({"unchecked"})
    public <A> A apply(A obj) {
        Object ref = mQueue.poll();
        if (ref != null) {
            cleanup(ref);
        }

        Class<?> objClass = obj.getClass();
        int hash = obj.hashCode();

        var entries = mEntries;
        for (Entry e = entries[hash & (entries.length - 1)]; e != null; e = e.mNext) {
            Object existing = e.get();
            if (existing != null && objClass == existing.getClass() && obj.equals(existing)) {
                return (A) existing;
            }
        }

        synchronized (this) {
            entries = mEntries;
            int slot = hash & (entries.length - 1);
            for (Entry e = entries[slot]; e != null; e = e.mNext) {
                Object existing = e.get();
                if (existing != null && objClass == existing.getClass() && obj.equals(existing)) {
                    return (A) existing;
                }
            }

            int size = mSize;

            if ((size + (size >> 1)) >= entries.length && entries.length < (1 << 30)) {
                // Rehash.
                var newEntries = new Entry[entries.length << 1];
                for (int i=0; i<entries.length; i++) {
                    for (var e = entries[i]; e != null; ) {
                        Entry next = e.mNext;
                        slot = e.mHash & (newEntries.length - 1);
                        e.mNext = newEntries[slot];
                        newEntries[slot] = e;
                        e = next;
                    }
                }
                mEntries = entries = newEntries;
                slot = hash & (entries.length - 1);
            }

            var newEntry = new Entry(obj, hash, mQueue);
            newEntry.mNext = entries[slot];
            VarHandle.storeStoreFence(); // ensure that entry object is safely visible
            entries[slot] = newEntry;
            mSize++;

            return obj;
        }
    }

    synchronized int size() {
        return mSize;
    }

    private void cleanup() throws InterruptedException {
        cleanup(mQueue.remove());
    }

    /**
     * @param ref not null
     */
    private synchronized void cleanup(Object ref) {
        var entries = mEntries;
        do {
            var cleared = (Entry) ref;
            int ix = cleared.mHash & (entries.length - 1);
            for (Entry e = entries[ix], prev = null; e != null; e = e.mNext) {
                if (e == cleared) {
                    if (prev == null) {
                        entries[ix] = e.mNext;
                    } else {
                        prev.mNext = e.mNext;
                    }
                    mSize--;
                    break;
                } else {
                    prev = e;
                }
            }
        } while ((ref = mQueue.poll()) != null);
    }

    private static final class Entry extends WeakReference<Object> {
        final int mHash;
        Entry mNext;

        Entry(Object obj, int hash, ReferenceQueue<Object> queue) {
            super(obj, queue);
            mHash = hash;
        }
    }
}
