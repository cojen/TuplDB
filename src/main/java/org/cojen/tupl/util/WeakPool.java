/*
 *  Copyright 2020 Cojen.org
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

package org.cojen.tupl.util;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import java.lang.ref.WeakReference;

import org.cojen.tupl.io.Utils;

/**
 * Utility for sharing a small set of poolable objects (like buffers) which are weakly
 * referenced. Only one thread at a time can access pooled objects, but any number of threads
 * can release pooled objects.
 *
 * @author Brian S O'Neill
 */
public class WeakPool<B> {
    private static final VarHandle cFirstHandle, cLastHandle, cNextHandle;

    static {
        try {
            var lookup = MethodHandles.lookup();
            cFirstHandle = lookup.findVarHandle(WeakPool.class, "mFirst", Entry.class);
            cLastHandle = lookup.findVarHandle(WeakPool.class, "mLast", Entry.class);
            cNextHandle = lookup.findVarHandle(Entry.class, "mNext", Entry.class);
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private volatile Entry<B> mFirst;
    private volatile Entry<B> mLast;

    public WeakPool() {
    }

    /**
     * Returns an available pooled entry if any exist. If an entry is returned but the pooled
     * object is null, discard the entry and try again. A strong reference to the pooled object
     * must be maintained to ensure that it doesn't vanish too soon.
     *
     * <p>Note: This method can only be called by one thread at a time.
     */
    public Entry<B> tryAccess() {
        Entry<B> e = mFirst;

        if (e != null) while (true) {
            Entry<B> next = e.mNext;
            if (next != null) {
                cNextHandle.set(e, null);
                mFirst = next;
                break;
            }
            // Queue is now empty, unless an enqueue is in progress.
            if (mLast == e && cLastHandle.compareAndSet(this, e, null)) {
                cFirstHandle.compareAndSet(this, e, null);
                break;
            }
            Thread.onSpinWait();
        }

        return e;
    }

    /**
     * Create a new pooled entry which refers to the given object. This method is thread-safe.
     */
    public Entry<B> newEntry(B obj) {
        return new Entry<B>(this, obj);
    }

    @SuppressWarnings("unchecked")
    private void release(Entry<B> entry) {
        // Enqueue the entry.
        var prev = (Entry<B>) cLastHandle.getAndSet(this, entry);
        if (prev == null) {
            mFirst = entry;
        } else {
            prev.mNext = entry;
        }
    }

    /**
     * An entry which weakly references a pooled object.
     */
    public static final class Entry<B> extends WeakReference<B> {
        final WeakPool<B> mOwner;
        volatile Entry<B> mNext;

        Entry(WeakPool<B> owner, B obj) {
            super(obj);
            mOwner = owner;
        }

        /**
         * Release the entry such that it can be accessed again later. This method is
         * thread-safe and doesn't need to be called from the thread that accessed the entry.
         */
        public void release() {
            mOwner.release(this);
        }

        /**
         * Discard the entry from the pool instead of releasing it.
         */
        public void discard() {
            // Nothing to do until the pool supports a maximum size.
        }
    }
}
