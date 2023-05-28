/*
 *  Copyright (C) 2022 Cojen.org
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

package org.cojen.tupl.rows;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;

import java.util.function.Consumer;

import org.cojen.tupl.util.Latch;

/**
 * Simple cache of weakly or softly referenced values. The keys must not strongly reference the
 * values, or else they won't get GC'd.
 *
 * @author Brian S O'Neill
 */
abstract class RefCache<K, V, H> extends ReferenceQueue<Object> {
    private static final MethodHandle START_VIRTUAL_THREAD;

    static {
        MethodHandle mh;
        if (Runtime.version().feature() < 21) {
            mh = null;
        } else {
            try {
                var mt = MethodType.methodType(Thread.class, Runnable.class);
                mh = MethodHandles.lookup().findStatic(Thread.class, "startVirtualThread", mt);
            } catch (Throwable e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        START_VIRTUAL_THREAD = mh;
    }

    public RefCache() {
        if (START_VIRTUAL_THREAD != null) {
            try {
                var t = (Thread) START_VIRTUAL_THREAD.invokeExact((Runnable) () -> {
                    try {
                        while (true) {
                            Object ref = remove();
                            synchronized (RefCache.this) {
                                cleanup(ref);
                            }
                        }
                    } catch (InterruptedException e) {
                    }
                });
            } catch (Throwable e) {
            }
        }
    }

    public abstract void clear();

    /**
     * Clears the cache and then calls the consumer for each value that was in the cache.
     */
    public abstract void clear(Consumer<V> c);

    /**
     * Can be called without explicit synchronization, but entries can appear to go missing.
     * Double check with synchronization.
     */
    public final V get(K key) {
        Reference<V> ref = getRef(key);
        return ref == null ? null : ref.get();
    }

    /**
     * Can be called without explicit synchronization, but entries can appear to go missing.
     * Double check with synchronization.
     */
    public abstract Reference<V> getRef(K key);

    /**
     * @return a new reference to the value
     */
    public abstract Reference<V> put(K key, V value);

    public abstract void removeKey(K key);

    /**
     * Generates missing entries on demand, using an entry latch to ensure that only one thread
     * does the work. The newValue method must be implemented or else an
     * UnsupportedOperationException can be thrown.
     */
    @SuppressWarnings({"unchecked"})
    public V obtain(K key, H helper) {
        Latch latch;
        while (true) {
            Object value = get(key);

            if (value != null) {
                if (!(value instanceof Latch)) {
                    return (V) value;
                }
                latch = (Latch) value;
            } else {
                synchronized (this) {
                    value = get(key);
                    if (value != null) {
                        if (!(value instanceof Latch)) {
                            return (V) value;
                        }
                        latch = (Latch) value;
                    } else {
                        latch = new Latch(Latch.EXCLUSIVE);
                        put(key, (V) latch);
                        // Break out of the loop and do the work.
                        break;
                    }
                }
            }

            // Wait for another thread to do the work and try again.
            latch.acquireShared();
        }

        V value;
        Throwable ex = null;

        try {
            value = newValue(key, helper);
        } catch (Throwable e) {
            value = null;
            ex = e;
        }

        put: try {
            if (value != null) {
                try {
                    put(key, value);
                    break put;
                } catch (Throwable e) {
                    if (ex == null) {
                        ex = e;
                    } else {
                        ex.addSuppressed(e);
                    }
                }
            }

            removeKey(key);
        } finally {
            latch.releaseExclusive();
        }

        if (ex != null) {
            throw RowUtils.rethrow(ex);
        }

        return value;
    }

    /**
     * Override to support the latched obtain method.
     */
    protected V newValue(K key, H helper) {
        throw new UnsupportedOperationException();
    }

    /**
     * Caller must be synchronized.
     *
     * @param ref not null
     */
    protected abstract void cleanup(Object ref);
}
