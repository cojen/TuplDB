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

import org.cojen.tupl.util.Latch;

/**
 * Cache which makes missing entries on demand and uses an entry latch to ensure that only one
 * thread does the work.
 *
 * @author Brian S O'Neill
 */
abstract class SoftLatchedCache<K, V, H> extends SoftCache<K, Object> {
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
                        put(key, latch);
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

    protected abstract V newValue(K key, H helper);
}
