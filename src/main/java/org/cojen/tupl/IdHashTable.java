/*
 *  Copyright 2012-2013 Brian S O'Neill
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

import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Partitioned hash table with node id keys. Caller should use scrambled id to
 * avoid collisions.
 *
 * @author Brian S O'Neill
 */
final class IdHashTable<V> {
    private final int mInitalSegmentCapacity;
    private final AtomicReferenceArray<LHashTable.Obj<V>> mSegments;
    private final long mSegmentShift;

    IdHashTable(int capacity) {
        this(capacity, Runtime.getRuntime().availableProcessors() * 4);
    }

    IdHashTable(int capacity, int segments) {
        segments = Utils.roundUpPower2(Math.max(2, segments));
        mInitalSegmentCapacity = capacity / segments;
        mSegments = new AtomicReferenceArray<>(segments);
        mSegmentShift = Long.numberOfLeadingZeros(segments - 1);
    }

    /**
     * @return null if not found
     */
    final V get(long id) {
        LHashTable.Obj<V> segment = mSegments.get((int) (id >>> mSegmentShift));
        if (segment == null) {
            return null;
        }
        synchronized (segment) {
            LHashTable.ObjEntry<V> e = segment.get(id);
            return e == null ? null : e.value;
        }
    }

    final void put(long id, V value) {
        int i = (int) (id >>> mSegmentShift);

        LHashTable.Obj<V> segment = mSegments.get(i);
        while (segment == null) {
            segment = new LHashTable.Obj<>(mInitalSegmentCapacity);
            if (mSegments.compareAndSet(i, null, segment)) {
                break;
            }
            segment = mSegments.get(i);
        }

        synchronized (segment) {
            segment.insert(id).value = value;
        }
    }

    final V remove(long id) {
        LHashTable.Obj<V> segment = mSegments.get((int) (id >>> mSegmentShift));
        if (segment == null) {
            return null;
        }
        synchronized (segment) {
            LHashTable.ObjEntry<V> e = segment.remove(id);
            return e == null ? null : e.value;
        }
    }
}
