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

package org.cojen.tupl.diag;

import java.io.Serializable;

import org.cojen.tupl.Index;

import org.cojen.tupl.core.Utils;

/**
 * Collection of stats from the {@link Index#analyze analyze} method.
 *
 * @author Brian S O'Neill
 */
public class IndexStats implements Cloneable, Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * The estimated number of index entries.
     */
    public double entryCount;

    /**
     * The estimated amount of bytes occupied by keys in the index.
     */
    public double keyBytes;

    /**
     * The estimated amount of bytes occupied by values in the index.
     */
    public double valueBytes;

    /**
     * The estimated amount of free bytes in the index.
     */
    public double freeBytes;

    /**
     * The estimated total amount of bytes in the index.
     */
    public double totalBytes;

    public IndexStats() {
    }

    public IndexStats(double entryCount,
                      double keyBytes,
                      double valueBytes,
                      double freeBytes,
                      double totalBytes)
    {
        this.entryCount = entryCount;
        this.keyBytes = keyBytes;
        this.valueBytes = valueBytes;
        this.freeBytes = freeBytes;
        this.totalBytes = totalBytes;
    } 

    /**
     * Adds stats into a new object.
     */
    public IndexStats add(IndexStats augend) {
        return new IndexStats(entryCount + augend.entryCount,
                              keyBytes + augend.keyBytes,
                              valueBytes + augend.valueBytes,
                              freeBytes + augend.freeBytes,
                              totalBytes + augend.totalBytes);
    }

    /**
     * Subtract stats into a new object.
     */
    public IndexStats subtract(IndexStats subtrahend) {
        return new IndexStats(entryCount - subtrahend.entryCount,
                              keyBytes - subtrahend.keyBytes,
                              valueBytes - subtrahend.valueBytes,
                              freeBytes - subtrahend.freeBytes,
                              totalBytes - subtrahend.totalBytes);
    }

    /**
     * Divide the stats by a scalar into a new object.
     */
    public IndexStats divide(double scalar) {
        return new IndexStats(entryCount / scalar,
                              keyBytes / scalar,
                              valueBytes / scalar,
                              freeBytes / scalar,
                              totalBytes / scalar);
    }

    /**
     * Round the stats to whole numbers into a new object.
     */
    public IndexStats round() {
        return new IndexStats(Math.round(entryCount),
                              Math.round(keyBytes),
                              Math.round(valueBytes),
                              Math.round(freeBytes),
                              Math.round(totalBytes));
    }

    /**
     * Divide the stats by a scalar and round to whole numbers into a new object.
     */
    public IndexStats divideAndRound(double scalar) {
        return new IndexStats(Math.round(entryCount / scalar),
                              Math.round(keyBytes / scalar),
                              Math.round(valueBytes / scalar),
                              Math.round(freeBytes / scalar),
                              Math.round(totalBytes / scalar));
    }

    @Override
    public IndexStats clone() {
        try {
            return (IndexStats) super.clone();
        } catch (CloneNotSupportedException e) {
            throw Utils.rethrow(e);
        }
    }

    @Override
    public int hashCode() {
        long hash = Double.doubleToLongBits(entryCount);
        hash = hash * 31 + Double.doubleToLongBits(keyBytes);
        hash = hash * 31 + Double.doubleToLongBits(valueBytes);
        hash = hash * 31 + Double.doubleToLongBits(freeBytes);
        hash = hash * 31 + Double.doubleToLongBits(totalBytes);
        return (int) Utils.scramble(hash);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj != null && obj.getClass() == IndexStats.class) {
            var other = (IndexStats) obj;
            return entryCount == other.entryCount
                && keyBytes == other.keyBytes
                && valueBytes == other.valueBytes
                && freeBytes == other.freeBytes
                && totalBytes == other.totalBytes;
        }
        return false;
    }

    @Override
    public String toString() {
        var b = new StringBuilder("IndexStats{");

        boolean any = false;
        any = append(b, any, "entryCount", entryCount);
        any = append(b, any, "keyBytes", keyBytes);
        any = append(b, any, "valueBytes", valueBytes);
        any = append(b, any, "freeBytes", freeBytes);
        any = append(b, any, "totalBytes", totalBytes);

        b.append('}');
        return b.toString();
    }

    private static boolean append(StringBuilder b, boolean any, String name, double value) {
        if (!Double.isNaN(value)) {
            if (any) {
                b.append(", ");
            }

            b.append(name).append('=');

            long v = (long) value;
            if (v == value) {
                b.append(v);
            } else {
                b.append(value);
            }
                
            any = true;
        }

        return any;
    }
}
