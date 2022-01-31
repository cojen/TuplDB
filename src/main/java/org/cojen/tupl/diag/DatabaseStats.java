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

import org.cojen.tupl.Database;

import org.cojen.tupl.core.Utils;

/**
 * Collection of database {@linkplain Database#stats statistics}.
 *
 * @author Brian S O'Neill
 */
public class DatabaseStats implements Cloneable, Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * The allocation page size
     */
    public int pageSize;

    /**
     * The amount of unused pages in the database.
     */
    public long freePages;

    /**
     * The total amount of pages in the database.
     */
    public long totalPages;

    /**
     * The current size of the cache, in pages.
     */
    public long cachePages;

    /**
     * The count of pages which are dirty (need to be written with a checkpoint).
     */
    public long dirtyPages;

    /**
     * The amount of indexes currently open.
     */
    public int openIndexes;

    /**
     * The amount of locks currently allocated. Locks are created as transactions access or
     * modify records, and they are destroyed when transactions exit or reset. An
     * accumulation of locks can indicate that transactions are not being reset properly.
     */
    public long lockCount;

    /**
     * The amount of cursors which are in a non-reset state. An accumulation of cursors can
     * indicate that they are not being reset properly.
     */
    public long cursorCount;

    /**
     * The amount of fully-established transactions which are in a non-reset state. This
     * value is unaffected by transactions which make no changes, and it is also unaffected
     * by auto-commit transactions. An accumulation of transactions can indicate that they
     * are not being reset properly.
     */
    public long transactionCount;

    /**
     * The time duration required for the last checkpoint to complete, in milliseconds. If
     * no checkpoints are running, then zero is returned.
     */
    public long checkpointDuration;

    /**
     * The amount of log bytes that a replica must apply to be fully caught up to the
     * leader. If the member is currently the leader, then the backlog is zero.
     */
    public long replicationBacklog;

    @Override
    public DatabaseStats clone() {
        try {
            return (DatabaseStats) super.clone();
        } catch (CloneNotSupportedException e) {
            throw Utils.rethrow(e);
        }
    }

    @Override
    public int hashCode() {
        long hash = freePages;
        hash = hash * 31 + totalPages;
        hash = hash * 31 + dirtyPages;
        return (int) Utils.scramble(hash);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj != null && obj.getClass() == DatabaseStats.class) {
            var other = (DatabaseStats) obj;
            return pageSize == other.pageSize
                && freePages == other.freePages
                && totalPages == other.totalPages
                && cachePages == other.cachePages
                && dirtyPages == other.dirtyPages
                && openIndexes == other.openIndexes
                && lockCount == other.lockCount
                && cursorCount == other.cursorCount
                && transactionCount == other.transactionCount
                && checkpointDuration == other.checkpointDuration
                && replicationBacklog == other.replicationBacklog;
        }
        return false;
    }

    @Override
    public String toString() {
        return "DatabaseStats{pageSize=" + pageSize
            + ", freePages=" + freePages
            + ", totalPages=" + totalPages
            + ", cachePages=" + cachePages
            + ", dirtyPages=" + dirtyPages
            + ", openIndexes=" + openIndexes
            + ", lockCount=" + lockCount
            + ", cursorCount=" + cursorCount
            + ", transactionCount=" + transactionCount
            + ", checkpointDuration=" + checkpointDuration
            + ", replicationBacklog=" + replicationBacklog
            + '}';
    }
}
