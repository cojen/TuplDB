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

package org.cojen.tupl.jmx;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface DatabaseMBean {
    long getFreeBytes();

    long getTotalBytes();

    long getCacheBytes();

    long getDirtyBytes();

    int getOpenIndexes();

    long getLockCount();

    long getCursorCount();

    long getTransactionCount();

    long getCheckpointDuration();

    long getReplicationBacklog();

    boolean isLeader();

    void flush();

    void sync();

    void checkpoint();

    void compactFile(double target);

    void verify();

    boolean failover();
}
