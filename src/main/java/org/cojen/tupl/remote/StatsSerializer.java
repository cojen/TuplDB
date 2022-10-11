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

package org.cojen.tupl.remote;

import java.io.IOException;

import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Serializer;

import org.cojen.tupl.diag.DatabaseStats;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class StatsSerializer implements Serializer<DatabaseStats> {
    static final StatsSerializer THE = new StatsSerializer();

    private StatsSerializer() {
    }

    public void write(Pipe pipe, DatabaseStats stats) throws IOException {
        pipe.writeByte(1); // version
        pipe.writeInt(stats.pageSize);
        pipe.writeLong(stats.freePages);
        pipe.writeLong(stats.totalPages);
        pipe.writeLong(stats.cachePages);
        pipe.writeLong(stats.dirtyPages);
        pipe.writeInt(stats.openIndexes);
        pipe.writeLong(stats.lockCount);
        pipe.writeLong(stats.cursorCount);
        pipe.writeLong(stats.transactionCount);
        pipe.writeLong(stats.checkpointDuration);
        pipe.writeLong(stats.replicationBacklog);
    }

    public DatabaseStats read(Pipe pipe) throws IOException {
        var stats = new DatabaseStats();
        byte version = pipe.readByte(); // ignore for now
        stats.pageSize = pipe.readInt();
        stats.freePages = pipe.readLong();
        stats.totalPages = pipe.readLong();
        stats.cachePages = pipe.readLong();
        stats.dirtyPages = pipe.readLong();
        stats.openIndexes = pipe.readInt();
        stats.lockCount = pipe.readLong();
        stats.cursorCount = pipe.readLong();
        stats.transactionCount = pipe.readLong();
        stats.checkpointDuration = pipe.readLong();
        stats.replicationBacklog = pipe.readLong();
        return stats;
    }
}
