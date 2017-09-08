/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

import java.io.IOException;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;

import org.cojen.tupl.ext.ReplicationManager;

/**
 * Implementation of a {@link ReplicationManager} for supporting full {@linkplain Database
 * database} replication. Applications shouldn't interact with a {@code DatabaseReplicator}
 * instance directly &mdash; only the database instance is permitted to interact with it.
 *
 * @author Brian S O'Neill
 */
public interface DatabaseReplicator extends Replicator, ReplicationManager {
    /**
     * Open a replicator instance, creating it if necessary. Pass the instance to the {@link
     * DatabaseConfig#replicate DatabaseConfig} object before opening the database.
     *
     * @throws IllegalArgumentException if misconfigured
     */
    public static DatabaseReplicator open(ReplicatorConfig config) throws IOException {
        StreamReplicator streamRepl = StreamReplicator.open(config);
        DatabaseReplicator dbRepl = new DatabaseStreamReplicator(streamRepl);
        return dbRepl;
    }
}
