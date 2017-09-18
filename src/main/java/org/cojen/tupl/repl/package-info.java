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

/**
 * General-purpose <a href="https://raft.github.io/">Raft-based</a> replication system,
 * intended for supporting fully replicated {@linkplain org.cojen.tupl.Database databases}.
 * Use {@link org.cojen.tupl.repl.DatabaseReplicator DatabaseReplicator} for enabling database
 * replication. Applications can use {@link org.cojen.tupl.repl.MessageReplicator
 * MessageReplicator} or {@link org.cojen.tupl.repl.StreamReplicator StreamReplicator} for
 * direct replication.
 */
package org.cojen.tupl.repl;
