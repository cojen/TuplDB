/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl;

/**
 * Various {@link Transaction transaction} durability modes, which control the durability
 * strength of committed transactions. Strong modes offer safety, but they are also relatively
 * slow. Weak modes are faster, but transactions committed in one of these modes can get lost.
 *
 * <p>Modes ordered from strongest to weakest:
 * <ul>
 * <li>{@link #SYNC} (default)
 * <li>{@link #NO_SYNC}
 * <li>{@link #NO_FLUSH}
 * <li>{@link #NO_REDO}
 * </ul>
 *
 * @author Brian S O'Neill
 * @see DatabaseConfig#durabilityMode
 */
public enum DurabilityMode {
    /**
     * Strongest durability mode, which ensures all modifications are unlikely to be lost in
     * the event of a sudden power failure or crash. Typically this requires that all data be
     * persisted to non-volatile storage. If the database is using a replication manager, it
     * might only guarantee that enough replicas have committed modifications to volatile
     * memory, and so the {@link Database#sync sync} method would also need to be called to
     * acheive stronger durability.
     */
    SYNC,

    /**
     * Durability mode which permits the operating system to lazily persist modifications to
     * non-volatile storage. This mode is vulnerable to power failures, operating system
     * crashes, and replication failures. Any of these events can cause recently committed
     * transactions to get lost.
     */
    NO_SYNC,

    /**
     * Durability mode which writes modifications to the file system when the in-process buffer
     * is full, or by automatic checkpoints. In addition to the vulnerabilities of NO_SYNC
     * mode, NO_FLUSH mode can lose recently committed transactions when the process crashes.
     * When the process exits cleanly, a shutdown hook switches this mode to behave like
     * NO_SYNC and flushes the log.
     */
    NO_FLUSH,

    /**
     * Weakest durability mode, which doesn't write anything to the redo log or replication
     * manager. An unlogged transaction does not become durable until a checkpoint is
     * performed. In addition to the vulnerabilities of NO_FLUSH mode, NO_REDO mode can lose
     * recently committed transactions when the process exits.
     */
    NO_REDO;

    DurabilityMode alwaysRedo() {
        return this == NO_REDO ? NO_FLUSH : this;
    }
}
