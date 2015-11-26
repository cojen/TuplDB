/*
 *  Copyright 2011-2015 Cojen.org
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
     * Strongest durability mode, which ensures all modifications are persisted to non-volatile
     * storage.
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
