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

import java.util.logging.Level;

/**
 * Defines the various types of events that an {@link EventListener
 * EventListener} can receive.
 *
 * @author Brian S O'Neill
 */
public enum EventType {
    /** General debug event type. */
    DEBUG(Category.DEBUG, Level.INFO),

    /** Signals the beginning of cache initialization. */
    CACHE_INIT_BEGIN(Category.CACHE_INIT, Level.INFO),
    /** Signals the end of cache initialization, reporting the duration. */
    CACHE_INIT_COMPLETE(Category.CACHE_INIT, Level.INFO),

    /** Signals the beginning of database recovery. */
    RECOVERY_BEGIN(Category.RECOVERY, Level.INFO),
    /** Signals that automatic cache priming is being performed. */
    RECOVERY_CACHE_PRIMING(Category.RECOVERY, Level.INFO),
    /** Generic recovery progress message. */
    RECOVERY_PROGRESS(Category.RECOVERY, Level.INFO),
    /** Signals that undo logs of in-flight transactions are being loaded. */
    RECOVERY_LOAD_UNDO_LOGS(Category.RECOVERY, Level.INFO),
    /** Signals that non-checkpointed transactions are being committed or rolled back. */
    RECOVERY_APPLY_REDO_LOG(Category.RECOVERY, Level.INFO),
    /** Signals that some redo operations were not recovered due to log file corruption. */
    RECOVERY_REDO_LOG_CORRUPTION(Category.RECOVERY, Level.WARNING),
    /** Signals that transactions not expliticly committed or rolled back are being processed. */
    RECOVERY_PROCESS_REMAINING(Category.RECOVERY, Level.INFO),
    /** Signals that large value fragments in the trash are being deleted. */
    RECOVERY_DELETE_FRAGMENTS(Category.RECOVERY, Level.INFO),
    /** Signals the end of database recovery, reporting the duration. */
    RECOVERY_COMPLETE(Category.RECOVERY, Level.INFO),

    /** Signals that deletion of an index has begun. */
    DELETION_BEGIN(Category.DELETION, Level.INFO),
    /** Signals that deletion of an index has failed. */
    DELETION_FAILED(Category.DELETION, Level.WARNING),
    /** Signals that deletion of an index has completed. */
    DELETION_COMPLETE(Category.DELETION, Level.INFO),

    /** Generic warning message from the replication system. */
    REPLICATION_WARNING(Category.REPLICATION, Level.WARNING),
    /** Unhandled in the replication system, and the database must be shutdown. */
    REPLICATION_PANIC(Category.REPLICATION, Level.SEVERE),

    /** Signals the beginning of a checkpoint. */
    CHECKPOINT_BEGIN(Category.CHECKPOINT, Level.INFO),
    /** Signals the checkpoint phase which flushes all dirty nodes to the main database file. */
    CHECKPOINT_FLUSH(Category.CHECKPOINT, Level.INFO),
    /** Signals the checkpoint phase which forcibly persists changes to the main database file. */
    CHECKPOINT_SYNC(Category.CHECKPOINT, Level.INFO),
    /** Signals that checkpoint task failed with an exception. */
    CHECKPOINT_FAILED(Category.CHECKPOINT, Level.WARNING),
    /** Signals the end of a checkpoint, reporting the duration. */
    CHECKPOINT_COMPLETE(Category.CHECKPOINT, Level.INFO),

    /** Signals that an unhandled exception has occurred, and the database must be shutdown. */
    PANIC_UNHANDLED_EXCEPTION(Category.PANIC, Level.SEVERE);

    public final Category category;
    public final Level level;

    private EventType(Category category, Level level) {
        this.category = category;
        this.level = level;
    }

    /**
     * Event type category.
     *
     * @see EventType
     */
    public enum Category {
        /** General debug category. */
        DEBUG,

        /** Cache initialization allocates memory according to the minimum cache size. */
        CACHE_INIT,

        /** Recovery processes transactions which did not get included in the last checkpoint. */
        RECOVERY,

        /** Index deletion event category. */
        DELETION,

        /** Event category for replication tasks performed by background threads. */
        REPLICATION,

        /** Checkpoints commit transactional and non-transactional changes to the main database. */
        CHECKPOINT,

        /** A panic indicates that something is wrong with the database and it must be shutdown. */
        PANIC;
    }
}
