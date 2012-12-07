/*
 *  Copyright 2012 Brian S O'Neill
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
 * Defines the various types of events that an {@link EventListener
 * EventListener} can receive.
 *
 * @author Brian S O'Neill
 */
public enum EventType {
    /** Signals the beginning of cache initialization. */
    CACHE_INIT_BEGIN(Category.CACHE_INIT),
    /** Signals the end of cache initialization, reporting the duration. */
    CACHE_INIT_COMPLETE(Category.CACHE_INIT),

    /** Signals the beginning of database recovery. */
    RECOVERY_BEGIN(Category.RECOVERY),
    /** Signals that undo logs of in-flight transactions are being loaded. */
    RECOVERY_LOAD_UNDO_LOGS(Category.RECOVERY),
    /** Signals that redo logs of non-checkpointed transactions are being examined. */
    RECOVERY_SCAN_REDO_LOG(Category.RECOVERY),
    /** Signals that non-checkpointed transactions are being committed or rolled back. */
    RECOVERY_APPLY_REDO_LOG(Category.RECOVERY),
    /** Signals that transactions not expliticly committed or rolled back are being processed. */
    RECOVERY_PROCESS_REMAINING(Category.RECOVERY),
    /** Signals that large value fragments in the trash are being deleted. */
    RECOVERY_DELETE_FRAGMENTS(Category.RECOVERY),
    /** Signals the end of database recovery, reporting the duration. */
    RECOVERY_COMPLETE(Category.RECOVERY),

    /** Signals the beginning of a checkpoint. */
    CHECKPOINT_BEGIN(Category.CHECKPOINT),
    /** Signals the checkpoint phase which flushes all dirty nodes to the main database file. */
    CHECKPOINT_FLUSH(Category.CHECKPOINT),
    /** Signals the end of a checkpoint, reporting the duration. */
    CHECKPOINT_COMPLETE(Category.CHECKPOINT),

    /** Signals that an unhandled exception has occurred, and the database must be shutdown. */
    PANIC_UNHANDLED_EXCEPTION(Category.PANIC);

    public final Category category;

    private EventType(Category category) {
        this.category = category;
    }

    /**
     * Event type category.
     *
     * @see EventType
     */
    public enum Category {
        /** Cache initialization allocates memory according to the minimum cache size. */
        CACHE_INIT,

        /** Recovery processes transactions which did not get included in the last checkpoint. */
        RECOVERY,

        /** Checkpoints commit transactional and non-transactional changes to the main database. */
        CHECKPOINT,

        /** A panic indicates that something is wrong with the database and it must be shutdown. */
        PANIC;
    }
}
