/*
 *  Copyright (C) 2018 Cojen.org
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

import java.util.function.LongConsumer;

/**
 * 
 *
 * @author Brian S O'Neill
 */
interface LogAccessor extends Replicator.Accessor {
    /**
     * Returns the term at the previous accessor index.
     */
    long prevTerm();

    /**
     * Invokes the given task when the commit index reaches the requested index. The current
     * commit index is passed to the task, or -1 if the term ended before the index could be
     * reached. If the task can be run when this method is called, then the current thread
     * invokes it.
     */
    void uponCommit(CommitCallback task);

    @Override
    default void uponCommit(long position, LongConsumer task) {
        uponCommit(new CommitCallback(position) {
            @Override
            public void reached(long position) {
                task.accept(position);
            }
        });
    }

    /**
     * Indicate that the accessor isn't intended to be used again, allowing file handles to be
     * closed. Accessing again will reopen them.
     */
    void release();
}
