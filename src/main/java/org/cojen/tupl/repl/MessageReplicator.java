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

/**
 * Message-oriented replication interface, which is a bit easier to use than {@link
 * StreamReplicator}. Although it has slightly higher overhead, control message passing is
 * handled automatically. Replicas are still required to be reading in order for control
 * messages to be processed, however.
 *
 * @author Brian S O'Neill
 * @see StreamReplicator
 */
public interface MessageReplicator extends DirectReplicator {
    /**
     * Open a replicator instance, creating it if necessary.
     *
     * @throws IllegalArgumentException if misconfigured
     */
    public static MessageReplicator open(ReplicatorConfig config) throws IOException {
        // FIXME
        throw null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    Reader newReader(long index, boolean follow);

    /**
     * {@inheritDoc}
     */
    @Override
    Writer newWriter();

    /**
     * {@inheritDoc}
     */
    @Override
    Writer newWriter(long index);

    public static interface Reader extends DirectReplicator.Reader {
        /**
         * Blocks until a log message is available, never reading past a commit index or term.
         *
         * @return complete message or null if the term end has been reached
         * @throws IllegalStateException if log was deleted (index is too low)
         */
        byte[] readMessage() throws IOException;

        /**
         * Blocks until a message is available, and then fully or partially copies it into the
         * given buffer. Messages are partially copied only when the given buffer length is too
         * small. When a message has been fully copied, 0 is returned.
         *
         * @return the amount of bytes remaining in the message, or EOF (-1) if the term end
         * has been reached
         * @throws IllegalStateException if log was deleted (index is too low)
         */
        int readMessage(byte[] buf, int offset, int length) throws IOException;
    }

    public static interface Writer extends DirectReplicator.Writer {
        /**
         * Write a single message to the log.
         *
         * @return false only if the writer is deactivated
         */
        boolean writeMessage(byte[] message) throws IOException;

        /**
         * Write a single message to the log.
         *
         * @return false only if the writer is deactivated
         */
        boolean writeMessage(byte[] message, int offset, int length) throws IOException;

        /**
         * Write a single message to the log, as part of an atomic batch.
         *
         * @param finished pass true for the last message in the batch
         * @return false only if the writer is deactivated
         */
        boolean writeMessage(byte[] message, int offset, int length, boolean finished)
            throws IOException;
    }
}
