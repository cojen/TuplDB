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
 * StreamReplicator}. Although this replicator has slightly higher overhead, control message
 * passing is handled automatically. Replicas are still required to be reading in order for
 * control messages to be processed, however.
 *
 * @author Brian S O'Neill
 * @see StreamReplicator
 */
public interface MessageReplicator extends Replicator {
    /**
     * Open a MessageReplicator instance, creating it if necessary. Be sure to call the {@link
     * #start start} method too.
     *
     * @throws IllegalArgumentException if misconfigured
     */
    public static MessageReplicator open(ReplicatorConfig config) throws IOException {
        return new MessageStreamReplicator(StreamReplicator.open(config));
    }

    /**
     * {@inheritDoc}
     * @throws InvalidReadException if position is lower than the start position
     */
    @Override
    Reader newReader(long position, boolean follow);

    /**
     * {@inheritDoc}
     * @throws IllegalStateException if an existing writer for the current term already exists
     */
    @Override
    Writer newWriter();

    /**
     * {@inheritDoc}
     * @throws IllegalStateException if an existing writer for the current term already exists
     */
    @Override
    Writer newWriter(long position);

    /**
     * Interface called by any group member for reading committed messages. Readers don't track
     * which messages are applied &mdash; applications are responsible for tracking the highest
     * applied position. When an application restarts, it must open the reader at an appropriate
     * position.
     *
     * @see MessageReplicator#newReader newReader
     */
    public static interface Reader extends Replicator.Reader {
        /**
         * Blocks until a log message is available, never reading past a commit position or term.
         *
         * @return complete message or null if the term end has been reached
         * @throws IllegalStateException if a partially read message remains
         * @throws InvalidReadException if log was deleted (position is too low)
         */
        byte[] readMessage() throws IOException;

        /**
         * Blocks until a message is available, and then fully or partially copies it into the
         * given buffer. Messages are partially copied only when the given buffer length is too
         * small. When a message has been fully copied, a positive length is returned. A
         * negative return value indicates the amount of bytes remaining in the message.
         * Compute the ones' complement (~) to determine the actual amount remaining. If the
         * amount remaining is 0 (or -1 when not complemented), then the term end has been
         * reached.
         *
         * @return message length if positive, or the amount of bytes remaining in the message
         * (ones' complement), or EOF (-1) if the term end has been reached
         * @throws InvalidReadException if log was deleted (position is too low)
         */
        int readMessage(byte[] buf, int offset, int length) throws IOException;
    }

    /**
     * Interface called by the group leader for proposing messages. When consensus has been
     * reached, the messages are committed and become available for all members to read.
     *
     * @see MessageReplicator#newWriter newWriter
     */
    public static interface Writer extends Replicator.Writer {
        /**
         * Write a single message to the log. Equivalent to: {@code writeMessage(message, 0,
         * message.length, true)}
         *
         * @return 1 if successful, -1 if fully deactivated, or 0 if should write any remaining
         * messages and then close the writer
         */
        default int writeMessage(byte[] message) throws IOException {
            return writeMessage(message, 0, message.length, true);
        }

        /**
         * Write a single message to the log. Equivalent to: {@code writeMessage(message,
         * offset, length, true)}
         *
         * @return 1 if successful, -1 if fully deactivated, or 0 if should write any remaining
         * messages and then close the writer
         */
        default int writeMessage(byte[] message, int offset, int length) throws IOException {
            return writeMessage(message, offset, length, true);
        }

        /**
         * Write a single message to the log, as part of an atomic batch. When read back, all
         * messages in the batch are still separate from each other. Pass false to the finished
         * parameter for all messages in the batch except the last one. To work correctly, no
         * other threads should be granted access to the writer in the middle of a batch.
         *
         * @param finished pass true for the last message in the batch
         * @return 1 if successful, -1 if fully deactivated, or 0 if should write any remaining
         * messages and then close the writer
         */
        int writeMessage(byte[] message, int offset, int length, boolean finished)
            throws IOException;
    }
}
