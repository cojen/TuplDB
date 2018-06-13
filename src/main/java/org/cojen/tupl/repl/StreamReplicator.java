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

import java.io.EOFException;
import java.io.File;
import java.io.IOException;

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;

import java.util.Collections;
import java.util.Set;

import java.util.function.Consumer;

/**
 * Low-level replication interface, which recives messages in a non-delineated stream.
 * Applications using this interface are responsible for encoding messages such that they can
 * be properly separated. Consider an application which writes these two messages (inside the
 * quotes): {@code ["hello", "world"]}. The messages might be read back as {@code ["hello",
 * "world"]}, {@code ["helloworld"]}, {@code ["he", "llowor", "ld"]}, etc.
 *
 * <p>For ensuring that messages aren't torn in the middle when a new leader is elected,
 * messages must be written into the replicator with properly defined boundaries. When writing
 * {@code ["hello", "world"]}, a leader election can cause the second message to be dropped,
 * and then only {@code ["hello"]} is read. If {@code ["helloworld"]} was written, no tearing
 * of the two words can occur. They might both be read or both be dropped, atomically.
 *
 * <p>StreamReplicators require that a control message {@link #controlMessageAcceptor acceptor}
 * be installed for supporting group membership changes. Consider using {@link
 * MessageReplicator} instead, although it has slightly higher overhead. In order for control
 * messages to be processed, replicas must be actively reading and calling {@link
 * #controlMessageReceived controlMessageReceived} as necessary.
 *
 * @author Brian S O'Neill
 * @see MessageReplicator
 */
public interface StreamReplicator extends DirectReplicator {
    /**
     * Open a replicator instance, creating it if necessary.
     *
     * @throws IllegalArgumentException if misconfigured
     */
    public static StreamReplicator open(ReplicatorConfig config) throws IOException {
        if (config == null) {
            throw new IllegalArgumentException("No configuration");
        }

        File base = config.mBaseFile;
        if (base == null) {
            throw new IllegalArgumentException("No base file configured");
        }

        if (base.isDirectory()) {
            throw new IllegalArgumentException("Base file is a directory: " + base);
        }

        long groupToken = config.mGroupToken;
        if (groupToken == 0) {
            throw new IllegalArgumentException("No group token configured");
        }

        SocketAddress localAddress = config.mLocalAddress;
        if (localAddress == null) {
            throw new IllegalArgumentException("No local address configured");
        }

        SocketAddress listenAddress = config.mListenAddress;
        ServerSocket localSocket = config.mLocalSocket;

        if (listenAddress == null) {
            listenAddress = localAddress;
            if (listenAddress instanceof InetSocketAddress) {
                int port = ((InetSocketAddress) listenAddress).getPort();
                listenAddress = new InetSocketAddress(port);
            }
        }

        if (localSocket == null) {
            // Attempt to bind the socket early, failing if the port is in use. This prevents
            // joining the group early, which can cause an existing member's role to be
            // downgraded to observer.
            localSocket = ChannelManager.newServerSocket(listenAddress);
        }

        Set<SocketAddress> seeds = config.mSeeds;

        if (seeds == null) {
            seeds = Collections.emptySet();
        }

        if (config.mMkdirs) {
            base.getParentFile().mkdirs();
        }

        return Controller.open(config.mEventListener,
                               new FileStateLog(base), groupToken,
                               new File(base.getPath() + ".group"), 
                               localAddress, listenAddress, config.mLocalRole,
                               seeds, localSocket);
    }

    /**
     * {@inheritDoc}
     * @throws IllegalStateException if index is lower than the start index
     */
    @Override
    Reader newReader(long index, boolean follow);

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
    Writer newWriter(long index);

    /**
     * Called to pass along a control message, which was originally provided through an {@link
     * #controlMessageAcceptor acceptor}. Control messages must be passed along in the original
     * order in which they were created. A control message cannot be treated as applied until
     * after this method returns.
     *
     * @param index log index just after the message
     */
    void controlMessageReceived(long index, byte[] message) throws IOException;

    /**
     * Install a callback to be invoked when the replicator needs to send control messages,
     * which must propagate through the replication log. From the perspective of the acceptor,
     * control messages should be treated as opaque. Control messages are primarily used for
     * supporting group membership changes, and without an acceptor, members cannot be added or
     * removed.
     *
     * <p>Acceptor implementations are expected to wrap messages such that they can be
     * propagated along with regular messages, and then later be passed to the {@link
     * #controlMessageReceived controlMessageReceived} method. If a control message cannot be
     * written (possibly because the local member isn't the leader), it might be silently
     * dropped. Implementations are not required to pass control messages to a remote leader.
     *
     * @param acceptor acceptor to use, or pass null to disable
     */
    void controlMessageAcceptor(Consumer<byte[]> acceptor);

    /**
     * Interface called by any group member for reading committed messages. Readers don't track
     * which messages are applied &mdash; applications are responsible for tracking the highest
     * applied index. When an application restarts, it must open the reader at an appropriate
     * index.
     *
     * @see StreamReplicator#newReader newReader
     */
    public static interface Reader extends DirectReplicator.Reader {
        /**
         * Blocks until log messages are available, never reading past a commit index or term.
         *
         * @return amount of bytes read, or EOF (-1) if the term end has been reached
         * @throws IllegalStateException if log was deleted (index is too low)
         */
        default int read(byte[] buf) throws IOException {
            return read(buf, 0, buf.length);
        }

        /**
         * Blocks until log messages are available, never reading past a commit index or term.
         *
         * @return amount of bytes read, or EOF (-1) if the term end has been reached
         * @throws IllegalStateException if log was deleted (index is too low)
         */
        int read(byte[] buf, int offset, int length) throws IOException;

        /**
         * Blocks until the buffer is fully read with messages, never reading past a commit
         * index or term.
         *
         * @throws IllegalStateException if log was deleted (index is too low)
         * @throws EOFException if the term end has been reached too soon
         */
        default void readFully(byte[] buf, int offset, int length) throws IOException {
            while (true) {
                int amt = read(buf, offset, length);
                if (amt <= 0) {
                    throw new EOFException();
                }
                if ((length -= amt) <= 0) {
                    break;
                }
                offset += amt;
            }
        }
    }

    /**
     * Interface called by the group leader for proposing messages. When consensus has been
     * reached, the messages are committed and become available for all members to read.
     *
     * @see StreamReplicator#newWriter newWriter
     */
    public static interface Writer extends DirectReplicator.Writer {
        /**
         * Write complete messages to the log. Equivalent to: {@code write(messages, 0,
         * messages.length, }{@link #index index() + }{@code messages.length)}
         *
         * @return amount of bytes written, which is less than the message length only if the
         * writer is deactivated
         */
        default int write(byte[] messages) throws IOException {
            return write(messages, 0, messages.length);
        }

        /**
         * Write complete messages to the log. Equivalent to: {@code write(messages, offset,
         * length, }{@link #index index() + }{@code length)}
         *
         * @return amount of bytes written, which is less than the given length only if the
         * writer is deactivated
         */
        default int write(byte[] messages, int offset, int length) throws IOException {
            return write(messages, offset, length, index() + length);
        }

        /**
         * Write complete or partial messages to the log. The {@code highestIndex} parameter
         * defines the new absolute log index which can become the commit index. The provided
         * highest index is permitted to exceed the current log size, in anticpation of future
         * writes which will fill in the gap. Until the gap is filled in, the highest index
         * won't be applied. In addition, the highest index can only ever advance. Passing in a
         * smaller value for the highest index won't actually change it. If all of the provided
         * messages are partial, simply pass zero as the highest index. To update the highest
         * index without actually writing anything, pass a length of zero.
         *
         * @param highestIndex highest index (exclusive) which can become the commit index
         * @return amount of bytes written, which is less than the given length only if the
         * writer is deactivated
         */
        int write(byte[] messages, int offset, int length, long highestIndex) throws IOException;
    }
}
