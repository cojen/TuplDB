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

import static org.cojen.tupl.io.Utils.closeQuietly;

/**
 * Low-level replication interface, which receives messages in a non-delineated stream.
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
 * <p>StreamReplicators require that a control message {@linkplain #controlMessageAcceptor
 * acceptor} be installed for supporting group membership changes. Consider using {@link
 * MessageReplicator} instead, although it has slightly higher overhead. In order for control
 * messages to be processed, replicas must be actively reading and calling {@link
 * #controlMessageReceived controlMessageReceived} as necessary.
 *
 * @author Brian S O'Neill
 * @see MessageReplicator
 */
public interface StreamReplicator extends Replicator {
    /**
     * Open a StreamReplicator instance, creating it if necessary. Be sure to call the {@link
     * #start start} method too.
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

        long groupToken1 = config.mGroupToken1;
        long groupToken2 = config.mGroupToken2;

        if (groupToken1 == 0 && groupToken2 == 0) {
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
            if (listenAddress instanceof InetSocketAddress isa) {
                listenAddress = new InetSocketAddress(isa.getPort());
            }
        }

        if (localSocket == null) {
            // Attempt to bind the socket early, failing if the port is in use. This prevents
            // joining the group early, which can cause an existing member's role to be
            // downgraded to observer.
            localSocket = ChannelManager.newServerSocket
                (config.mServerSocketFactory, listenAddress);
        }

        StateLog log = null;

        try {
            Set<SocketAddress> seeds = config.mSeeds;

            if (seeds == null) {
                seeds = Collections.emptySet();
            }

            if (config.mMkdirs) {
                base.getParentFile().mkdirs();
            }

            log = FileStateLog.open(base);

            return Controller.open(config,
                                   log, groupToken1, groupToken2,
                                   new File(base.getPath() + ".group"), 
                                   localAddress, listenAddress,
                                   seeds, localSocket);
        } catch (Throwable e) {
            closeQuietly(localSocket);
            closeQuietly(log);
            throw e;
        }
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
     * Called to pass along a control message, which was originally provided through an
     * {@linkplain #controlMessageAcceptor acceptor}. Control messages must be passed along in
     * the original order in which they were created. A control message cannot be treated as
     * applied until after this method returns.
     *
     * @param position log position just after the message
     */
    void controlMessageReceived(long position, byte[] message) throws IOException;

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
     * applied position. When an application restarts, it must open the reader at an appropriate
     * position.
     *
     * @see StreamReplicator#newReader newReader
     */
    public static interface Reader extends Replicator.Reader {
        /**
         * Blocks until log messages are available, never reading past a commit position or
         * term.
         *
         * @return amount of bytes read, or EOF (-1) if the term end has been reached
         * @throws InvalidReadException if log was deleted (position is too low)
         */
        default int read(byte[] buf) throws IOException {
            return read(buf, 0, buf.length);
        }

        /**
         * Blocks until log messages are available, never reading past a commit position or
         * term.
         *
         * @return amount of bytes read, or EOF (-1) if the term end has been reached
         * @throws InvalidReadException if log was deleted (position is too low)
         */
        int read(byte[] buf, int offset, int length) throws IOException;

        /**
         * Blocks until the buffer is fully read with messages, never reading past a commit
         * position or term.
         *
         * @throws InvalidReadException if log was deleted (position is too low)
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

        /**
         * Reads whatever log data is available, never higher than a commit position, never
         * higher than a term, and never blocking.
         *
         * @return amount of bytes read, or EOF (-1) if the term end has been reached
         * @throws InvalidReadException if log data was deleted (position is too low)
         */
        int tryRead(byte[] buf, int offset, int length) throws IOException;
    }

    /**
     * Interface called by the group leader for proposing messages. When consensus has been
     * reached, the messages are committed and become available for all members to read.
     *
     * @see StreamReplicator#newWriter newWriter
     */
    public static interface Writer extends Replicator.Writer {
        /**
         * Write complete messages to the log. Equivalent to: {@code write(messages, 0,
         * messages.length, }{@link #position position() + }{@code messages.length)}
         *
         * @return 1 if successful, -1 if fully deactivated, or 0 if should write any remaining
         * messages and then close the writer
         */
        default int write(byte[] messages) throws IOException {
            return write(messages, 0, messages.length);
        }

        /**
         * Write complete messages to the log. Equivalent to: {@code write(messages, offset,
         * length, }{@link #position position() + }{@code length)}
         *
         * @return 1 if successful, -1 if fully deactivated, or 0 if should write any remaining
         * messages and then close the writer
         */
        default int write(byte[] messages, int offset, int length) throws IOException {
            return write(null, messages, offset, length, position() + length);
        }

        /**
         * Write complete or partial messages to the log. The {@code highestPosition} parameter
         * defines the new absolute log position which can become the commit position. The
         * provided highest position is permitted to exceed the current log size, in
         * anticipation of future writes which will fill in the gap. Until the gap is filled in,
         * the highest position won't be applied. In addition, the highest position can only
         * ever advance. Passing in a smaller value for the highest position won't actually
         * change it. If all of the provided messages are partial, simply pass zero as the
         * highest position. To update the highest position without actually writing anything,
         * pass a length of zero.
         *
         * @param highestPosition highest position (exclusive) which can become the commit
         * position
         * @return 1 if successful, -1 if fully deactivated, or 0 if should write any remaining
         * messages and then close the writer
         */
        default int write(byte[] messages, int offset, int length, long highestPosition)
            throws IOException
        {
            return write(null, messages, offset, length, highestPosition);
        }

        /**
         * Write complete or partial messages to the log.
         *
         * @param prefix optional prefix message to fully write, which advances the log
         * position just like any other message
         * @param highestPosition highest position (exclusive) which can become the commit
         * position
         * @return 1 if successful, -1 if fully deactivated, or 0 if should write any remaining
         * messages and then close the writer
         */
        int write(byte[] prefix, byte[] messages, int offset, int length, long highestPosition)
            throws IOException;
    }
}
