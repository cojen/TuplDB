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

import java.io.File;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import java.util.HashSet;
import java.util.Set;

import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import org.cojen.tupl.diag.EventListener;

import org.cojen.tupl.io.Utils;

/**
 * Configuration options used when opening a replicator.
 *
 * @author Brian S O'Neill
 * @see StreamReplicator#open StreamReplicator.open
 * @see MessageReplicator#open MessageReplicator.open
 */
public class ReplicatorConfig implements Cloneable {
    File mBaseFile;
    boolean mMkdirs;
    long mGroupToken1, mGroupToken2;
    SocketAddress mLocalAddress;
    SocketAddress mListenAddress;
    ServerSocket mLocalSocket;
    Role mLocalRole;
    Set<SocketAddress> mSeeds;
    boolean mProxyWrites;
    boolean mChecksumSockets;
    EventListener mEventListener;
    SocketFactory mSocketFactory;
    ServerSocketFactory mServerSocketFactory;
    long mFailoverLagTimeoutMillis = 1000;

    public ReplicatorConfig() {
        createFilePath(true);
        localRole(Role.NORMAL);
        checksumSockets(true);
    }

    /**
     * Set the base file name for the replicator, which must reside in an ordinary file
     * directory.
     *
     * @throws IllegalArgumentException if file is null
     */
    public ReplicatorConfig baseFile(File file) {
        if (file == null) {
            throw new IllegalArgumentException();
        }
        mBaseFile = file.getAbsoluteFile();
        return this;
    }

    /**
     * Set the base file name for the replicator, which must reside in an ordinary file
     * directory.
     *
     * @throws IllegalArgumentException if path is null
     */
    public ReplicatorConfig baseFilePath(String path) {
        if (path == null) {
            throw new IllegalArgumentException();
        }
        mBaseFile = new File(path).getAbsoluteFile();
        return this;
    }

    /**
     * Set true to create the directory for the replication files, if it doesn't already
     * exist. Default is true.
     */
    public ReplicatorConfig createFilePath(boolean mkdirs) {
        mMkdirs = mkdirs;
        return this;
    }

    /**
     * Set a unique group token, which acts as a simple security measure to prevent different
     * replication groups from communicating with each other. Connections are accepted when the
     * tokens match.
     *
     * @throws IllegalArgumentException if the token is zero
     */
    public ReplicatorConfig groupToken(long token) {
        return groupTokens(token, token);
    }

    /**
     * Set a unique group token (and an alternate), which acts as a simple security measure to
     * prevent different replication groups from communicating with each other. Connections are
     * accepted when a token matches to any other.
     *
     * @throws IllegalArgumentException if either token is zero
     */
    public ReplicatorConfig groupTokens(long token, long altToken) {
        if (token == 0 || altToken == 0) {
            throw new IllegalArgumentException();
        }
        mGroupToken1 = token;
        mGroupToken2 = altToken;
        return this;
    }

    /**
     * Set the local member socket port, for accepting connections on any address. Calling this
     * overrides any explicitly set local address or listen address.
     */
    public ReplicatorConfig localPort(int port) throws UnknownHostException {
        if (port <= 0) {
            throw new IllegalArgumentException();
        }
        mLocalAddress = new InetSocketAddress(LocalHost.getLocalHost(), port);
        mListenAddress = new InetSocketAddress(port);
        return this;
    }

    /**
     * Set the local member socket address and port.
     *
     * @throws IllegalArgumentException if address is null or a wildcard address
     */
    public ReplicatorConfig localAddress(SocketAddress addr) {
        if (addr == null) {
            throw new IllegalArgumentException();
        }
        if (addr instanceof InetSocketAddress sockAddr) {
            if (sockAddr.getAddress().isAnyLocalAddress()) {
                throw new IllegalArgumentException("Wildcard address: " + addr);
            }
        }
        mLocalAddress = addr;
        return this;
    }

    /**
     * Optionally restrict the socket address for accepting connections, which can be a
     * wildcard address.
     *
     * @throws IllegalArgumentException if address is null
     */
    public ReplicatorConfig listenAddress(SocketAddress addr) {
        if (addr == null) {
            throw new IllegalArgumentException();
        }
        mListenAddress = addr;
        return this;
    }

    /**
     * Explicitly specify a connected local socket, intended only for testing.
     */
    public ReplicatorConfig localSocket(ServerSocket ss) throws UnknownHostException {
        mLocalSocket = ss;
        mListenAddress = ss.getLocalSocketAddress();
        mLocalAddress = mListenAddress;

        if (mLocalAddress instanceof InetSocketAddress sockAddr) {
            InetAddress addr = sockAddr.getAddress();
            if (addr.isAnyLocalAddress()) {
                mLocalAddress = new InetSocketAddress
                    (LocalHost.getLocalHost(), sockAddr.getPort());
            }
        }

        return this;
    }

    /**
     * Set the desired local member role, which is {@linkplain Role#NORMAL normal} by default,
     * and for the primordial group member. Members can join an existing group only by
     * consensus, which implies that the group has a leader. All joining members start out as
     * {@linkplain Role#RESTORING restoring}, and then the role is updated after the replicator
     * has started. Role changes also require consensus.
     *
     * @throws IllegalArgumentException if role is null
     */
    public ReplicatorConfig localRole(Role role) {
        if (role == null) {
            throw new IllegalArgumentException();
        }
        mLocalRole = role;
        return this;
    }

    /**
     * Add a remote member address for allowing the local member to join the group. Opening a
     * replicator for the first time without any seeds indicates that a new replication group
     * is to be formed. If the local member is already in a group, the seeds are ignored.
     *
     * @throws IllegalArgumentException if addressString is null or malformed
     */
    public ReplicatorConfig addSeed(String addressString) throws UnknownHostException {
        if (addressString == null) {
            throw new IllegalArgumentException();
        }
        SocketAddress addr = GroupFile.parseSocketAddress(addressString);
        if (addr == null) {
            throw new IllegalArgumentException("Malformed address: " + addressString);
        }
        return addSeed(addr);
    }

    /**
     * Add a remote member address for allowing the local member to join the group. Opening a
     * replicator for the first time without any seeds indicates that a new replication group
     * is to be formed. If the local member is already in a group, the seeds are ignored.
     *
     * @throws IllegalArgumentException if hostname is null
     */
    public ReplicatorConfig addSeed(String hostname, int port) {
        return addSeed(new InetSocketAddress(hostname, port));
    }

    /**
     * Add a remote member address for allowing the local member to join the group. Opening a
     * replicator for the first time without any seeds indicates that a new replication group
     * is to be formed. If the local member is already in a group, the seeds are ignored.
     *
     * @throws IllegalArgumentException if address is null
     */
    public ReplicatorConfig addSeed(SocketAddress addr) {
        if (addr == null) {
            throw new IllegalArgumentException();
        }
        if (mSeeds == null) {
            mSeeds = new HashSet<>();
        }
        mSeeds.add(addr);
        return this;
    }

    /**
     * Pass true to proxy writes from the leader, reducing load on the leader, but increasing
     * commit latency a bit. Default is false.
     */
    public ReplicatorConfig proxyWrites(boolean proxy) {
        mProxyWrites = proxy;
        return this;
    }

    /**
     * Pass true to enable CRC checksums for all socket commands written. Default is true.
     */
    public ReplicatorConfig checksumSockets(boolean checksums) {
        mChecksumSockets = checksums;
        return this;
    }

    /**
     * Set a listener which receives notifications of actions being performed by the replicator.
     */
    public ReplicatorConfig eventListener(EventListener listener) {
        mEventListener = listener;
        return this;
    }

    /**
     * Set a factory for creating new client-side sockets.
     */
    public ReplicatorConfig socketFactory(SocketFactory factory) {
        mSocketFactory = factory;
        return this;
    }

    /**
     * Set a factory for creating new server-side sockets.
     */
    public ReplicatorConfig serverSocketFactory(ServerSocketFactory factory) {
        mServerSocketFactory = factory;
        return this;
    }

    /**
     * Set a timeout for a newly elected leader to be caught up, or else a new election is run.
     * Default is 1000 milliseconds, and a negative timeout disables the check.
     */
    public ReplicatorConfig failoverLagTimeoutMillis(long timeout) {
        mFailoverLagTimeoutMillis = timeout;
        return this;
    }

    @Override
    public ReplicatorConfig clone() {
        ReplicatorConfig copy;
        try {
            copy = (ReplicatorConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw Utils.rethrow(e);
        }

        if (mSeeds != null) {
            copy.mSeeds = new HashSet<>(mSeeds);
        }

        return copy;
    }
}
