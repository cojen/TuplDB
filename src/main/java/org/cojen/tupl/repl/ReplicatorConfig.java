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
import java.io.Serializable;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import java.util.HashSet;
import java.util.Set;

import java.util.function.BiConsumer;

import java.util.logging.Level;

import org.cojen.tupl.io.Utils;

/**
 * Configuration options used when opening a replicator.
 *
 * @author Brian S O'Neill
 * @see StreamReplicator#open StreamReplicator.open
 * @see MessageReplicator#open MessageReplicator.open
 * @see DatabaseReplicator#open DatabaseReplicator.open
 */
public class ReplicatorConfig implements Cloneable, Serializable {
    private static final long serialVersionUID = 1L;

    File mBaseFile;
    boolean mMkdirs;
    long mGroupToken;
    SocketAddress mLocalAddress;
    SocketAddress mListenAddress;
    ServerSocket mLocalSocket;
    Role mLocalRole;
    Set<SocketAddress> mSeeds;
    transient BiConsumer<Level, String> mEventListener;

    public ReplicatorConfig() {
        createFilePath(true);
        localRole(Role.NORMAL);
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
     * Set a unique group identifier, which acts as a simple security measure to prevent
     * different replication groups from communicating with each other.
     *
     * @throws IllegalArgumentException if groupToken is zero
     */
    public ReplicatorConfig groupToken(long groupToken) {
        if (groupToken == 0) {
            throw new IllegalArgumentException();
        }
        mGroupToken = groupToken;
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
        if (addr instanceof InetSocketAddress) {
            if (((InetSocketAddress) addr).getAddress().isAnyLocalAddress()) {
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

    // Intended only for testing.
    ReplicatorConfig localSocket(ServerSocket ss) throws UnknownHostException {
        mLocalSocket = ss;
        mListenAddress = ss.getLocalSocketAddress();
        mLocalAddress = mListenAddress;

        if (mLocalAddress instanceof InetSocketAddress) {
            InetSocketAddress sockAddr = (InetSocketAddress) mLocalAddress;
            InetAddress addr = sockAddr.getAddress();
            if (addr.isAnyLocalAddress()) {
                mLocalAddress = new InetSocketAddress
                    (LocalHost.getLocalHost(), sockAddr.getPort());
            }
        }

        return this;
    }

    /**
     * Set the desired local member role, which is {@link Role#NORMAL normal} by default, and
     * for the primoridal group member. Members can join an existing group only by consensus,
     * which implies that the group has a leader. All joining members start out as {@link
     * Role#OBSERVER observers}, and then the role is updated after the replicator has
     * started. Role changes also require consensus.
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
     * Set a listener which receives notifications of actions being performed by the replicator.
     */
    public ReplicatorConfig eventListener(BiConsumer<Level, String> listener) {
        mEventListener = listener;
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
