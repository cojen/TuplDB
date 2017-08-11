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

import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.cojen.tupl.io.Utils;

/**
 * Configuration options used when {@link StreamReplicator#open opening} a replicator.
 *
 * @author Brian S O'Neill
 */
public class ReplicatorConfig implements Cloneable, Serializable {
    private static final long serialVersionUID = 1L;

    File mBaseFile;
    boolean mMkdirs;
    long mGroupId;
    SocketAddress mLocalAddress;
    ServerSocket mLocalSocket;
    Map<Long, SocketAddress> mStaticMembers;
    Set<SocketAddress> mSeeds;

    public ReplicatorConfig() {
        createFilePath(true);
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
     * @throws IllegalArgumentException if groupId is zero
     */
    public ReplicatorConfig groupId(long groupId) {
        if (groupId == 0) {
            throw new IllegalArgumentException();
        }
        mGroupId = groupId;
        return this;
    }

    /**
     * Set the local member socket port, for accepting connections on any address.
     */
    public ReplicatorConfig localPort(int port) {
        return localAddress(new InetSocketAddress(port));
    }

    /**
     * Set the local member socket address and port. If given a wildcard address, the local
     * member will accept connections on any address.
     *
     * @throws IllegalArgumentException if address is null
     */
    public ReplicatorConfig localAddress(SocketAddress addr) {
        if (addr == null) {
            throw new IllegalArgumentException();
        }
        mLocalAddress = addr;
        return this;
    }

    // Intended only for testing.
    ReplicatorConfig localSocket(ServerSocket ss) {
        mLocalAddress = ss.getLocalSocketAddress();
        mLocalSocket = ss;
        return this;
    }

    /**
     * Add a local or remote member to the replication group, which cannot be dynamically
     * removed.
     *
     * @throws IllegalArgumentException if memberId is zero or address is null
     */
    public ReplicatorConfig addMember(long memberId, SocketAddress addr) {
        if (memberId == 0 || addr == null) {
            throw new IllegalArgumentException();
        }
        if (mStaticMembers == null) {
            mStaticMembers = new HashMap<>();
        }
        mStaticMembers.put(memberId, addr);
        return this;
    }

    /**
     * Add a remote member address for allowing the local member to join the group. Opening a
     * replicator for the first time without any seeds indicates that a new replication group
     * is to be formed.
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

    @Override
    public ReplicatorConfig clone() {
        ReplicatorConfig copy;
        try {
            copy = (ReplicatorConfig) super.clone();
        } catch (CloneNotSupportedException e) {
            throw Utils.rethrow(e);
        }

        if (mStaticMembers != null) {
            copy.mStaticMembers = new HashMap<>(mStaticMembers);
        }

        if (mSeeds != null) {
            copy.mSeeds = new HashSet<>(mSeeds);
        }

        return copy;
    }
}
