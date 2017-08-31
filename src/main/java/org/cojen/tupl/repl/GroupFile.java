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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Reader;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import java.security.SecureRandom;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;

import java.util.function.Consumer;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.Latch;

/**
 * Persists members and group information into a text file.
 *
 * @author Brian S O'Neill
 */
final class GroupFile extends Latch {
    /*
      File format example:

      version = 123
      groupId = 5369366177412205944
      2056066484459339106 = localhost/127.0.0.1:3456 | NORMAL
      16595601473713733522 = localhost/127.0.0.1:4567 | NORMAL
      3421928112176343799 = localhost/127.0.0.1:5678 | STANDBY

    */

    private final File mFile;
    private final SocketAddress mLocalMemberAddress;
    private final NavigableSet<Peer> mPeerSet;
    private final long mGroupId;

    private long mVersion;

    private long mLocalMemberId;
    private Role mLocalMemberRole;

    private Map<byte[], Consumer<InputStream>> mJoinConsumers;

    /**
     * @return null if file doesn't exist and create is false
     * @throws IllegalStateException if local member address doesn't match the file
     */
    public static GroupFile open(File file, SocketAddress localMemberAddress, boolean create)
        throws IOException
    {
        if (file == null || localMemberAddress == null) {
            throw new IllegalArgumentException();
        }

        RandomAccessFile raf = openFile(file);

        return raf == null && !create ? null : new GroupFile(file, localMemberAddress, raf);
    }

    private GroupFile(File file, SocketAddress localMemberAddress, RandomAccessFile raf)
        throws IOException
    {
        mFile = file;
        mLocalMemberAddress = localMemberAddress;
        mPeerSet = new ConcurrentSkipListSet<>((a, b) -> Long.compare(a.mMemberId, b.mMemberId));

        if (raf == null) {
            // Create the file.

            long groupId;
            SecureRandom rnd = new SecureRandom();
            do {
                groupId = rnd.nextLong();
            } while (groupId == 0);

            mGroupId = groupId;
            mLocalMemberId = mVersion + 1;
            mLocalMemberRole = Role.NORMAL;

            // Version is bumped to 1 as a side-effect.
            persist();

            return;
        }

        // Parse the file.

        Properties props = new Properties();
        try (Reader r = new BufferedReader(new FileReader(raf.getFD()))) {
            props.load(r);
        }

        long version = 0;
        long groupId = 0;
        long localMemberId = 0;

        for (Map.Entry e : props.entrySet()) {
            String key = (String) e.getKey();
            String value = (String) e.getValue();

            if (key.equals("version")) {
                try {
                    version = Long.parseUnsignedLong(value);
                } catch (NumberFormatException ex) {
                    throw new IllegalStateException("Unsupported version: " + value);
                }
            }

            if (key.equals("groupId")) {
                try {
                    groupId = Long.parseUnsignedLong(value);
                } catch (NumberFormatException ex) {
                }
                continue;
            }

            long memberId;
            try {
                memberId = Long.parseUnsignedLong(key);
            } catch (NumberFormatException ex) {
                continue;
            }

            if (memberId == 0) {
                continue;
            }

            int ix = value.indexOf('|');
            if (ix <= 0) {
                continue;
            }

            SocketAddress addr = parseSocketAddress(value.substring(0, ix).trim());

            if (addr == null) {
                continue;
            }

            Role role;
            try {
                role = Role.valueOf(Role.class, value.substring(ix + 1).trim());
            } catch (IllegalArgumentException ex) {
                continue;
            }

            if (addr.equals(localMemberAddress)) {
                if (mLocalMemberRole != null) {
                    throw new IllegalStateException("Duplicate address: " + addr);
                }
                localMemberId = memberId;
                mLocalMemberRole = role;
                continue;
            }

            if (memberId == localMemberId) {
                throw new IllegalStateException("Duplicate member identifier: " + memberId);
            }

            Peer peer = new Peer(memberId, addr, role);

            if (!mPeerSet.add(peer)) {
                throw new IllegalStateException("Duplicate member identifier: " + memberId);
            }
        }

        if (version == 0) {
            throw new IllegalStateException("Unsupported version: " + version);
        }

        if (groupId == 0) {
            throw new IllegalStateException("Group identifier not found");
        }

        mVersion = version;
        mGroupId = groupId;
        mLocalMemberId = localMemberId;
    }

    /**
     * @return non-zero group identifier
     */
    public long groupId() {
        return mGroupId;
    }

    /**
     * @return non-null local member address
     */
    public SocketAddress localMemberAddress() {
        return mLocalMemberAddress;
    }

    /**
     * @return local member identifier, or 0 if not in the group
     */
    public long localMemberId() {
        acquireShared();
        long id = mLocalMemberId;
        releaseShared();
        return id;
    }

    /**
     * @return local member role, or null if not in the group
     */
    public Role localMemberRole() {
        acquireShared();
        Role role = mLocalMemberRole;
        releaseShared();
        return role;
    }

    /**
     * @return non-null set, possibly empty
     */
    public NavigableSet<Peer> allPeers() {
        return mPeerSet;
    }

    /**
     * @return the current file version, which increases with each update
     */
    public long version() {
        acquireShared();
        long version = mVersion;
        releaseShared();
        return version;
    }

    /**
     * Create a control message for joining the group as an observer. If the control message is
     * accepted, apply it by calling applyJoin, and the consumer receives a copy of the group
     * file with the new member. To avoid leaking memory in case the message is rejected, call
     * discardJoinConsumer when timed out.
     *
     * @param op opcode used to identify the join control message
     * @param address member which is joining; pass null to just capture the file
     * @param dest optional callback which is registered and invoked when message is accepted
     * @return control message; first byte is the opcode
     */
    public byte[] proposeJoin(byte op, SocketAddress address, Consumer<InputStream> dest) {
        EncodingOutputStream eout = new EncodingOutputStream();
        eout.write(op);
        eout.encodeLongLE(mVersion);
        eout.encodeStr(address == null ? "" : address.toString());

        byte[] message = eout.toByteArray();

        if (dest != null) {
            acquireExclusive();
            Map<byte[], Consumer<InputStream>> consumers = mJoinConsumers;
            try {
                if (consumers == null) {
                    consumers = new ConcurrentSkipListMap<>(Utils::compareUnsigned);
                    mJoinConsumers = consumers;
                }
            } finally {
                releaseExclusive();
            }

            consumers.put(message, dest);
        }

        return message;
    }

    /**
     * Discard the file consumer associated with a proposed join message. This action doesn't
     * cancel the join proposal itself -- the member might still be added to the group.
     *
     * @param message exact message as returned by proposeJoin
     */
    public void discardJoinConsumer(byte[] message) {
        acquireExclusive();

        if (mJoinConsumers != null
            && mJoinConsumers.remove(message) != null
            && mJoinConsumers.isEmpty())
        {
            mJoinConsumers = null;
        }

        releaseExclusive();
    }

    /**
     * Invokes the registered callback corresponding to the given join control message. The
     * callback is invoked with a shared latch held on this GroupFile object, preventing
     * changes to the group until the callback returns. The stream is also closed automatically
     * when it returns.
     *
     * @param message exact message as returned by proposeJoin
     * @throws RuntimeException if message encoding is wrong
     */
    public void applyJoin(byte[] message) throws IOException {
        Consumer<InputStream> consumer = null;

        acquireExclusive();
        try {
            if (mJoinConsumers != null) {
                consumer = mJoinConsumers.remove(message);
                if (mJoinConsumers.isEmpty()) {
                    mJoinConsumers = null;
                }
            }

            DecodingInputStream din = new DecodingInputStream(message);
            din.read(); // skip opcode
            long version = din.decodeLongLE();
            String addressStr = din.decodeStr();

            if (addressStr.length() != 0) {
                SocketAddress address = parseSocketAddress(addressStr);
                if (address == null) {
                    throw new IllegalArgumentException("Illegal address: " + addressStr);
                }
                try {
                    doAddPeer(version, address, Role.OBSERVER);
                } catch (IllegalStateException e) {
                    // Assume member is already in the group.
                }
            }
        } catch (Throwable e) {
            releaseExclusive();
            throw e;
        }

        if (consumer == null) {
            releaseExclusive();
            return;
        }

        downgrade();

        try {
            try (InputStream in = new FileInputStream(mFile)) {
                consumer.accept(in);
            } catch (Throwable e) {
                Utils.uncaught(e);
            }
        } finally {
            releaseShared();
        }
    }

    /**
     * @return peer with non-zero member id, or null if address matched the local member address
     * @throws IllegalArgumentException if address or role is null
     * @throws IllegalStateException if address is already in use
     */
    public Peer addPeer(SocketAddress address, Role role) throws IOException {
        return addPeer(mVersion, address, role);
    }

    /**
     * @param version expected version
     * @return peer with non-zero member id, or null if version doesn't match, or null if
     * address matched the local member address
     * @throws IllegalArgumentException if address or role is null
     * @throws IllegalStateException if address is already in use
     */
    public Peer addPeer(long version, SocketAddress address, Role role) throws IOException {
        acquireExclusive();
        try {
            return doAddPeer(version, address, role);
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Caller must hold exclusive latch.
     */
    private Peer doAddPeer(long version, SocketAddress address, Role role) throws IOException {
        if (version != mVersion) {
            return null;
        }

        boolean isPeer = checkAddPeer(address, role);

        if (isPeer) {
            Peer peer = new Peer(mVersion + 1, address, role);

            if (!mPeerSet.add(peer)) {
                // 64-bit identifier wrapped around, which is unlikely.
                throw new IllegalStateException("Identifier collision: " + peer);
            }

            try {
                persist();
            } catch (IOException e) {
                // Rollback.
                mPeerSet.remove(peer);
                throw e;
            }

            return peer;
        }

        mLocalMemberId = mVersion + 1;
        mLocalMemberRole = role;
            
        try {
            persist();
        } catch (IOException e) {
            // Rollback.
            mLocalMemberId = 0;
            mLocalMemberRole = null;
            throw e;
        }

        return null;
    }

    /**
     * Caller must hold any latch.
     *
     * @return true if peer, false if adding local member id
     */
    private boolean checkAddPeer(SocketAddress address, Role role) {
        if (address == null || role == null) {
            throw new IllegalArgumentException();
        }

        boolean isPeer = true;

        if (address.equals(mLocalMemberAddress)) {
            if (mLocalMemberId == 0) {
                isPeer = false;
            } else {
                throw new IllegalStateException("Address used by local member");
            }
        }

        for (Peer peer : mPeerSet) {
            if (address.equals(peer.mAddress)) {
                throw new IllegalStateException("Address used by another peer");
            }
        }

        return isPeer;
    }

    /**
     * Checks if updating the role is allowed, and returns a control message to replicate. If
     * the control message is accepted, apply it by calling applyUpdateRole.
     *
     * @param op opcode used to identify the updateRole control message
     * @return control message; first byte is the opcode
     * @throws IllegalArgumentException if role is null
     * @throws IllegalStateException if member doesn't exist
     */
    public byte[] proposeUpdateRole(byte op, long memberId, Role role) {
        acquireShared();
        try {
            checkUpdateRole(memberId, role);

            byte[] message = new byte[1 + 8 + 8 + 1];

            message[0] = op;
            Utils.encodeLongLE(message, 1, mVersion);
            Utils.encodeLongLE(message, 1 + 8, memberId);
            message[1 + 8 + 8] = role.mCode;

            return message;
        } finally {
            releaseShared();
        }
    }

    /**
     * @param message first byte is the opcode, which is not examined by this method
     * @return false if version doesn't match
     * @throws RuntimeException if message encoding is wrong
     * @throws IllegalStateException if member doesn't exist
     */
    public boolean applyUpdateRole(byte[] message) throws IOException {
        long version = Utils.decodeLongLE(message, 1);
        long memberId = Utils.decodeLongLE(message, 1 + 8);
        Role role = Role.decode(message[1 + 8 + 8]);
        return updateRole(version, memberId, role);
    }

    /**
     * @throws IllegalArgumentException if role is null
     * @throws IllegalStateException if member doesn't exist
     */
    public void updateRole(long memberId, Role role) throws IOException {
        updateRole(mVersion, memberId, role);
    }

    /**
     * @param version expected version
     * @return false if version doesn't match
     * @throws IllegalArgumentException if role is null
     * @throws IllegalStateException if member doesn't exist
     */
    public boolean updateRole(long version, long memberId, Role role) throws IOException {
        acquireExclusive();
        try {
            if (version != mVersion) {
                return false;
            }

            Peer existing = checkUpdateRole(memberId, role);

            if (memberId == mLocalMemberId) {
                Role existingRole = mLocalMemberRole;

                if (existingRole != role) {
                    mLocalMemberRole = role;

                    try {
                        persist();
                    } catch (IOException e) {
                        // Rollback.
                        mLocalMemberRole = existingRole;
                        throw e;
                    }
                }

                return true;
            }

            Role existingRole = existing.mRole;

            if (existingRole != role) {
                existing.mRole = role;

                try {
                    persist();
                } catch (IOException e) {
                    // Rollback.
                    existing.mRole = existingRole;
                    throw e;
                }
            }

            return true;
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Caller must hold any latch.
     *
     * @return existing peer instance, or null for local member
     */
    private Peer checkUpdateRole(long memberId, Role role) {
        if (role == null) {
            throw new IllegalArgumentException();
        }

        if (memberId == 0) {
            throw new IllegalStateException("Member doesn't exist: " + memberId);
        }

        if (memberId == mLocalMemberId) {
            return null;
        }

        Peer existing = mPeerSet.floor(new Peer(memberId));

        if (existing == null || existing.mMemberId != memberId) {
            throw new IllegalStateException("Member doesn't exist: " + memberId);
        }

        return existing;
    }

    /**
     * Checks if removing the peer is allowed, and returns a control message to replicate. If
     * the control message is accepted, apply it by calling applyRemovePeer.
     *
     * @param op opcode used to identify the removePeer control message
     * @return control message; first byte is the opcode
     * @throws IllegalStateException if removing the local member
     */
    public byte[] proposeRemovePeer(byte op, long memberId) {
        acquireShared();
        try {
            checkRemovePeer(memberId);

            byte[] message = new byte[1 + 8 + 8];

            message[0] = op;
            Utils.encodeLongLE(message, 1, mVersion);
            Utils.encodeLongLE(message, 1 + 8, memberId);

            return message;
        } finally {
            releaseShared();
        }
    }

    /**
     * @param message first byte is the opcode, which is not examined by this method
     * @return false if member doesn't exist or if version doesn't match
     * @throws RuntimeException if message encoding is wrong
     * @throws IllegalStateException if removing the local member
     */
    public boolean applyRemovePeer(byte[] message) throws IOException {
        long version = Utils.decodeLongLE(message, 1);
        long memberId = Utils.decodeLongLE(message, 1 + 8);
        return removePeer(version, memberId);
    }

    /**
     * @return false if member doesn't exist
     * @throws IllegalStateException if removing the local member
     */
    public boolean removePeer(long memberId) throws IOException {
        return removePeer(mVersion, memberId);
    }

    /**
     * @param version expected version
     * @return false if member doesn't exist or if version doesn't match
     * @throws IllegalStateException if removing the local member
     */
    public boolean removePeer(long version, long memberId) throws IOException {
        acquireExclusive();
        try {
            if (version != mVersion) {
                return false;
            }

            checkRemovePeer(memberId);

            if (memberId == 0) {
                return false;
            }

            NavigableSet<Peer> peers = mPeerSet;

            Peer existing = peers.floor(new Peer(memberId));
            if (existing == null || existing.mMemberId != memberId) {
                return false;
            }

            if (!peers.remove(existing)) {
                throw new AssertionError();
            }

            try {
                persist();
            } catch (IOException e) {
                // Rollback.
                peers.add(existing);
                throw e;
            }

            return true;
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Caller must hold any latch.
     */
    private void checkRemovePeer(long memberId) {
        if (memberId == mLocalMemberId) {
            throw new IllegalStateException("Cannot remove local member");
        }
    }

    /**
     * Caller must hold exclusive latch.
     */
    private void persist() throws IOException {
        mVersion++;
        try {
            doPersist();
        } catch (Throwable e) {
            // Rollback.
            mVersion--;
            throw e;
        }
    }

    /**
     * Caller must hold exclusive latch.
     */
    private void doPersist() throws IOException {
        String path = mFile.getPath();
        File oldFile = new File(path + ".old");
        File newFile = new File(path + ".new");

        try (FileOutputStream out = new FileOutputStream(newFile)) {
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(out));

            w.write('#');
            w.write(getClass().getName());
            w.newLine();

            w.write("version");
            w.write(" = ");
            w.write(Long.toUnsignedString(mVersion));
            w.newLine();

            w.write("groupId");
            w.write(" = ");
            w.write(Long.toUnsignedString(mGroupId));
            w.newLine();

            writeMember(w, mLocalMemberId, mLocalMemberAddress, mLocalMemberRole);

            for (Peer peer : mPeerSet) {
                writeMember(w, peer.mMemberId, peer.mAddress, peer.mRole);
            }

            w.flush();
            out.getFD().sync();
        }

        if (!mFile.renameTo(oldFile) && mFile.exists()) {
            throw new IOException("Unable to rename: " + mFile + " to " + oldFile);
        }

        if (!newFile.renameTo(mFile)) {
            throw new IOException("Unable to rename: " + newFile + " to " + mFile);
        }

        // No need to check if delete failed.
        oldFile.delete();
    }

    private static void writeMember(BufferedWriter w, long memberId, SocketAddress addr, Role role)
        throws IOException
    {
        w.write(Long.toUnsignedString(memberId));
        w.write(" = ");
        w.write(addr.toString());
        w.write(" | ");
        w.write(role.toString());
        w.newLine();
    }

    /**
     * @return null if doesn't exist
     */
    private static RandomAccessFile openFile(File file) throws IOException {
        String path = file.getPath();
        File oldFile = new File(path + ".old");
        File newFile = new File(path + ".new");

        while (true) {
            try {
                RandomAccessFile raf = new RandomAccessFile(file, "r");
                if (raf.length() == 0) {
                    raf.close();
                    raf = null;
                }
                oldFile.delete();
                newFile.delete();
                return raf;
            } catch (FileNotFoundException e) {
            }

            if (newFile.exists()) {
                newFile.renameTo(file);
            } else if (oldFile.exists()) {
                oldFile.renameTo(file);
            } else {
                return null;
            }
        }
    }

    static SocketAddress parseSocketAddress(String str) throws UnknownHostException {
        int ix = str.indexOf(':');
        if (ix <= 0) {
            return null;
        }

        int port;
        try {
            port = Integer.parseInt(str.substring(ix + 1).trim());
        } catch (NumberFormatException e) {
            return null;
        }

        str = str.substring(0, ix).trim();

        ix = str.indexOf('/');

        InetAddress addr;
        if (ix < 0) {
            addr = InetAddress.getByName(str);
        } else {
            String host = str.substring(0, ix).trim();
            String addrStr = str.substring(ix + 1).trim();
            if (host.length() == 0) {
                addr = InetAddress.getByName(addrStr);
            } else {
                addr = InetAddress.getByName(addrStr);
                addr = InetAddress.getByAddress(host, addr.getAddress());
            }
        }

        try {
            return new InetSocketAddress(addr, port);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }
}
