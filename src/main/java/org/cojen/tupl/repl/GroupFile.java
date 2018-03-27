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
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Reader;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import java.security.SecureRandom;

import java.util.Iterator;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;

import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;

import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.ObjLongConsumer;

import java.util.logging.Level;

import java.util.zip.Checksum;
import java.util.zip.CRC32C;

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

    private final BiConsumer<Level, String> mEventListener;
    private final File mFile;
    private final SocketAddress mLocalMemberAddress;
    private final long mGroupId;

    private NavigableSet<Peer> mPeerSet;
    private long mVersion;

    private long mLocalMemberId;
    private Role mLocalMemberRole;

    private Map<byte[], Object> mProposeConsumers;

    /**
     * @return null if file doesn't exist and create is false
     * @throws IllegalStateException if local member address doesn't match the file
     */
    public static GroupFile open(BiConsumer<Level, String> eventListener,
                                 File file, SocketAddress localMemberAddress, boolean create)
        throws IOException
    {
        if (file == null || localMemberAddress == null) {
            throw new IllegalArgumentException();
        }

        RandomAccessFile raf = openFile(file);

        return raf == null && !create ? null
            : new GroupFile(eventListener, file, localMemberAddress, raf);
    }

    private GroupFile(BiConsumer<Level, String> eventListener,
                      File file, SocketAddress localMemberAddress, RandomAccessFile raf)
        throws IOException
    {
        mEventListener = eventListener;
        mFile = file;
        mLocalMemberAddress = localMemberAddress;
        mPeerSet = new ConcurrentSkipListSet<>((a, b) -> Long.compare(a.mMemberId, b.mMemberId));

        if (raf != null) {
            acquireExclusive();
            try {
                mGroupId = parseFile(raf);
            } finally {
                releaseExclusive();
            }
        } else {
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

            localAddedEvent();
        }
    }

    private void event(Level level, String message) {
        if (mEventListener != null) {
            try {
                mEventListener.accept(level, message);
            } catch (Throwable e) {
                // Ignore.
            }
        }
    }

    private void localAddedEvent() {
        event(Level.INFO, "Local member added: " + mLocalMemberRole);
    }

    private void peerAddedEvent(Peer peer) {
        event(Level.INFO, "Remote member added: " + peer.mAddress + ", " + peer.mRole);
    }

    private void localRoleChangeEvent(Role from) {
        if (from == null) {
            localAddedEvent();
        } else {
            event(Level.INFO, "Local member role changed: " + from + " to " + mLocalMemberRole);
        }
    }

    private void peerRoleChangeEvent(Role from, Peer to) {
        if (from == null) {
            peerAddedEvent(to);
        } else {
            event(Level.INFO, "Remote member role changed: " + to.mAddress + ", "
                  + from + " to " + to.mRole);
        }
    }

    private void peerRemoveEvent(Peer peer) {
        event(Level.INFO, "Remote member removed: " + peer.mAddress + ", " + peer.mRole);
    }

    /**
     * Caller must hold exclusive latch.
     *
     * @return groupId
     */
    private long parseFile(RandomAccessFile raf) throws IOException {
        Properties props = new Properties();
        try (Reader r = new BufferedReader(new FileReader(raf.getFD()))) {
            props.load(r);
        }

        NavigableSet<Peer> peerSet =
            new ConcurrentSkipListSet<>((a, b) -> Long.compare(a.mMemberId, b.mMemberId));

        long version = 0;
        long groupId = 0;
        long localMemberId = 0;
        Role localMemberRole = null;

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

            if (addr.equals(mLocalMemberAddress)) {
                if (localMemberRole != null) {
                    throw new IllegalStateException("Duplicate address: " + addr);
                }
                localMemberId = memberId;
                localMemberRole = role;
                continue;
            }

            if (memberId == localMemberId) {
                throw new IllegalStateException("Duplicate member identifier: " + memberId);
            }

            Peer peer = new Peer(memberId, addr, role);

            if (!peerSet.add(peer)) {
                throw new IllegalStateException("Duplicate member identifier: " + memberId);
            }
        }

        if (version == 0) {
            throw new IllegalStateException("Unsupported version: " + version);
        }

        if (groupId == 0) {
            throw new IllegalStateException("Group identifier not found");
        }

        if (mGroupId != 0 && groupId != mGroupId) {
            throw new IllegalStateException
                ("Group identifier changed: " + mGroupId + " -> " + groupId);
        }

        // The file can be parsed after calling readFrom, which can be called at any time
        // to sync the group from a peer. The existing Peer objects must not be replaced.
        // Merge the new set of peers into the old set: add, remove, and update.

        Iterator<Peer> oldIt = mPeerSet.iterator();
        Iterator<Peer> newIt = peerSet.iterator();

        Peer oldPeer = null, newPeer = null;

        while (true) {
            oldPeer = tryNext(oldIt, oldPeer);
            newPeer = tryNext(newIt, newPeer);

            if (oldPeer == null) {
                if (newPeer == null) {
                    break;
                }
            } else if (newPeer == null || oldPeer.mMemberId < newPeer.mMemberId) {
                // Remove old peer.
                peerRemoveEvent(oldPeer);
                oldIt.remove();
                oldPeer = null;
                continue;
            } else if (oldPeer.mMemberId == newPeer.mMemberId) {
                // Update existing peer.
                Role currentRole = oldPeer.mRole;
                if (currentRole != newPeer.mRole) {
                    oldPeer.mRole = newPeer.mRole;
                    peerRoleChangeEvent(currentRole, oldPeer);
                }
                oldPeer = null;
                newPeer = null;
                continue;
            }

            // Add new peer.
            mPeerSet.add(newPeer);
            peerAddedEvent(newPeer);
            newPeer = null;
        }

        mVersion = version;

        mLocalMemberId = localMemberId;

        Role currentRole = mLocalMemberRole;
        if (currentRole != localMemberRole) {
            mLocalMemberRole = localMemberRole;
            localRoleChangeEvent(currentRole);
        }

        return groupId;
    }

    /**
     * @param obj previous object returned by iterator (start with null)
     * @return next object returned by iterator, or null if none left
     */
    private static <P> P tryNext(Iterator<P> it, P obj) {
        if (obj == null && it.hasNext()) {
            obj = it.next();
        }
        return obj;
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
     * @return non-null set ordered by member id, possibly empty
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
     * Writes the version, the file length, the file contents, and then a CRC32C to the given
     * stream.
     */
    public void writeTo(OutputStream out) throws IOException {
        byte[] buf = new byte[1000];
        Checksum checksum = new CRC32C();

        acquireShared();
        try {
            RandomAccessFile raf = openFile(mFile);

            if (raf == null) {
                throw new FileNotFoundException(mFile.toString());
            }

            try {
                long length = raf.length();

                Utils.encodeLongLE(buf, 0, mVersion);
                Utils.encodeLongLE(buf, 8, length);
                int offset = 16;

                while (true) {
                    int amt = raf.read(buf, offset, buf.length - offset);
                    if (amt <= 0) {
                        throw new IOException("File length changed");
                    }
                    out.write(buf, 0, amt + offset);
                    checksum.update(buf, 0, amt + offset);
                    length -= amt;
                    if (length <= 0) {
                        if (length < 0) {
                            throw new IOException("File length changed");
                        }
                        break;
                    }
                    offset = 0;
                }
            } finally {
                Utils.closeQuietly(raf);
            }

            Utils.encodeIntLE(buf, 0, (int) checksum.getValue());
            out.write(buf, 0, 4);
        } finally {
            releaseShared();
        }
    }

    /**
     * Reads the version, the file length, and the file contents, and then a CRC32C from the
     * given stream. If the version isn't higher than the current one, no changes are made and
     * false is returned.
     */
    public boolean readFrom(InputStream in) throws IOException {
        byte[] buf = new byte[1000];
        Checksum checksum = new CRC32C();

        long version, length;

        acquireExclusive();
        try {
            in.read(buf, 0, 16);
            version = Utils.decodeLongLE(buf, 0);
            length = Utils.decodeLongLE(buf, 8);
            checksum.update(buf, 0, 16);
        } catch (Throwable e) {
            releaseExclusive();
            throw e;
        }

        if (version <= mVersion) {
            releaseExclusive();

            length += 4; // skip checksum too

            do {
                long amt = in.skip(length);
                if (amt <= 0) {
                    throw new EOFException();
                }
                length -= amt;
            } while (length > 0);

            return false;
        }

        File oldFile;

        try {
            String path = mFile.getPath();
            oldFile = new File(path + ".old");
            File newFile = new File(path + ".new");

            try (FileOutputStream out = new FileOutputStream(newFile)) {
                while (length > 0) {
                    int amt = in.read(buf, 0, length < buf.length ? (int) length : buf.length);
                    if (amt <= 0) {
                        throw new EOFException();
                    }
                    out.write(buf, 0, amt);
                    checksum.update(buf, 0, amt);
                    length -= amt;
                }

                // Read the checksum.
                int expect = (int) checksum.getValue();
                in.read(buf, 0, 4);
                int actual = Utils.decodeIntLE(buf, 0);
                if (expect != actual) {
                    throw new IOException("Checksum mismatch: " + expect + " != " + actual);
                }

                out.getFD().sync();
            } catch (Throwable e) {
                newFile.delete();
                throw e;
            }

            swapFiles(oldFile, newFile);

            try {
                parseFile(new RandomAccessFile(mFile, "r"));
            } catch (Throwable e) {
                // No changes have been made. Delete the current file and then attempt to
                // rename the old file to the current file.
                mFile.delete();
                oldFile.renameTo(mFile);
                throw e;
            }
        } finally {
            releaseExclusive();
        }

        // No need to check if delete failed.
        oldFile.delete();

        return true;
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
    public byte[] proposeJoin(byte op, SocketAddress address, ObjLongConsumer<InputStream> dest) {
        EncodingOutputStream eout = new EncodingOutputStream();
        eout.write(op);
        eout.encodeLongLE(version());
        // Encode a simple "unique" key to identify the request, although not strictly
        // necessary. The risk is that a registered callback might get dropped when putting a
        // new one in, but they could also be chained.
        eout.encodeLongLE(ThreadLocalRandom.current().nextLong());
        eout.encodeStr(address == null ? "" : address.toString());

        byte[] message = eout.toByteArray();

        if (dest != null) {
            acquireExclusive();
            addProposeConsumer(message, dest);
        }

        return message;
    }

    /**
     * Caller must acquire exclusive latch, which is always released by this method.
     */
    private void addProposeConsumer(byte[] message, Object consumer) {
        Map<byte[], Object> consumers = mProposeConsumers;
        try {
            if (consumers == null) {
                consumers = new ConcurrentSkipListMap<>(Utils::compareUnsigned);
                mProposeConsumers = consumers;
            }
        } finally {
            releaseExclusive();
        }

        consumers.put(message, consumer);
    }

    /**
     * Discard the consumer associated with a proposal message. This action doesn't cancel the
     * proposal itself -- the member might still be added or removed.
     *
     * @param message exact message as returned by the propose method
     * @return false if no consumer was found
     */
    public boolean discardProposeConsumer(byte[] message) {
        acquireExclusive();

        boolean removed = false;

        if (mProposeConsumers != null && mProposeConsumers.remove(message) != null) {
            removed = true;
            if (mProposeConsumers.isEmpty()) {
                mProposeConsumers = null;
            }
        }

        releaseExclusive();

        return removed;
    }

    /**
     * Caller must hold exclusive latch.
     */
    @SuppressWarnings("unchecked")
    private <C> C removeProposeConsumer(byte[] message) {
        C consumer = null;
        if (mProposeConsumers != null) {
            consumer = (C) mProposeConsumers.remove(message);
            if (mProposeConsumers.isEmpty()) {
                mProposeConsumers = null;
            }
        }
        return consumer;
    }

    /**
     * Invokes the registered callback corresponding to the given join control message. The
     * callback is invoked with a shared latch held on this GroupFile object, preventing
     * changes to the group until the callback returns. The stream is also closed automatically
     * when it returns.
     *
     * @param index log index just after the message
     * @param message exact message as returned by proposeJoin
     * @return peer if joined, else null
     */
    public Peer applyJoin(long index, byte[] message) throws IOException {
        ObjLongConsumer<InputStream> consumer;
        Peer peer = null;

        acquireExclusive();
        try {
            consumer = removeProposeConsumer(message);

            DecodingInputStream din = new DecodingInputStream(message);
            din.read(); // skip opcode
            long version = din.decodeLongLE();
            long requestId = din.decodeLongLE();
            String addressStr = din.decodeStr();

            if (addressStr.length() != 0) {
                SocketAddress address = parseSocketAddress(addressStr);
                if (address == null) {
                    throw new IllegalArgumentException("Illegal address: " + addressStr);
                }
                try {
                    peer = doAddPeer(version, address, Role.OBSERVER);
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
        } else if (peer == null) {
            releaseExclusive();
            // Rejected for any number of reasons, possibly a version mismatch.
            try {
                consumer.accept(null, 0);
            } catch (Throwable e) {
                Utils.uncaught(e);
            }
        } else {
            downgrade();
            try {
                try (InputStream in = new FileInputStream(mFile)) {
                    consumer.accept(in, index);
                } catch (Throwable e) {
                    Utils.uncaught(e);
                }
            } finally {
                releaseShared();
            }
        }

        return peer;
    }

    /**
     * @return peer with non-zero member id, or null if address matched the local member address
     * @throws IllegalArgumentException if address or role is null
     * @throws IllegalStateException if address is already in use
     */
    public Peer addPeer(SocketAddress address, Role role) throws IOException {
        acquireExclusive();
        try {
            return doAddPeer(mVersion, address, role);
        } finally {
            releaseExclusive();
        }
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
                // Member already exists, so update the role instead.
                doUpdateRole(peer, role);
                return peer;
            }
        }

        if (isPeer) {
            Peer peer = new Peer(mVersion + 1, address, role);
            
            if (!mPeerSet.add(peer)) {
                // 64-bit identifier wrapped around, which is unlikely.
                throw new IllegalStateException("Identifier collision: " + peer);
            }

            try {
                persist();
            } catch (Throwable e) {
                // Rollback.
                mPeerSet.remove(peer);
                throw e;
            }

            peerAddedEvent(peer);

            return peer;
        }

        mLocalMemberId = mVersion + 1;
        mLocalMemberRole = role;
            
        try {
            persist();
        } catch (Throwable e) {
            // Rollback.
            mLocalMemberId = 0;
            mLocalMemberRole = null;
            throw e;
        }

        localAddedEvent();

        return null;
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
        acquireExclusive();
        try {
            doUpdateRole(mVersion, memberId, role);
        } finally {
            releaseExclusive();
        }
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
            return doUpdateRole(version, memberId, role);
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Caller must hold exclusive latch.
     */
    private boolean doUpdateRole(long version, long memberId, Role role) throws IOException {
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
                } catch (Throwable e) {
                    // Rollback.
                    mLocalMemberRole = existingRole;
                    throw e;
                }

                localRoleChangeEvent(existingRole);
            }

            return true;
        }

        doUpdateRole(existing, role);

        return true;
    }

    /**
     * Caller must hold exclusive latch.
     */
    private void doUpdateRole(Peer existing, Role role) throws IOException {
        Role existingRole = existing.mRole;

        if (existingRole != role) {
            existing.mRole = role;

            try {
                persist();
            } catch (Throwable e) {
                // Rollback.
                existing.mRole = existingRole;
                throw e;
            }

            peerRoleChangeEvent(existingRole, existing);
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
     * @param result optional callback which is registered and invoked when message is accepted
     * @throws IllegalStateException if removing the local member
     */
    public byte[] proposeRemovePeer(byte op, long memberId, Consumer<Boolean> result) {
        byte[] message;

        acquireShared();
        try {
            checkRemovePeer(memberId);

            message = new byte[1 + 8 + 8];

            message[0] = op;
            Utils.encodeLongLE(message, 1, mVersion);
            Utils.encodeLongLE(message, 1 + 8, memberId);
        } catch (Throwable e) {
            releaseShared();
            throw e;
        }

        if (result == null) {
            releaseShared();
        } else {
            if (!tryUpgrade()) {
                releaseShared();
                acquireExclusive();
            }
            addProposeConsumer(message, result);
        }

        return message;
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

        Consumer<Boolean> consumer;
        boolean result;

        acquireExclusive();
        try {
            consumer = removeProposeConsumer(message);
            result = doRemovePeer(version, memberId);
        } finally {
            releaseExclusive();
        }

        if (consumer != null) {
            consumer.accept(result);
        }

        return result;
    }

    /**
     * @return false if member doesn't exist
     * @throws IllegalStateException if removing the local member
     */
    public boolean removePeer(long memberId) throws IOException {
        acquireExclusive();
        try {
            return doRemovePeer(mVersion, memberId);
        } finally {
            releaseExclusive();
        }
    }

    /**
     * @param version expected version
     * @return false if member doesn't exist or if version doesn't match
     * @throws IllegalStateException if removing the local member
     */
    public boolean removePeer(long version, long memberId) throws IOException {
        acquireExclusive();
        try {
            return doRemovePeer(version, memberId);
        } finally {
            releaseExclusive();
        }
    }

    /**
     * Caller must hold exclusive latch.
     */
    private boolean doRemovePeer(long version, long memberId) throws IOException {
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
        } catch (Throwable e) {
            // Rollback.
            peers.add(existing);
            throw e;
        }

        peerRemoveEvent(existing);

        return true;
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

        swapFiles(oldFile, newFile);

        // No need to check if delete failed.
        oldFile.delete();
    }

    /**
     * Renames current file to the old file, and then renames the new file to the current file.
     */
    private void swapFiles(File oldFile, File newFile) throws IOException {
        if (!mFile.renameTo(oldFile) && mFile.exists()) {
            throw new IOException("Unable to rename: " + mFile + " to " + oldFile);
        }

        if (!newFile.renameTo(mFile)) {
            throw new IOException("Unable to rename: " + newFile + " to " + mFile);
        }
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
        int ix = str.lastIndexOf(':');
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
