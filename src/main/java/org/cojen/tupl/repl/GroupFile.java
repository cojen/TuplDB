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
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Reader;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;

import java.util.Map;
import java.util.NavigableSet;
import java.util.Properties;

import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Persists members and group information into a text file.
 *
 * @author Brian S O'Neill
 */
final class GroupFile {
    /*
      File format example:

      groupId = 5369366177412205944
      2056066484459339106 = localhost/127.0.0.1:3456 | NORMAL
      16595601473713733522 = localhost/127.0.0.1:4567 | NORMAL
      3421928112176343799 = localhost/127.0.0.1:5678 | STANDBY

    */

    private final File mFile;
    private final SocketAddress mLocalMemberAddress;
    private final NavigableSet<Peer> mPeerSet;
    private final long mGroupId;
    private final long mLocalMemberId;

    private Role mLocalMemberRole;

    /**
     * @throws IllegalStateException if local member address doesn't match the file
     */
    GroupFile(File file, SocketAddress localMemberAddress) throws IOException {
        if (file == null || localMemberAddress == null) {
            throw new IllegalArgumentException();
        }

        RandomAccessFile raf = openFile(file);

        mFile = file;
        mLocalMemberAddress = localMemberAddress;
        mPeerSet = new ConcurrentSkipListSet<>((a, b) -> Long.compare(a.mMemberId, b.mMemberId));

        if (raf == null) {
            // Create the file.
            mGroupId = newId();
            mLocalMemberId = newId();
            mLocalMemberRole = Role.NORMAL;
            persist();
            return;
        }

        // Parse the file.

        Properties props = new Properties();
        try (Reader r = new BufferedReader(new FileReader(raf.getFD()))) {
            props.load(r);
        }

        long groupId = 0;
        long localMemberId = 0;

        for (Map.Entry e : props.entrySet()) {
            String key = (String) e.getKey();
            String value = (String) e.getValue();

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

        if (groupId == 0) {
            throw new IllegalStateException("Group identifier not found");
        }

        if (localMemberId == 0) {
            throw new IllegalStateException
                ("Local member identifier not found: " + localMemberAddress);
        }

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
     * @return non-zero local member identifier
     */
    public long localMemberId() {
        return mLocalMemberId;
    }

    /**
     * @return non-null local member address
     */
    public SocketAddress localMemberAddress() {
        return mLocalMemberAddress;
    }

    /**
     * @return non-null local member role
     */
    public synchronized Role localMemberRole() {
        return mLocalMemberRole;
    }

    /**
     * @return non-null set, possibly empty
     */
    public NavigableSet<Peer> allPeers() throws IOException {
        return mPeerSet;
    }

    /**
     * @return non-zero member id
     * @throws IllegalArgumentException if address or role is null
     * @throws IllegalStateException if address is already in use
     */
    public synchronized Peer addPeer(SocketAddress address, Role role) throws IOException {
        if (address == null || role == null) {
            throw new IllegalArgumentException();
        }

        if (address.equals(mLocalMemberAddress)) {
            throw new IllegalStateException("Address used by local member");
        }

        NavigableSet<Peer> peers = mPeerSet;

        for (Peer peer : peers) {
            if (address.equals(peer.mAddress)) {
                throw new IllegalStateException("Address used by another peer");
            }
        }

        Peer peer;
        while (true) {
            peer = new Peer(newId(), address, role);
            Peer existing = peers.floor(peer);
            if (existing == null || existing.mMemberId != peer.mMemberId) {
                break;
            }
        }

        if (!peers.add(peer)) {
            throw new AssertionError();
        }

        try {
            persist();
        } catch (IOException e) {
            // Rollback.
            peers.remove(peer);
            throw e;
        }

        return peer;
    }

    /**
     * @throws IllegalArgumentException if role is null
     * @throws IllegalStateException if member doesn't exist
     */
    public synchronized void updateRole(long memberId, Role role) throws IOException {
        if (role == null) {
            throw new IllegalArgumentException();
        }

        if (memberId == 0) {
            throw new IllegalStateException("Member doesn't exist: " + memberId);
        }

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

            return;
        }

        NavigableSet<Peer> peers = mPeerSet;

        Peer existing = peers.floor(new Peer(memberId));
        if (existing == null || existing.mMemberId != memberId) {
            throw new IllegalStateException("Member doesn't exist: " + memberId);
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
    }

    /**
     * @return false if member doesn't exist
     * @throws IllegalStateException if removing the local member
     */
    public synchronized boolean removePeer(long memberId) throws IOException {
        if (memberId == 0) {
            return false;
        }

        if (memberId == mLocalMemberId) {
            throw new IllegalStateException("Cannot remove local member");
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
    }

    private synchronized void persist() throws IOException {
        String path = mFile.getPath();
        File oldFile = new File(path + ".old");
        File newFile = new File(path + ".new");

        try (FileOutputStream out = new FileOutputStream(newFile)) {
            BufferedWriter w = new BufferedWriter(new OutputStreamWriter(out));

            w.write('#');
            w.write(getClass().getName());
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

    private static long newId() {
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        while (true) {
            long id = rnd.nextLong();
            if (id != 0) {
                return id;
            }
        }
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

    private static SocketAddress parseSocketAddress(String str) throws UnknownHostException {
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
