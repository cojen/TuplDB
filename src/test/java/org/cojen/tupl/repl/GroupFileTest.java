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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;

import java.net.InetSocketAddress;

import java.util.Set;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.TestUtils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class GroupFileTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(GroupFileTest.class.getName());
    }

    @Before
    public void setup() throws Exception {
    }

    @After
    public void teardown() throws Exception {
        TestUtils.deleteTempFiles(getClass());
    }

    @Test
    public void basic() throws Exception {
        try {
            GroupFile.open(null, null, null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }

        File f = TestUtils.newTempBaseFile(getClass());

        try {
            GroupFile.open(null, f, null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }

        assertNull(GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), false));

        GroupFile gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);

        assertEquals(1, gf.version());

        long groupId = gf.groupId();
        assertTrue(groupId != 0);

        long localMemberId = gf.localMemberId();
        assertTrue(localMemberId != 0);

        assertEquals(new InetSocketAddress("localhost", 1001), gf.localMemberAddress());
        assertEquals(Role.NORMAL, gf.localMemberRole());
        assertEquals(0, gf.allPeers().size());

        // Re-open with wrong address.
        gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1002), true);
        assertEquals(new InetSocketAddress("localhost", 1002), gf.localMemberAddress());
        assertEquals(0, gf.localMemberId());
        assertNull(gf.localMemberRole());

        // Re-open correctly.
        gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);

        assertEquals(groupId, gf.groupId());
        assertEquals(localMemberId, gf.localMemberId());

        assertEquals(new InetSocketAddress("localhost", 1001), gf.localMemberAddress());
        assertEquals(Role.NORMAL, gf.localMemberRole());
        assertEquals(0, gf.allPeers().size());
    }

    @Test
    public void addPeer() throws Exception {
        File f = TestUtils.newTempBaseFile(getClass());
        GroupFile gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);

        long groupId = gf.groupId();
        long localMemberId = gf.localMemberId();

        try {
            gf.addPeer(null, null);
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            gf.addPeer(new InetSocketAddress("localhost", 1001), null);
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            gf.addPeer(new InetSocketAddress("localhost", 1001), Role.OBSERVER);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("local") > 0);
        }

        Peer peer = gf.addPeer(new InetSocketAddress("localhost", 1002), Role.OBSERVER);
        assertTrue(peer.mMemberId != 0);
        assertEquals(new InetSocketAddress("localhost", 1002), peer.mAddress);
        assertEquals(Role.OBSERVER, peer.mRole);

        Set<Peer> allPeers = gf.allPeers();
        assertEquals(1, allPeers.size());
        assertEquals(peer, allPeers.iterator().next());

        Peer result = gf.addPeer(new InetSocketAddress("localhost", 1002), Role.NORMAL);
        assertTrue(result == peer);
        assertEquals(Role.NORMAL, peer.mRole);

        Peer peer2 = gf.addPeer(new InetSocketAddress("localhost", 1003), Role.STANDBY);
        assertTrue(peer2.mMemberId != 0);
        assertEquals(new InetSocketAddress("localhost", 1003), peer2.mAddress);
        assertEquals(Role.STANDBY, peer2.mRole);

        allPeers = gf.allPeers();
        assertEquals(2, allPeers.size());

        // Re-open.
        gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);

        assertEquals(groupId, gf.groupId());
        assertEquals(localMemberId, gf.localMemberId());

        allPeers = gf.allPeers();
        assertEquals(2, allPeers.size());
        assertTrue(allPeers.contains(peer));
        assertTrue(allPeers.contains(peer2));

        for (Peer p : allPeers) {
            if (p.equals(peer)) {
                assertEquals(new InetSocketAddress("localhost", 1002), p.mAddress);
                assertEquals(Role.NORMAL, p.mRole);
            } else {
                assertEquals(new InetSocketAddress("localhost", 1003), p.mAddress);
                assertEquals(Role.STANDBY, p.mRole);
            }
        }
    }

    @Test
    public void addPeerMessage() throws Exception {
        File f = TestUtils.newTempBaseFile(getClass());
        GroupFile gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);

        byte[] message = gf.proposeJoin
            ((byte) 10, new InetSocketAddress("localhost", 1002), null);

        assertEquals(10, message[0]);

        gf.applyJoin(123, message);

        Set<Peer> allPeers = gf.allPeers();
        assertEquals(1, allPeers.size());
        Peer peer = allPeers.iterator().next();
        assertTrue(peer.mMemberId != 0);
        assertEquals(new InetSocketAddress("localhost", 1002), peer.mAddress);
        assertEquals(Role.OBSERVER, peer.mRole);

        // Capture the version.
        message = gf.proposeJoin((byte) 1, new InetSocketAddress("localhost", 1003), null);

        // Update version.
        gf.updateRole(peer.mMemberId, Role.NORMAL);

        // Version mismatch, so peer not added.
        gf.applyJoin(123, message);

        allPeers = gf.allPeers();
        assertEquals(1, allPeers.size());
        assertEquals(peer, allPeers.iterator().next());
    }

    @Test
    public void addLocalMember() throws Exception {
        File f = TestUtils.newTempBaseFile(getClass());
        GroupFile gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);

        // Re-open with different address (restored member perhaps)
        InetSocketAddress addr2 = new InetSocketAddress("localhost", 1002);

        gf = GroupFile.open(null, f, addr2, false);

        assertEquals(addr2, gf.localMemberAddress());
        assertEquals(0, gf.localMemberId());
        assertNull(gf.localMemberRole());

        assertNull(gf.addPeer(addr2, Role.NORMAL));

        assertEquals(addr2, gf.localMemberAddress());
        assertEquals(2, gf.localMemberId());
        assertEquals(Role.NORMAL, gf.localMemberRole());

        Set<Peer> allPeers = gf.allPeers();
        assertEquals(1, allPeers.size());
        Peer peer = allPeers.iterator().next();
        assertEquals(1, peer.mMemberId);
        assertEquals(new InetSocketAddress("localhost", 1001), peer.mAddress);

        // Add again will fail, now that local member address is assigned.
        try {
            gf.addPeer(addr2, Role.NORMAL);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("local") > 0);
        }
    }

    @Test
    public void updateRole() throws Exception {
        File f = TestUtils.newTempBaseFile(getClass());
        GroupFile gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);
        Peer peer = gf.addPeer(new InetSocketAddress("localhost", 1002), Role.OBSERVER);
        Peer peer2 = gf.addPeer(new InetSocketAddress("localhost", 1003), Role.STANDBY);

        try {
            gf.updateRole(1, null);
            fail();
        } catch (IllegalArgumentException e) {
        }

        try {
            gf.updateRole(0, Role.NORMAL);
            fail();
        } catch (IllegalStateException e) {
        }

        try {
            long notExist = 1;
            while (notExist == gf.localMemberId()
                   || notExist == peer.mMemberId || notExist == peer2.mMemberId)
            {
                notExist++;
            }
            gf.updateRole(notExist, Role.NORMAL);
            fail();
        } catch (IllegalStateException e) {
        }

        gf.updateRole(gf.localMemberId(), Role.NORMAL);
        assertEquals(Role.NORMAL, gf.localMemberRole());
        gf.updateRole(gf.localMemberId(), Role.OBSERVER);
        assertEquals(Role.OBSERVER, gf.localMemberRole());

        gf.updateRole(peer.mMemberId, Role.OBSERVER);
        assertEquals(Role.OBSERVER, peer.mRole);
        gf.updateRole(peer.mMemberId, Role.NORMAL);
        assertEquals(Role.NORMAL, peer.mRole);

        gf.updateRole(peer2.mMemberId, Role.NORMAL);
        assertEquals(Role.NORMAL, peer2.mRole);

        // Re-open.
        gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);

        assertEquals(Role.OBSERVER, gf.localMemberRole());

        Set<Peer> allPeers = gf.allPeers();
        assertEquals(2, allPeers.size());
        assertTrue(allPeers.contains(peer));
        assertTrue(allPeers.contains(peer2));

        for (Peer p : allPeers) {
            if (p.equals(peer)) {
                assertEquals(new InetSocketAddress("localhost", 1002), p.mAddress);
                assertEquals(Role.NORMAL, p.mRole);
            } else {
                assertEquals(new InetSocketAddress("localhost", 1003), p.mAddress);
                assertEquals(Role.NORMAL, p.mRole);
            }
        }
    }

    @Test
    public void updateRoleMessage() throws Exception {
        File f = TestUtils.newTempBaseFile(getClass());
        GroupFile gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);
        Peer peer = gf.addPeer(new InetSocketAddress("localhost", 1002), Role.OBSERVER);
        Peer peer2 = gf.addPeer(new InetSocketAddress("localhost", 1003), Role.STANDBY);

        byte[] message = gf.proposeUpdateRole((byte) 11, peer.mMemberId, Role.NORMAL);

        assertEquals(11, message[0]);

        assertTrue(gf.applyUpdateRole(message));

        assertEquals(Role.NORMAL, peer.mRole);

        // Capture the version.
        message = gf.proposeUpdateRole((byte) 11, peer2.mMemberId, Role.NORMAL);

        // Update version.
        gf.updateRole(peer.mMemberId, Role.STANDBY);

        // Version mismatch, so role not updated.
        assertFalse(gf.applyUpdateRole(message));
    }

    @Test
    public void removePeer() throws Exception {
        File f = TestUtils.newTempBaseFile(getClass());
        GroupFile gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);

        assertFalse(gf.removePeer(gf.localMemberId() + 1));

        Peer peer = gf.addPeer(new InetSocketAddress("localhost", 1002), Role.OBSERVER);
        Peer peer2 = gf.addPeer(new InetSocketAddress("localhost", 1003), Role.STANDBY);

        assertFalse(gf.removePeer(0));

        try {
            gf.removePeer(gf.localMemberId());
            fail();
        } catch (IllegalStateException e) {
        }

        long notExist = 1;
        while (notExist == gf.localMemberId()
               || notExist == peer.mMemberId || notExist == peer2.mMemberId)
        {
            notExist++;
        }

        assertFalse(gf.removePeer(notExist));

        assertTrue(gf.removePeer(peer.mMemberId));

        Set<Peer> allPeers = gf.allPeers();
        assertEquals(1, allPeers.size());
        assertFalse(allPeers.contains(peer));
        assertTrue(allPeers.contains(peer2));

        // Re-open.
        gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);

        allPeers = gf.allPeers();
        assertEquals(1, allPeers.size());
        assertFalse(allPeers.contains(peer));
        assertTrue(allPeers.contains(peer2));
    }

    @Test
    public void removePeerMessage() throws Exception {
        File f = TestUtils.newTempBaseFile(getClass());
        GroupFile gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);

        Peer peer = gf.addPeer(new InetSocketAddress("localhost", 1002), Role.OBSERVER);
        Peer peer2 = gf.addPeer(new InetSocketAddress("localhost", 1003), Role.STANDBY);

        byte[] message = gf.proposeRemovePeer((byte) -1, peer.mMemberId);

        assertEquals(-1, message[0]);

        assertTrue(gf.applyRemovePeer(message));

        Set<Peer> allPeers = gf.allPeers();
        assertEquals(1, allPeers.size());
        assertFalse(allPeers.contains(peer));
        assertTrue(allPeers.contains(peer2));

        // Capture the version.
        message = gf.proposeRemovePeer((byte) -1, peer2.mMemberId);

        // Update version.
        gf.updateRole(peer2.mMemberId, Role.NORMAL);

        // Version mismatch, so not removed.
        assertFalse(gf.applyRemovePeer(message));
    }

    @Test
    public void snapshot() throws Exception {
        File f = TestUtils.newTempBaseFile(getClass());
        GroupFile gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);

        Peer peer = gf.addPeer(new InetSocketAddress("localhost", 1002), Role.STANDBY);

        assertFalse(gf.discardJoinConsumer(new byte[1]));

        try {
            gf.applyJoin(123, new byte[1]);
            fail();
        } catch (ArrayIndexOutOfBoundsException e) {
        }

        byte[] message = gf.proposeJoin((byte) 1, null, (in, index) -> {});

        assertEquals(1, message[0]);

        assertTrue(gf.discardJoinConsumer(message));
        assertFalse(gf.discardJoinConsumer(message));
        assertNull(gf.applyJoin(123, message));

        File newFile = TestUtils.newTempBaseFile(getClass());

        message = gf.proposeJoin((byte) 1, null, (in, index) -> {
            try (FileOutputStream out = new FileOutputStream(newFile)) {
                byte[] buf = new byte[1000];
                int amt;
                while ((amt = in.read(buf)) > 0) {
                    out.write(buf, 0, amt);
                }
            } catch (Exception e) {
                throw Utils.rethrow(e);
            }
        });

        assertNull(gf.applyJoin(123, message));
        assertNull(gf.applyJoin(123, message));
        assertFalse(gf.discardJoinConsumer(message));

        GroupFile gf2 = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), false);

        assertEquals(gf.groupId(), gf2.groupId());
        assertEquals(gf.localMemberId(), gf2.localMemberId());
        assertEquals(new InetSocketAddress("localhost", 1001), gf2.localMemberAddress());
        assertEquals(Role.NORMAL, gf2.localMemberRole());

        Set<Peer> allPeers = gf2.allPeers();
        assertEquals(1, allPeers.size());
        assertTrue(allPeers.contains(peer));
    }

    @Test
    public void copyFullFileSmall() throws Exception {
        copyFullFile(false);
    }

    @Test
    public void copyFullFileLarge() throws Exception {
        copyFullFile(true);
    }

    private void copyFullFile(boolean large) throws Exception {
        File f = TestUtils.newTempBaseFile(getClass());
        GroupFile gf = GroupFile.open(null, f, new InetSocketAddress("localhost", 1001), true);
        Peer peer = gf.addPeer(new InetSocketAddress("localhost", 1002), Role.OBSERVER);
        Peer peer2 = gf.addPeer(new InetSocketAddress("localhost", 1003), Role.STANDBY);

        if (large) {
            for (int i=0; i<200; i++) {
                gf.addPeer(new InetSocketAddress("localhost", 2000 + i), Role.OBSERVER);
            }
        }

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        gf.writeTo(bout);
        byte[] copy = bout.toByteArray();

        if (large) {
            assertTrue(copy.length > 2000);
        }

        // Version unchanged.
        ByteArrayInputStream bin = new ByteArrayInputStream(copy);
        assertFalse(gf.readFrom(bin));
        assertTrue(bin.read() < 0);

        // Copy into a new group file object.
        File f2 = TestUtils.newTempBaseFile(getClass());
        try (FileOutputStream fout = new FileOutputStream(f2)) {
            fout.write(copy);
        }

        GroupFile gf2 = GroupFile.open(null, f2, new InetSocketAddress("localhost", 1001), false);

        assertEquals(gf.version(), gf2.version());

        // Update the first group file.
        gf.updateRole(peer2.mMemberId, Role.NORMAL);
        assertEquals(gf2.version() + 1, gf.version());

        // Write a new copy into the second group file.
        bout = new ByteArrayOutputStream();
        gf.writeTo(bout);
        // Write a bit more which should be ignored.
        bout.write(99);
        copy = bout.toByteArray();

        bin = new ByteArrayInputStream(copy);
        assertTrue(gf2.readFrom(bin));
        assertEquals(99, bin.read());
        assertTrue(bin.read() < 0);
        assertEquals(gf.version(), gf2.version());

        assertEquals(gf.allPeers(), gf2.allPeers());
    }

    @Test
    public void fullUpdate() throws Exception {
        // Test behavior of updating the group when calling readFile. Retained Peer instances
        // should be updated -- not replaced with new Peer instances.

        File f1 = TestUtils.newTempBaseFile(getClass());
        GroupFile gf1 = GroupFile.open(null, f1, new InetSocketAddress("localhost", 1000), true);
        Peer p1 = gf1.addPeer(new InetSocketAddress("localhost", 1001), Role.NORMAL);
        Peer p2 = gf1.addPeer(new InetSocketAddress("localhost", 1002), Role.NORMAL);

        // Update the group in a different object.
        GroupFile gf2 = GroupFile.open(null, f1, new InetSocketAddress("localhost", 1000), false);
        gf2.removePeer(p1.mMemberId);
        Peer p3 = gf2.addPeer(new InetSocketAddress("localhost", 1003), Role.NORMAL);
        gf2.updateRole(p2.mMemberId, Role.OBSERVER);
        Peer p4 = gf2.addPeer(new InetSocketAddress("localhost", 1004), Role.NORMAL);

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        gf2.writeTo(bout);
        gf1.readFrom(new ByteArrayInputStream(bout.toByteArray()));

        assertEquals(gf1.allPeers(), gf2.allPeers());

        // Updated instance should be the same.

        boolean found = false;

        for (Peer p : gf1.allPeers()) {
            if (p.equals(p2)) {
                assertFalse(found);
                assertEquals(Role.OBSERVER, p.mRole);
                assertTrue(p == p2);
                found = true;
            }
        }

        assertTrue(found);
    }
}
