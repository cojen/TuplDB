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

import java.net.InetSocketAddress;

import java.util.Set;

import org.junit.*;
import static org.junit.Assert.*;

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
            GroupFile.open(null, null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }

        File f = TestUtils.newTempBaseFile(getClass());

        try {
            GroupFile.open(f, null, true);
            fail();
        } catch (IllegalArgumentException e) {
        }

        assertNull(GroupFile.open(f, new InetSocketAddress("localhost", 1001), false));

        GroupFile gf = GroupFile.open(f, new InetSocketAddress("localhost", 1001), true);

        assertEquals(1, gf.version());

        long groupId = gf.groupId();
        assertTrue(groupId != 0);

        long localMemberId = gf.localMemberId();
        assertTrue(localMemberId != 0);

        assertEquals(new InetSocketAddress("localhost", 1001), gf.localMemberAddress());
        assertEquals(Role.NORMAL, gf.localMemberRole());
        assertEquals(0, gf.allPeers().size());

        // Re-open with wrong address.
        try {
            gf = GroupFile.open(f, new InetSocketAddress("localhost", 1002), true);
            fail();
        } catch (IllegalStateException e) {
        }
        
        // Re-open with wrong address.
        try {
            gf = GroupFile.open(f, new InetSocketAddress("localhost", 1002), true);
            fail();
        } catch (IllegalStateException e) {
        }

        // Re-open correctly.
        gf = GroupFile.open(f, new InetSocketAddress("localhost", 1001), true);

        assertEquals(groupId, gf.groupId());
        assertEquals(localMemberId, gf.localMemberId());

        assertEquals(new InetSocketAddress("localhost", 1001), gf.localMemberAddress());
        assertEquals(Role.NORMAL, gf.localMemberRole());
        assertEquals(0, gf.allPeers().size());
    }

    @Test
    public void addPeer() throws Exception {
        File f = TestUtils.newTempBaseFile(getClass());
        GroupFile gf = GroupFile.open(f, new InetSocketAddress("localhost", 1001), true);

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

        try {
            gf.addPeer(new InetSocketAddress("localhost", 1002), Role.STANDBY);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().indexOf("peer") > 0);
        }

        Peer peer2 = gf.addPeer(new InetSocketAddress("localhost", 1003), Role.STANDBY);
        assertTrue(peer2.mMemberId != 0);
        assertEquals(new InetSocketAddress("localhost", 1003), peer2.mAddress);
        assertEquals(Role.STANDBY, peer2.mRole);

        allPeers = gf.allPeers();
        assertEquals(2, allPeers.size());

        // Re-open.
        gf = GroupFile.open(f, new InetSocketAddress("localhost", 1001), true);

        assertEquals(groupId, gf.groupId());
        assertEquals(localMemberId, gf.localMemberId());

        allPeers = gf.allPeers();
        assertEquals(2, allPeers.size());
        assertTrue(allPeers.contains(peer));
        assertTrue(allPeers.contains(peer2));

        for (Peer p : allPeers) {
            if (p.equals(peer)) {
                assertEquals(new InetSocketAddress("localhost", 1002), p.mAddress);
                assertEquals(Role.OBSERVER, p.mRole);
            } else {
                assertEquals(new InetSocketAddress("localhost", 1003), p.mAddress);
                assertEquals(Role.STANDBY, p.mRole);
            }
        }
    }

    @Test
    public void addPeerMessage() throws Exception {
        File f = TestUtils.newTempBaseFile(getClass());
        GroupFile gf = GroupFile.open(f, new InetSocketAddress("localhost", 1001), true);

        byte[] message = gf.proposeAddPeer
            ((byte) 10, new InetSocketAddress("localhost", 1002), Role.OBSERVER);

        assertEquals(10, message[0]);

        Peer peer = gf.applyAddPeer(message);

        assertTrue(peer.mMemberId != 0);
        assertEquals(new InetSocketAddress("localhost", 1002), peer.mAddress);
        assertEquals(Role.OBSERVER, peer.mRole);

        Set<Peer> allPeers = gf.allPeers();
        assertEquals(1, allPeers.size());
        assertEquals(peer, allPeers.iterator().next());

        // Capture the version.
        message = gf.proposeAddPeer
            ((byte) 1, new InetSocketAddress("localhost", 1003), Role.NORMAL);

        // Update version.
        gf.updateRole(peer.mMemberId, Role.NORMAL);

        // Version mismatch, so peer not added.
        assertNull(gf.applyAddPeer(message));
    }

    @Test
    public void updateRole() throws Exception {
        File f = TestUtils.newTempBaseFile(getClass());
        GroupFile gf = GroupFile.open(f, new InetSocketAddress("localhost", 1001), true);
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
        gf = GroupFile.open(f, new InetSocketAddress("localhost", 1001), true);

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
        GroupFile gf = GroupFile.open(f, new InetSocketAddress("localhost", 1001), true);
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
        GroupFile gf = GroupFile.open(f, new InetSocketAddress("localhost", 1001), true);

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
        gf = GroupFile.open(f, new InetSocketAddress("localhost", 1001), true);

        allPeers = gf.allPeers();
        assertEquals(1, allPeers.size());
        assertFalse(allPeers.contains(peer));
        assertTrue(allPeers.contains(peer2));
    }

    @Test
    public void removePeerMessage() throws Exception {
        File f = TestUtils.newTempBaseFile(getClass());
        GroupFile gf = GroupFile.open(f, new InetSocketAddress("localhost", 1001), true);

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
}
