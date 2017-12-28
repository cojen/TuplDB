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

import java.net.SocketAddress;

import java.util.Collections;
import java.util.Set;

/**
 * Simple command-line tool to remove replication group members. It connects to an existing
 * group member, finds the leader, and instructs it to remove the member. Operation can fail if
 * there is no group leader, or if the token doesn't match, or if the connection failed, or if
 * the member doesn't exist. Usage:
 *
 * <p>{@code RemoveMember --token <group token> --connect <existing member address> --remove <member to remove>}
 * 
 * <p>The member to remove can be specified by member id or by address:port.
 *
 * @author Brian S O'Neill
 */
public class RemoveMember {
    public static void main(String[] args) throws Exception {
        Long groupToken = null;
        SocketAddress connectAddr = null;
        long removeMemberId = 0;
        SocketAddress removeMemberAddr = null;

        try {
            int i = 0;
            loop: for (; i<args.length; i++) {
                switch (args[i]) {
                case "--token":
                    groupToken = Long.parseLong(args[++i]);
                    break;
                case "--connect":
                    connectAddr = GroupFile.parseSocketAddress(args[++i]);
                    break;
                case "--remove":
                    removeMemberAddr = GroupFile.parseSocketAddress(args[++i]);
                    if (removeMemberAddr == null) {
                        removeMemberId = Long.parseLong(args[i]);
                    }
                    break;
                default:
                    throw new Exception();
                }
            }

            if (groupToken == null || connectAddr == null ||
                (removeMemberId == 0 && removeMemberAddr == null))
            {
                throw new Exception();
            }
        } catch (Exception e) {
            System.out.println("Usage: RemoveMember --token <group token> --connect <existing member address> --remove <member to remove>");
            return;
        }

        Set<SocketAddress> seeds = Collections.singleton(connectAddr);
        GroupJoiner joiner = new GroupJoiner(groupToken);
        int timeoutMillis = 10000;

        try {
            if (removeMemberAddr != null) {
                joiner.unjoin(seeds, timeoutMillis, removeMemberAddr);
            } else {
                joiner.unjoin(seeds, timeoutMillis, removeMemberId);
            }
            System.out.println("Removed");
        } catch (JoinException e) {
            String message = e.getMessage();

            if (message != null) {
                if (message.indexOf("EOFException") >= 0) {
                    System.out.println("Incorrect group token");
                    return;
                }
                if (message.indexOf("ConnectException") >= 0) {
                    System.out.println("Connection refused");
                    return;
                }
                if (message.indexOf("invalid address") >= 0) {
                    System.out.println("Cannot remove group leader");
                    return;
                }
                if (message.indexOf("no leader") >= 0) {
                    System.out.println("No group leader");
                    return;
                }
                if (message.indexOf("timed out") >= 0) {
                    System.out.println("Operation timed out");
                    return;
                }
                if (message.indexOf("version mismatch") >= 0) {
                    System.out.println("Member not found (or concurrent group modification)");
                    return;
                }
            }

            System.out.println(e);
        }
    }
}
