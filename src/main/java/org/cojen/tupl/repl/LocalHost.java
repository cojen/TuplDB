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

import java.net.InetAddress;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;

import java.util.Enumeration;

/**
 * Utility for obtaining a non-loopback local host. Returns a loopback address as a last
 * resort.
 *
 * @author Brian S O'Neill
 */
class LocalHost {
    public static void main(String[] args) throws Exception {
        System.out.println(getLocalHost());
    }

    private static volatile InetAddress cLocalAddress;

    public static InetAddress getLocalHost() throws UnknownHostException {
        InetAddress local = cLocalAddress;
        if (local == null) synchronized (LocalHost.class) {
            local = cLocalAddress;
            if (local == null) {
                cLocalAddress = local = doGetLocalHost();
            }
        }
        return local;
    }

    private static InetAddress doGetLocalHost() throws UnknownHostException {
        final InetAddress local = InetAddress.getLocalHost();

        if (!local.isLoopbackAddress()) {
            // Windows typically has a proper local address.
            return local;
        }

        // Linux typically doesn't have a proper local address (must hack /etc/hosts), and so
        // this huge mess is required as a workaround.

        NetworkInterface ni = null;

        try {
            Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
            en = NetworkInterface.getNetworkInterfaces();
            while (en.hasMoreElements()) {
                NetworkInterface n = en.nextElement();
                if (!n.isLoopback()) {
                    ni = n;
                    break;
                }
            }
        } catch (SocketException e) {
            if (ni == null) {
                UnknownHostException u = new UnknownHostException(e.getMessage());
                u.initCause(e);
                throw u;
            }
        }

        if (ni == null) {
            return local;
        }

        Inet4Address v4 = null;
        Inet6Address v6 = null;

        Enumeration<InetAddress> en = ni.getInetAddresses();

        while (en.hasMoreElements()) {
            InetAddress a = en.nextElement();
            if (a instanceof Inet4Address && v4 == null) {
                v4 = (Inet4Address) a;
            } else if (a instanceof Inet6Address && v6 == null) {
                v6 = (Inet6Address) a;
            }
        }

        InetAddress actual;

        if (v4 == null) {
            if (v6 == null) {
                return local;
            }
            actual = v6;
        } else if (v6 == null || Boolean.getBoolean("java.net.preferIPv4Stack")) {
            actual = v4;
        } else {
            actual = v6;
        }

        String name = actual.getHostName();

        if (name.equals(actual.getHostAddress())) {
            name = local.getHostName();
            if (name.equals(local.getHostAddress())) {
                name = null;
            }
        }

        return InetAddress.getByAddress(name, actual.getAddress());
    }
}
