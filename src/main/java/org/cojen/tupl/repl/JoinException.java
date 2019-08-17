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

import java.io.IOException;

import java.net.SocketAddress;

/**
 * Thrown when unable to join a replication group.
 *
 * @author Brian S O'Neill
 */
public class JoinException extends IOException {
    private static final long serialVersionUID = 1L;

    final SocketAddress mAddress;

    public JoinException() {
        mAddress = null;
    }

    public JoinException(String message) {
        this(message, null);
    }

    JoinException(String message, SocketAddress addr) {
        super(message);
        mAddress = addr;
    }

    @Override
    public String getMessage() {
        String message = super.getMessage();
        if (mAddress != null) {
            // Only construct the full message when something actually wants it.
            message += ": " + mAddress;
        }
        return message;
    }
}
