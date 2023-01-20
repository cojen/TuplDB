/*
 *  Copyright (C) 2022 Cojen.org
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
 *  along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.remote;

import java.io.IOException;
import java.io.OutputStream;

import java.util.Map;

import org.cojen.dirmi.ClosedException;
import org.cojen.dirmi.Pipe;

import org.cojen.tupl.Snapshot;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ClientSnapshot implements Snapshot {
    private final Map mMap;

    ClientSnapshot(Map map) {
        mMap = map;
    }

    @Override
    public long length() {
        return (long) mMap.get("length");
    }

    @Override
    public long position() {
        return (long) mMap.get("position");
    }

    @Override
    public boolean isCompressible() {
        return (boolean) mMap.get("isCompressible");
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        try (Pipe pipe = snapshot().writeTo(null)) {
            pipe.flush();
            pipe.inputStream().transferTo(out);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            snapshot().close();
        } catch (ClosedException e) {
            // Ignore.
        }
    }

    private RemoteSnapshot snapshot() {
        return (RemoteSnapshot) mMap.get("snapshot");
    }
}
