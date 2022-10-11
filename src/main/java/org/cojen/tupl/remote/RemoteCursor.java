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

import org.cojen.dirmi.Batched;
import org.cojen.dirmi.Disposer;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Remote;
import org.cojen.dirmi.RemoteFailure;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RemoteCursor extends Remote {
    @RemoteFailure(declared=false)
    public byte ordering();

    @Batched
    @RemoteFailure(declared=false)
    public void link(RemoteTransaction txn);

    @RemoteFailure(declared=false)
    public byte[] key();

    @RemoteFailure(declared=false)
    public byte[] value();

    @Batched
    @RemoteFailure(declared=false)
    public void autoload(boolean mode);

    @RemoteFailure(declared=false)
    public int compareKeyTo(byte[] rkey);

    public boolean register() throws IOException;

    @Batched
    @RemoteFailure(declared=false)
    public void unregister();

    public byte first() throws IOException;

    public byte last() throws IOException;

    public byte skip(long amount) throws IOException;

    public byte skip(long amount, byte[] limitKey, boolean inclusive) throws IOException;

    public byte next() throws IOException;

    public byte nextLe(byte[] limitKey) throws IOException;

    public byte nextLt(byte[] limitKey) throws IOException;

    public byte previous() throws IOException;

    public byte previousGe(byte[] limitKey) throws IOException;

    public byte previousGt(byte[] limitKey) throws IOException;

    public byte find(byte[] key) throws IOException;

    public byte findGe(byte[] key) throws IOException;

    public byte findGt(byte[] key) throws IOException;

    public byte findLe(byte[] key) throws IOException;

    public byte findLt(byte[] key) throws IOException;

    public byte findNearby(byte[] key) throws IOException;

    public byte findNearbyGe(byte[] key) throws IOException;

    public byte findNearbyGt(byte[] key) throws IOException;

    public byte findNearbyLe(byte[] key) throws IOException;

    public byte findNearbyLt(byte[] key) throws IOException;

    public byte random(byte[] lowKey, byte[] highKey) throws IOException;

    public byte random(byte[] lowKey, boolean lowInclusive,
                       byte[] highKey, boolean highInclusive) throws IOException;

    public boolean exists() throws IOException;

    public byte lock() throws IOException;

    public byte load() throws IOException;

    public void store(byte[] value) throws IOException;

    public void delete() throws IOException;

    public void commit(byte[] value) throws IOException;

    @RemoteFailure(declared=false)
    public RemoteCursor copy();

    @Disposer
    @RemoteFailure(declared=false)
    public void reset();

    public long valueLength() throws IOException;

    public void valueLength(long length) throws IOException;

    public Pipe valueRead(long pos, int len, Pipe pipe) throws IOException;

    public Pipe valueWrite(long pos, int len, Pipe pipe) throws IOException;

    public void valueClear(long pos, long length) throws IOException;

    public Pipe newValueInputStream(long pos, Pipe pipe) throws IOException;

    public Pipe newValueInputStream(long pos, int bufferSize, Pipe pipe) throws IOException;

    public Pipe newValueOutputStream(long pos, Pipe pipe) throws IOException;

    public Pipe newValueOutputStream(long pos, int bufferSize, Pipe pipe) throws IOException;
}
