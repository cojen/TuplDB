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
import org.cojen.dirmi.Data;
import org.cojen.dirmi.Disposer;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Remote;
import org.cojen.dirmi.RemoteFailure;

import org.cojen.tupl.LockResult;
import org.cojen.tupl.Ordering;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public interface RemoteCursor extends Remote {
    @Data
    public Ordering ordering();

    @Batched
    @RemoteFailure(declared=false)
    public void link(RemoteTransaction txn);

    @RemoteFailure(declared=false)
    public byte[] key();

    @RemoteFailure(declared=false)
    public Object value();

    @Batched
    @RemoteFailure(declared=false)
    public void autoload(boolean mode);

    @RemoteFailure(declared=false)
    public int compareKeyTo(byte[] rkey);

    public boolean register() throws IOException;

    @Batched
    @RemoteFailure(declared=false)
    public void unregister();

    public LockResult first() throws IOException;

    public LockResult last() throws IOException;

    public LockResult skip(long amount) throws IOException;

    public LockResult skip(long amount, byte[] limitKey, boolean inclusive) throws IOException;

    public LockResult next() throws IOException;

    public LockResult nextLe(byte[] limitKey) throws IOException;

    public LockResult nextLt(byte[] limitKey) throws IOException;

    public LockResult previous() throws IOException;

    public LockResult previousGe(byte[] limitKey) throws IOException;

    public LockResult previousGt(byte[] limitKey) throws IOException;

    public LockResult find(byte[] key) throws IOException;

    public LockResult findGe(byte[] key) throws IOException;

    public LockResult findGt(byte[] key) throws IOException;

    public LockResult findLe(byte[] key) throws IOException;

    public LockResult findLt(byte[] key) throws IOException;

    public LockResult findNearby(byte[] key) throws IOException;

    public LockResult findNearbyGe(byte[] key) throws IOException;

    public LockResult findNearbyGt(byte[] key) throws IOException;

    public LockResult findNearbyLe(byte[] key) throws IOException;

    public LockResult findNearbyLt(byte[] key) throws IOException;

    public LockResult random(byte[] lowKey, byte[] highKey) throws IOException;

    public LockResult random(byte[] lowKey, boolean lowInclusive,
                             byte[] highKey, boolean highInclusive) throws IOException;

    public boolean exists() throws IOException;

    public LockResult lock() throws IOException;

    public LockResult load() throws IOException;

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

    /**
     * Reads the value and transfers it down the pipe.
     */
    public Pipe valueReadTransfer(long pos, Pipe pipe) throws IOException;

    /**
     * Reads the value and transfers it down the pipe.
     */
    public Pipe valueReadTransfer(long pos, int bufferSize, Pipe pipe) throws IOException;

    /**
     * Consumes data from the pipe and writes the value.
     */
    public Pipe valueWriteTransfer(long pos, Pipe pipe) throws IOException;

    /**
     * Test method.
     */
    public boolean equalPositions(RemoteCursor other) throws IOException;

    /**
     * Test method.
     */
    public boolean verifyExtremities(byte extremity) throws IOException;
}
