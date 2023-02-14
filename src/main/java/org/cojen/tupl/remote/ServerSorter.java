/*
 *  Copyright (C) 2023 Cojen.org
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

import java.net.SocketException;

import java.nio.channels.ClosedChannelException;

import org.cojen.dirmi.ClosedException;
import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Session;
import org.cojen.dirmi.SessionAware;

import org.cojen.tupl.Database;
import org.cojen.tupl.Entry;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Sorter;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ServerSorter implements RemoteSorter, SessionAware {
    private final Database mDb;
    private final Sorter mSorter;

    ServerSorter(Database db, Sorter sorter) {
        mDb = db;
        mSorter = sorter;
    }

    @Override
    public void attached(Session<?> session) {
    }

    @Override
    public void detached(Session<?> session) {
        try {
            reset();
        } catch (IOException e) {
            // Ignore.
        }
    }

    @Override
    public void add(byte[] key, byte[] value) throws IOException {
        mSorter.add(key, value);
    }

    @Override
    public Pipe addBatch(Pipe pipe) {
        byte[][] kvPairs;

        try {
            int size = pipe.readInt();
            kvPairs = new byte[size << 1][];
            int i = 0;
            do {
                kvPairs[i++] = (byte[]) pipe.readObject();
                kvPairs[i++] = (byte[]) pipe.readObject();
            } while (--size > 0);

            Exception ex = null;

            try {
                mSorter.addBatch(kvPairs, 0, kvPairs.length >> 1);
            } catch (IOException | RuntimeException e) {
                ex = e;
            }

            pipe.writeObject(ex);
            pipe.flush();
            pipe.recycle();
        } catch (IOException e) {
            Utils.closeQuietly(pipe);
        }

        return null;
    }

    public Pipe addAll(Pipe pipe) throws IOException {
        try {
            mSorter.addAll(new PipeEntryScanner(pipe, true));
        } catch (ClosedException | SocketException | ClosedChannelException e) {
            // Ignore.
        }
        return null;
    }

    @Override
    public RemoteIndex finish() throws IOException {
        return new ServerTemporaryIndex(mDb, mSorter.finish());
    }

    @Override
    public Pipe finishScan(boolean reverse, Pipe pipe) throws IOException {
        try {
            var scanner = reverse ? mSorter.finishScanReverse() : mSorter.finishScan();
            PipeEntryWriter.writeAll(scanner, pipe, true);
        } catch (ClosedException | SocketException | ClosedChannelException e) {
            // Ignore.
        }
        return null;
    }

    @Override
    public Pipe addAllFinishScan(boolean reverse, Pipe pipe) throws IOException {
        try {
            var srcScanner = new PipeEntryScanner(pipe, false);

            Scanner<Entry> scanner;
            if (reverse) {
                scanner = mSorter.finishScanReverse(srcScanner);
            } else {
                scanner = mSorter.finishScan(srcScanner);
            }

            PipeEntryWriter.writeAll(scanner, pipe, true);
        } catch (ClosedException | SocketException | ClosedChannelException e) {
            // Ignore. PipeEntryScanner or PipeEntryWriter should have cleaned things up.
        }

        return null;
    }

    @Override
    public long progress() {
        return mSorter.progress();
    }

    @Override
    public void reset() throws IOException {
        mSorter.reset();
    }
}
