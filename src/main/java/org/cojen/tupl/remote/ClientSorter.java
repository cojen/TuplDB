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

import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.RemoteException;

import org.cojen.tupl.Entry;
import org.cojen.tupl.Index;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Sorter;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.util.Runner;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ClientSorter implements Sorter {
    private final ClientDatabase mDb;

    private volatile RemoteSorter mSorter;

    ClientSorter(ClientDatabase db) {
        mDb = db;
    }

    public void add(byte[] key, byte[] value) throws IOException {
        sorter().add(key, value);
    }

    public void addBatch(byte[][] kvPairs, int offset, int size) throws IOException {
        if (size > 0) {
            int end = size << 1;
            if (end < 0 || end > kvPairs.length) {
                throw new IndexOutOfBoundsException();
            }

            Exception ex;

            Pipe pipe = sorter().addBatch(null);
            try {
                pipe.writeInt(size);

                int i = 0;
                do {
                    pipe.writeObject(kvPairs[i++]);
                    pipe.writeObject(kvPairs[i++]);
                } while (--size > 0);

                pipe.flush();

                ex = (Exception) pipe.readThrowable();
                
                pipe.recycle();
            } catch (IOException e) {
                Utils.closeQuietly(pipe);
                throw e;
            }

            if (ex != null) {
                throw Utils.rethrow(ex);
            }
        }
    }

    public void addAll(Scanner<Entry> s) throws IOException {
        PipeEntryWriter.writeAll(s, sorter().addAll(null), true);
    }

    public Index finish() throws IOException {
        return new ClientIndex.Temp(mDb, finishSorter().finish());
    }

    public Scanner<Entry> finishScan() throws IOException {
        return finishScan(false);
    }

    public Scanner<Entry> finishScanReverse() throws IOException {
        return finishScan(true);
    }

    private Scanner<Entry> finishScan(boolean reverse) throws IOException {
        Pipe pipe = sorter().finishScan(reverse, null);
        try {
            pipe.flush();
        } catch (IOException e) {
            Utils.closeQuietly(pipe);
        }
        return new PipeEntryScanner(pipe, true);
    }

    public Scanner<Entry> finishScan(Scanner<Entry> s) throws IOException {
        return finishScan(false, s);
    }

    public Scanner<Entry> finishScanReverse(Scanner<Entry> s) throws IOException {
        return finishScan(true, s);
    }

    public Scanner<Entry> finishScan(boolean reverse, Scanner<Entry> s) throws IOException {
        Pipe pipe = sorter().addAllFinishScan(reverse, null);

        Runner.start(() -> {
            try {
                PipeEntryWriter.writeAll(s, pipe, false);
            } catch (IOException e) {
                // Ignore. PipeEntryWriter should have cleaned things up.
            }
        });

        return new PipeEntryScanner(pipe, true);
    }

    public long progress() {
        try {
            RemoteSorter sorter = mSorter;
            return sorter == null ? 0 : sorter.progress();
        } catch (RemoteException e) {
            return 0;
        }
    }

    public synchronized void reset() throws IOException {
        RemoteSorter sorter = mSorter;
        if (sorter != null) {
            mSorter = null;
            sorter.reset();
        }
    }

    private RemoteSorter sorter() throws RemoteException {
        RemoteSorter sorter = mSorter;

        if (sorter == null) {
            synchronized (this) {
                sorter = mSorter;
                if (sorter == null) {
                    mSorter = sorter = mDb.newRemoteSorter();
                }
            }
        }

        return sorter;
    }

    /**
     * Clears the mSorter field in anticipation of it being disposed.
     */
    private synchronized RemoteSorter finishSorter() throws RemoteException {
        RemoteSorter sorter = mSorter;
        if (sorter == null) {
            sorter = mDb.newRemoteSorter();
        } else {
            mSorter = null;
        }
        return sorter;
    }
}
