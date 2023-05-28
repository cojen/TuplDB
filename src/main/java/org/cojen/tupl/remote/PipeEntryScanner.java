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

import java.lang.invoke.MethodHandle;

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.Entry;
import org.cojen.tupl.Scanner;

import org.cojen.tupl.core.EntryPopulator;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see PipeEntryWriter
 */
final class PipeEntryScanner implements Scanner<Entry> {
    private static final MethodHandle POPULATOR = EntryPopulator.THE;

    private final long mSize;
    private final int mCharacteristics;

    private final boolean mRecycle;

    private Pipe mPipe;
    private Entry mEntry;

    PipeEntryScanner(Pipe pipe, boolean recycle) throws IOException {
        try {
            mSize = pipe.readLong();
            mCharacteristics = pipe.readInt();
        } catch (IOException e) {
            Utils.closeQuietly(pipe);
            throw e;
        }

        mRecycle = recycle;

        mPipe = pipe;
        step();
    }

    @Override
    public long estimateSize() {
        return mSize;
    }

    @Override
    public int characteristics() {
        return mCharacteristics;
    }

    @Override
    public Entry row() {
        return mEntry;
    }

    @Override
    public Entry step(Entry row) throws IOException {
        return doStep(row);
    }

    private Entry doStep(Entry row) throws IOException {
        Pipe pipe = mPipe;

        if (pipe == null) {
            return null;
        }

        byte[] key;
        try {
            key = (byte[]) pipe.readObject();
        } catch (Exception e) {
            Utils.closeQuietly(this);
            throw e;
        }

        if (key == null) {
            mEntry = null;
            mPipe = null;
            if (mRecycle) {
                pipe.write(1); // ack
                pipe.flush();
                pipe.recycle();
            }
            return null;
        }

        byte[] value;
        try {
            value = (byte[]) pipe.readObject();
            mEntry = row = (Entry) POPULATOR.invokeExact(row, key, value);
            return row;
        } catch (Throwable e) {
            Utils.closeQuietly(this);
            throw Utils.rethrow(e);
        }
    }

    @Override
    public void close() throws IOException {
        Pipe pipe = mPipe;
        if (pipe != null) {
            mEntry = null;
            mPipe = null;
            pipe.close();
        }
    }
}
