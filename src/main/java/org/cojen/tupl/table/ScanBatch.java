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

package org.cojen.tupl.table;

import java.io.IOException;

import org.cojen.tupl.Sorter;

/**
 * @author Brian S O'Neill
 * @see RowSorter
 */
class ScanBatch<R> {
    private Block mFirstBlock;
    private Block mLastBlock;

    private ScanBatch<R> mNextBatch;

    RowEvaluator<R> mEvaluator;

    ScanBatch() {
        mFirstBlock = mLastBlock = new Block();
    }

    final void addEntry(byte[] key, byte[] value) {
        Block block = mLastBlock;

        byte[][] entries = block.mEntries;
        int size = block.mSize;

        if (size >= entries.length) {
            block = block.extend();
            mLastBlock = block;
            entries = block.mEntries;
            size = 0;
        }

        entries[size++] = key;
        entries[size++] = value;
        block.mSize = size;
    }

    /**
     * Decode rows for just this batch.
     *
     * @return updated offset
     */
    private int decodeRows(R[] rows, int offset) throws IOException {
        Block block = mFirstBlock;

        mFirstBlock = null;
        mLastBlock = null;

        do {
            byte[][] entries = block.mEntries;
            for (int i=0; i<entries.length; i+=2) {
                byte[] key = entries[i];
                if (key == null) {
                    return offset;
                }
                byte[] value = entries[i + 1];
                rows[offset++] = mEvaluator.decodeRow(null, key, value);
            }
        } while ((block = block.mNextBlock) != null);

        return offset;
    }

    /**
     * Decode rows for all batches, starting with this one.
     *
     * @return updated offset
     */
    final int decodeAllRows(R[] rows, int offset) throws IOException {
        ScanBatch<R> batch = this;
        do {
            offset = batch.decodeRows(rows, offset);
        } while ((batch = batch.detachNext()) != null);
        return offset;
    }

    /**
     * Transcode the rows in this batch and add them to the given sorter.
     *
     * @param kvPairs temporary workspace; can pass null initially
     * @return new or original kvPairs
     */
    final byte[][] transcode(Transcoder t, Sorter sorter, byte[][] kvPairs) throws IOException {
        {
            int capacity = mLastBlock.mEntries.length;
            if (kvPairs == null || kvPairs.length < capacity) {
                kvPairs = new byte[capacity][];
            }
        }

        Block block = mFirstBlock;

        mFirstBlock = null;
        mLastBlock = null;

        do {
            byte[][] entries = block.mEntries;
            int i = 0;
            for (; i<entries.length; i+=2) {
                byte[] key = entries[i];
                if (key == null) {
                    break;
                }
                t.transcode(key, entries[i + 1], kvPairs, i);
            }
            sorter.addBatch(kvPairs, 0, i >> 1);
        } while ((block = block.mNextBlock) != null);
        
        return kvPairs;
    }

    final void appendNext(ScanBatch<R> batch) {
        mNextBatch = batch;
    }

    final ScanBatch<R> detachNext() {
        ScanBatch<R> next = mNextBatch;
        mNextBatch = null;
        mEvaluator = null;
        return next;
    }

    static final class Block {
        private static final int FIRST_BLOCK_CAPACITY = 16;
        private static final int HIGHEST_BLOCK_CAPACITY = 1024;

        // Alternating key and values.
        private final byte[][] mEntries;

        private int mSize;

        private Block mNextBlock;

        Block() {
            this(FIRST_BLOCK_CAPACITY);
        }

        Block(int capacity) {
            mEntries = new byte[capacity][];
        }

        Block extend() {
            var block = new Block(Math.min(mEntries.length << 1, HIGHEST_BLOCK_CAPACITY));
            mNextBlock = block;
            return block;
        }
    }
}
