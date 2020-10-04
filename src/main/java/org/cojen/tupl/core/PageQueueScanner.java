/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl.core;

import java.io.IOException;

import java.util.function.LongConsumer;

import org.cojen.tupl.CorruptDatabaseException;

import org.cojen.tupl.io.PageArray;

import static org.cojen.tupl.core.PageQueue.*;

/**
 * Used by {@link StoredPageDb} to scan all pages in a free list, directly decoding the data
 * structure starting from the header page.
 *
 * @author Brian S O'Neill
 */
class PageQueueScanner {
    /**
     * Scan all stored page ids in a page queue, passing them to the given consumer. Caller
     * must ensure that no changes are being made to the page queue.
     *
     * @param array stored page array
     * @param headerId typically 0 or 1
     * @param headerOffset offset into header where page queue is encoded
     * @param dst destination for scanned page ids
     */
    static void scan(PageArray array, long headerId, int headerOffset, LongConsumer dst)
        throws IOException
    {
        long ptr = 0;
        int directPageSize = array.directPageSize();
        if (directPageSize < 0) {
            ptr = DirectPageOps.p_allocPage(directPageSize);
        }

        try {
            doScan(array, headerId, headerOffset, dst, ptr);
        } catch (Throwable e) {
            if (ptr != 0) {
                DirectPageOps.p_delete(ptr);
            }
            throw e;
        }
    }

    private static void doScan(PageArray array, long headerId, int headerOffset, LongConsumer dst,
                               long ptr)
        throws IOException
    {
        var buf = new byte[array.pageSize()];

        readPage(array, headerId, buf, ptr);

        final long pageCount = PageOps.p_longGetLE(buf, headerOffset + I_REMOVE_PAGE_COUNT);
        final long nodeCount = PageOps.p_longGetLE(buf, headerOffset + I_REMOVE_NODE_COUNT);
        long nodeId = PageOps.p_longGetLE(buf, headerOffset + I_REMOVE_HEAD_ID);
        int nodeOffset = PageOps.p_intGetLE(buf, headerOffset + I_REMOVE_HEAD_OFFSET);
        long pageId = PageOps.p_longGetLE(buf, headerOffset + I_REMOVE_HEAD_FIRST_PAGE_ID);
        final long tailId = PageOps.p_longGetLE(buf, headerOffset + I_APPEND_HEAD_ID);

        if (nodeId == 0) {
            if (pageCount != 0 || nodeCount != 0) {
                throw new CorruptDatabaseException
                    ("Invalid empty page queue: " + pageCount + ", " + nodeCount);
            }
            return;
        }

        readPage(array, nodeId, buf, ptr);

        if (pageId == 0) {
            if (nodeOffset != I_NODE_START) {
                throw new CorruptDatabaseException("Invalid node offset: " + nodeOffset);
            }
            pageId = PageOps.p_longGetBE(buf, I_FIRST_PAGE_ID);
        }

        long actualPageCount = 0;
        long actualNodeCount = 1;

        var nodeOffsetRef = new IntegerRef.Value();
        nodeOffsetRef.value = nodeOffset;

        while (true) {
            actualPageCount++;
            dst.accept(pageId);

            if (nodeOffsetRef.value < buf.length) {
                long delta = PageOps.p_ulongGetVar(buf, nodeOffsetRef);
                if (delta > 0) {
                    pageId += delta;
                    continue;
                }
            }

            // Pass along page queue node itself, and move to the next node. It doesn't get
            // counted towards the total page count.

            dst.accept(nodeId);

            nodeId = PageOps.p_longGetBE(buf, I_NEXT_NODE_ID);
            if (nodeId == tailId) {
                break;
            }

            readPage(array, nodeId, buf, ptr);

            actualNodeCount++;
            pageId = PageOps.p_longGetBE(buf, I_FIRST_PAGE_ID);
            nodeOffsetRef.value = I_NODE_START;
        }

        if (pageCount != actualPageCount) {
            throw new CorruptDatabaseException
                ("Mismatched page count: " + pageCount + " != " + actualPageCount);
        }

        if (nodeCount != actualNodeCount) {
            throw new CorruptDatabaseException
                ("Mismatched node count: " + nodeCount + " != " + actualNodeCount);
        }
    }

    private static void readPage(PageArray array, long id, byte[] buf, long ptr)
        throws IOException
    {
        if (ptr == 0) {
            array.readPage(id, buf);
        } else {
            array.readPage(id, ptr);
            DirectPageOps.p_copyToArray(ptr, 0, buf, 0, buf.length);
        }
    }
}
