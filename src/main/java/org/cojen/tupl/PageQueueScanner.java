/*
 *  Copyright 2016 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.cojen.tupl;

import java.io.IOException;

import java.util.function.LongConsumer;

import org.cojen.tupl.io.PageArray;

import static org.cojen.tupl.PageQueue.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class PageQueueScanner {
    /**
     * Scan all durable page ids in a page queue, passing them to the given consumer. Caller
     * must ensure that no changes are being made to the page queue.
     *
     * @param array durable page array
     * @param headerId typically 0 or 1
     * @param headerOffset offset into header where page queue is encoded
     * @param dst destination for scanned page ids
     */
    static void scan(PageArray array, long headerId, int headerOffset, LongConsumer dst)
        throws IOException
    {
        byte[] buf = new byte[array.pageSize()];
        array.readPage(headerId, buf);

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

        array.readPage(nodeId, buf);

        if (pageId == 0) {
            if (nodeOffset != I_NODE_START) {
                throw new CorruptDatabaseException("Invalid node offset: " + nodeOffset);
            }
            pageId = PageOps.p_longGetBE(buf, I_FIRST_PAGE_ID);
        }

        long actualPageCount = 0;
        long actualNodeCount = 1;

        IntegerRef.Value nodeOffsetRef = new IntegerRef.Value();
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

            array.readPage(nodeId, buf);

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
}
