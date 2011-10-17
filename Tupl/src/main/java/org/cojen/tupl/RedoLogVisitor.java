/*
 *  Copyright 2011 Brian S O'Neill
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

/**
 * 
 *
 * @author Brian S O'Neill
 */
interface RedoLogVisitor {
    /**
     * @param indexId non-zero index id
     * @param key non-null key
     * @param value value to store; null to delete
     */
    void store(long indexId, byte[] key, byte[] value, DurabilityMode mode) throws IOException;

    /**
     * @param indexId non-zero index id
     */
    void clear(long indexId, DurabilityMode mode) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param parentTxnId parent transaction id; zero if none
     */
    void txnBegin(long txnId, long parentTxnId) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param parentTxnId parent transaction id; zero if none
     */
    void txnRollback(long txnId, long parentTxnId) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param parentTxnId parent transaction id; zero if none
     */
    void txnCommit(long txnId, long parentTxnId, DurabilityMode mode) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param indexId non-zero index id
     * @param key non-null key
     * @param value value to store; null to delete
     */
    void txnStore(long txnId, long indexId, byte[] key, byte[] value) throws IOException;
}
