/*
 *  Copyright 2011-2012 Brian S O'Neill
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
interface RedoVisitor {
    /**
     * @return false to stop visiting
     */
    public boolean timestamp(long timestamp) throws IOException;

    /**
     * @return false to stop visiting
     */
    public boolean shutdown(long timestamp) throws IOException;

    /**
     * @return false to stop visiting
     */
    public boolean close(long timestamp) throws IOException;

    /**
     * @return false to stop visiting
     */
    public boolean endFile(long timestamp) throws IOException;

    /**
     * @param indexId non-zero index id
     * @param key non-null key
     * @param value value to store; null to delete
     * @return false to stop visiting
     */
    public boolean store(long indexId, byte[] key, byte[] value) throws IOException;

    /**
     * @param txnId non-zero transaction id
     */
    public boolean txnBegin(long txnId) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param parentTxnId parent transaction id; zero if none
     * @return false to stop visiting
     */
    public boolean txnBeginChild(long txnId, long parentTxnId) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @return false to stop visiting
     */
    public boolean txnRollback(long txnId) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param parentTxnId parent transaction id; zero if none
     * @return false to stop visiting
     */
    public boolean txnRollbackChild(long txnId, long parentTxnId) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @return false to stop visiting
     */
    public boolean txnCommit(long txnId) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param parentTxnId parent transaction id; zero if none
     * @return false to stop visiting
     */
    public boolean txnCommitChild(long txnId, long parentTxnId) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param indexId non-zero index id
     * @param key non-null key
     * @param value value to store; null to delete
     * @return false to stop visiting
     */
    public boolean txnStore(long txnId, long indexId, byte[] key, byte[] value) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param indexId non-zero index id
     * @param key non-null key
     * @param value value to store; null to delete
     * @return false to stop visiting
     */
    public boolean txnStoreCommit(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param indexId non-zero index id
     * @param key non-null key
     * @param value value to store; null to delete
     * @return false to stop visiting
     */
    public boolean txnStoreCommitChild(long txnId, long parentTxnId,
                                       long indexId, byte[] key, byte[] value) throws IOException;
}
