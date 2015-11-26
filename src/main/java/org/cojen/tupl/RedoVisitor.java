/*
 *  Copyright 2011-2015 Cojen.org
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
    public boolean reset() throws IOException;

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
     * @param indexId non-zero index id
     * @param key non-null key
     * @param value value to store; null to delete
     * @return false to stop visiting
     */
    public boolean storeNoLock(long indexId, byte[] key, byte[] value) throws IOException;

    /**
     * @param indexId non-zero index id
     * @param newName non-null new index name
     * @return false to stop visiting
     */
    public boolean renameIndex(long txnId, long indexId, byte[] newName) throws IOException;

    /**
     * @param indexId non-zero index id
     * @return false to stop visiting
     */
    public boolean deleteIndex(long txnId, long indexId) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @return false to stop visiting
     */
    public boolean txnEnter(long txnId) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @return false to stop visiting
     */
    public boolean txnRollback(long txnId) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @return false to stop visiting
     */
    public boolean txnRollbackFinal(long txnId) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @return false to stop visiting
     */
    public boolean txnCommit(long txnId) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @return false to stop visiting
     */
    public boolean txnCommitFinal(long txnId) throws IOException;

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
    public boolean txnStoreCommitFinal(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param message custom message
     */
    public boolean txnCustom(long txnId, byte[] message) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param message custom message
     */
    public boolean txnCustomLock(long txnId, byte[] message, long indexId, byte[] key)
        throws IOException;
}
