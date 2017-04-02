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
     * @return false to stop visiting
     */
    public boolean fence() throws IOException;

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
    public boolean txnEnterStore(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException;

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
    public boolean txnStoreCommitFinal(long txnId, long indexId, byte[] key, byte[] value)
        throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param indexId non-zero index id
     * @param key non-null key
     * @return false to stop visiting
     */
    public boolean txnLockShared(long txnId, long indexId, byte[] key) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param indexId non-zero index id
     * @param key non-null key
     * @return false to stop visiting
     */
    public boolean txnLockUpgradable(long txnId, long indexId, byte[] key) throws IOException;

    /**
     * @param txnId non-zero transaction id
     * @param indexId non-zero index id
     * @param key non-null key
     * @return false to stop visiting
     */
    public boolean txnLockExclusive(long txnId, long indexId, byte[] key) throws IOException;

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
