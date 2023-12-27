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

package org.cojen.tupl.remote;

import java.io.IOException;

import java.util.concurrent.TimeUnit;

import org.cojen.dirmi.RemoteException;
import org.cojen.dirmi.Session;
import org.cojen.dirmi.SessionAware;

import org.cojen.tupl.DatabaseException;
import org.cojen.tupl.DeadlockException;
import org.cojen.tupl.DurabilityMode;
import org.cojen.tupl.LockFailureException;
import org.cojen.tupl.LockMode;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.Transaction;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public final class ServerTransaction implements RemoteTransaction, SessionAware {
    public static Transaction txn(RemoteTransaction remote) {
        return remote == null ? null : ((ServerTransaction) remote).mTxn;
    }

    final Transaction mTxn;

    ServerTransaction(Transaction txn) {
        mTxn = txn;
    }

    @Override
    public void attached(Session<?> session) {
    }

    @Override
    public void detached(Session<?> session) {
        try {
            mTxn.reset();
        } catch (IOException e) {
            // Ignore.
        }
    }

    @Override
    public void lockMode(LockMode mode) {
        mTxn.lockMode(mode);
    }

    @Override
    public LockMode lockMode() {
        return mTxn.lockMode();
    }

    @Override
    public void lockTimeout(long timeout, TimeUnit unit) {
        mTxn.lockTimeout(timeout, unit);
    }

    @Override
    public void lockTimeoutNanos(long timeout) {
        mTxn.lockTimeout(timeout, TimeUnit.NANOSECONDS);
    }

    @Override
    public long lockTimeout(TimeUnit unit) {
        return mTxn.lockTimeout(unit);
    }

    @Override
    public long lockTimeoutNanos() {
        return mTxn.lockTimeout(TimeUnit.NANOSECONDS);
    }

    @Override
    public void durabilityMode(DurabilityMode dm) {
        mTxn.durabilityMode(dm);
    }

    @Override
    public DurabilityMode durabilityMode() {
        return mTxn.durabilityMode();
    }

    @Override
    public void check() throws DatabaseException {
        mTxn.check();
    }

    @Override
    public boolean isBogus() {
        return mTxn.isBogus();
    }

    @Override
    public void commit() throws IOException {
        mTxn.commit();
    }

    @Override
    public void commitAll() throws IOException {
        mTxn.commitAll();
    }

    @Override
    public void enter() throws IOException {
        mTxn.enter();
    }

    @Override
    public void exit() throws IOException {
        mTxn.exit();
    }

    @Override
    public void reset() throws IOException {
        mTxn.reset();
    }

    @Override
    public void reset(Throwable cause) throws RemoteException {
        mTxn.reset(cause);
    }

    @Override
    public void rollback() throws IOException {
        mTxn.rollback();
    }

    @Override
    public LockResult lockShared(long indexId, byte[] key) throws LockFailureException {
        return mTxn.lockShared(indexId, key);
    }

    @Override
    public LockResult lockUpgradable(long indexId, byte[] key) throws LockFailureException {
        return mTxn.lockUpgradable(indexId, key);
    }

    @Override
    public LockResult lockExclusive(long indexId, byte[] key) throws LockFailureException {
        return mTxn.lockExclusive(indexId, key);
    }

    @Override
    public boolean isNested() {
        return mTxn.isNested();
    }

    @Override
    public int nestingLevel() {
        return mTxn.nestingLevel();
    }

    @Override
    public LockResult tryLockShared(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException
    {
        return mTxn.tryLockShared(indexId, key, nanosTimeout);
    }

    @Override
    public LockResult lockShared(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return mTxn.lockShared(indexId, key, nanosTimeout);
    }

    @Override
    public LockResult tryLockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException
    {
        return mTxn.tryLockUpgradable(indexId, key, nanosTimeout);
    }

    @Override
    public LockResult lockUpgradable(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return mTxn.lockUpgradable(indexId, key, nanosTimeout);
    }

    @Override
    public LockResult tryLockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws DeadlockException, LockFailureException
    {
        return mTxn.tryLockExclusive(indexId, key, nanosTimeout);
    }

    @Override
    public LockResult lockExclusive(long indexId, byte[] key, long nanosTimeout)
        throws LockFailureException
    {
        return mTxn.lockExclusive(indexId, key, nanosTimeout);
    }

    @Override
    public LockResult lockCheck(long indexId, byte[] key) {
        return mTxn.lockCheck(indexId, key);
    }

    @Override
    public long lastLockedIndex() {
        return mTxn.lastLockedIndex();
    }

    @Override
    public byte[] lastLockedKey() {
        return mTxn.lastLockedKey();
    }

    @Override
    public boolean wasAcquired(long indexId, byte[] key) {
        return mTxn.wasAcquired(indexId, key);
    }

    @Override
    public void unlock() {
        mTxn.unlock();
    }

    @Override
    public void unlockToShared() {
        mTxn.unlockToShared();
    }

    @Override
    public void unlockCombine() {
        mTxn.unlockCombine();
    }

    @Override
    public long id() {
        return mTxn.id();
    }

    @Override
    public void flush() throws IOException {
        mTxn.flush();
    }

    @Override
    public void dispose() {
    }
}
