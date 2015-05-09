/*
 *  Copyright 2012-2013 Brian S O'Neill
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

import java.util.concurrent.locks.Lock;

import static java.lang.System.arraycopy;

import static org.cojen.tupl.PageOps.*;
import static org.cojen.tupl.Utils.*;

/**
 * Persisted collection of fragmented values which should be deleted. Trash is
 * emptied after transactions commit and during recovery.
 *
 * @author Brian S O'Neill
 */
final class FragmentedTrash {
    final Tree mTrash;

    /**
     * @param trash internal index for persisting trash
     */
    FragmentedTrash(Tree trash) {
        mTrash = trash;
    }

    /**
     * Copies a fragmented value to the trash and pushes an entry to the undo
     * log. Caller must hold commit lock.
     *
     * @param entry Node page; starts with variable length key
     * @param keyStart inclusive index into entry for key; includes key header
     * @param keyLen length of key
     * @param valueStart inclusive index into entry for fragmented value; excludes value header
     * @param valueLen length of value
     */
    void add(Transaction txn, long indexId,
             /*P*/ byte[] entry, int keyStart, int keyLen, int valueStart, int valueLen)
        throws IOException
    {
        // It would be nice if cursor store supported array slices. Instead, a
        // temporary array needs to be created.
        byte[] payload = new byte[valueLen];
        p_copyToArray(entry, valueStart, payload, 0, valueLen);

        TreeCursor cursor = prepareEntry(txn.txnId());
        byte[] key = cursor.key();
        try {
            // Write trash entry first, ensuring that the undo log entry will
            // refer to something valid.
            txn.setHasTrash();
            cursor.store(payload);
            cursor.reset();
        } catch (Throwable e) {
            txn.borked(e, false);
            throw closeOnFailure(cursor, e);
        }

        // Now write the undo log entry.

        int tidLen = key.length - 8;
        int payloadLen = keyLen + tidLen;
        if (payloadLen > payload.length) {
            // Cannot re-use existing temporary array.
            payload = new byte[payloadLen];
        }
        p_copyToArray(entry, keyStart, payload, 0, keyLen);
        arraycopy(key, 8, payload, keyLen, tidLen);

        txn.pushUndeleteFragmented(indexId, payload, 0, payloadLen);
    }

    /**
     * Returns a cursor ready to store a new trash entry. Caller must reset or
     * close the cursor when done.
     */
    private TreeCursor prepareEntry(long txnId) throws IOException {
        // Key entry format is transaction id prefix, followed by a variable
        // length integer. Integer is reverse encoded, and newer entries within
        // the transaction have lower integer values.

        byte[] prefix = new byte[8];
        encodeLongBE(prefix, 0, txnId);

        TreeCursor cursor = new TreeCursor(mTrash, Transaction.BOGUS);
        try {
            cursor.autoload(false);
            cursor.findGt(prefix);
            byte[] key = cursor.key();
            if (key == null || compareKeys(key, 0, 8, prefix, 0, 8) != 0) {
                // Create first entry for this transaction.
                key = new byte[8 + 1];
                arraycopy(prefix, 0, key, 0, 8);
                key[8] = (byte) 0xff;
                cursor.findNearby(key);
            } else {
                // Decrement from previously created entry. Although key will
                // be modified, it doesn't need to be cloned because no
                // transaction was used by the search. The key instance is not
                // shared with the lock manager.
                cursor.findNearby(decrementReverseUnsignedVar(key, 8));
            }
            return cursor;
        } catch (Throwable e) {
            throw closeOnFailure(cursor, e);
        }
    }

    /**
     * Remove an entry from the trash, as an undo operation. Original entry is
     * stored back into index.
     */
    void remove(long txnId, Tree index, byte[] undoEntry) throws IOException {
        // Extract the index and trash keys.

        /*P*/ byte[] undo = p_transfer(undoEntry);

        byte[] indexKey, trashKey;
        try {
            DatabaseAccess dbAccess = mTrash.mRoot;
            indexKey = Node.retrieveKeyAtLoc(dbAccess, undo, 0);

            int tidLoc = Node.keyLengthAtLoc(undo, 0);
            int tidLen = p_length(undo) - tidLoc;
            trashKey = new byte[8 + tidLen];
            encodeLongBE(trashKey, 0, txnId);
            p_copyToArray(undo, tidLoc, trashKey, 8, tidLen);
        } finally {
            p_delete(undo);
        }

        byte[] fragmented;
        TreeCursor cursor = new TreeCursor(mTrash, Transaction.BOGUS);
        try {
            cursor.find(trashKey);
            fragmented = cursor.value();
            if (fragmented == null) {
                // Nothing to remove, possibly caused by double undo.
                cursor.reset();
                return;
            }
            cursor.store(null);
            cursor.reset();
        } catch (Throwable e) {
            throw closeOnFailure(cursor, e);
        }

        cursor = new TreeCursor(index, Transaction.BOGUS);
        try {
            cursor.find(indexKey);
            cursor.storeFragmented(fragmented);
            cursor.reset();
        } catch (Throwable e) {
            throw closeOnFailure(cursor, e);
        }
    }

    /**
     * Non-transactionally deletes all fragmented values for the given
     * top-level transaction.
     */
    void emptyTrash(long txnId) throws IOException {
        byte[] prefix = new byte[8];
        encodeLongBE(prefix, 0, txnId);

        Database db = mTrash.mDatabase;
        final Lock sharedCommitLock = db.sharedCommitLock();
        TreeCursor cursor = new TreeCursor(mTrash, Transaction.BOGUS);
        try {
            cursor.autoload(false);
            cursor.findGt(prefix);
            while (true) {
                byte[] key = cursor.key();
                if (key == null || compareKeys(key, 0, 8, prefix, 0, 8) != 0) {
                    break;
                }
                cursor.load();
                /*P*/ byte[] fragmented = p_transfer(cursor.value());
                sharedCommitLock.lock();
                try {
                    db.deleteFragments(fragmented, 0, p_length(fragmented));
                    cursor.store(null);
                } finally {
                    sharedCommitLock.unlock();
                    p_delete(fragmented);
                }
                cursor.next();
            }
            cursor.reset();
        } catch (Throwable e) {
            throw closeOnFailure(cursor, e);
        }
    }

    /**
     * Non-transactionally deletes all fragmented values. Expected to be called
     * only during recovery.
     *
     * @return true if any trash was found
     */
    boolean emptyAllTrash(EventListener listener) throws IOException {
        boolean found = false;
        Database db = mTrash.mDatabase;
        final Lock sharedCommitLock = db.sharedCommitLock();
        TreeCursor cursor = new TreeCursor(mTrash, Transaction.BOGUS);
        try {
            cursor.first();
            if (cursor.key() != null) {
                if (listener != null) {
                    listener.notify(EventType.RECOVERY_DELETE_FRAGMENTS,
                                    "Deleting unused large fragments");
                }
                found = true;
                do {
                    /*P*/ byte[] fragmented = p_transfer(cursor.value());
                    try {
                        sharedCommitLock.lock();
                        try {
                            db.deleteFragments(fragmented, 0, p_length(fragmented));
                            cursor.store(null);
                        } finally {
                            sharedCommitLock.unlock();
                        }
                    } finally {
                        p_delete(fragmented);
                    }
                    cursor.next();
                } while (cursor.key() != null);
            }
            cursor.reset();
        } catch (Throwable e) {
            throw closeOnFailure(cursor, e);
        }
        return found;
    }
}
