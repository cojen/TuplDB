/*
 *  Copyright 2019 Cojen.org
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

package org.cojen.tupl.ext;

import java.io.IOException;

import org.cojen.tupl.Database;
import org.cojen.tupl.DatabaseConfig;
import org.cojen.tupl.Transaction;
import org.cojen.tupl.UnmodifiableReplicaException;

import org.cojen.tupl.diag.EventType;

/**
 * Handler for prepared transactions. Instances which are passed to the {@link
 * DatabaseConfig#prepareHandlers prepareHandlers} method support recovery, and companion
 * instances for creating prepared transactions are provided by the {@link
 * Database#prepareWriter Database.prepareWriter} method.
 *
 * @author Brian S O'Neill
 */
public interface PrepareHandler extends Handler {
    /**
     * Called to prepare a transaction or to recover one. Exclusive locks acquired by the
     * transaction are always recovered, whether they were acquired explicitly or
     * automatically. Shared and upgradable locks aren't recovered.
     *
     * <p>When a transaction is initially prepared, non-exclusive locks are released, and the
     * application should attempt to drive the transaction to completion. If the application
     * crashes before this, or if leadership is lost, the recovery handler instance takes over.
     * It will be invoked on the group member which has become the new leader.
     *
     * <p>When an {@link UnmodifiableReplicaException} is thrown from a commit or rollback
     * operation, this signals that a recovery handler must take over. This exception can also
     * be thrown within recovery, which signals that a replacement recovery will take over. To
     * ensure that the handoff occurs, the reset method must be called on transaction which
     * threw the exception. Failing to do this might require that the application be restarted
     * to unstick the prepared transaction.
     *
     * <p>If an {@link UnmodifiableReplicaException} is thrown from the recovery handler, the
     * necessary reset is performed automatically. Any other exception is propagated as an
     * {@linkplain EventType#RECOVERY_HANDLER_UNCAUGHT uncaught} exception, and the prepared
     * transaction might remain in a stuck state.
     *
     * @param txn transaction the prepare applies to, which can be modified
     * @param message optional message
     * @throws NullPointerException if transaction or message is null
     */
    void prepare(Transaction txn, byte[] message) throws IOException;

    /**
     * A prepare variant intended to be called by a two-phase commit coordinator after all
     * participants have prepared their transactions. All changes made up to the prepare are
     * effectively committed, their locks are released, and the changes cannot be rolled
     * back. This permits the changes to be observed outside the transaction.
     *
     * <p>To fully complete the prepared transaction, a commit operation must still be called
     * to finish it, but only after all participants have committed their transactions. If not
     * finished, the recovery handler will be invoked after each failover. A rollback operation
     * can also finish the prepared transaction, although it will not roll back any changes
     * made before the prepare.
     *
     * <p>Strictly speaking, this variant isn't required. The benefit is that a coordinator can
     * perform the second phase of a two-phase commit without its own transaction getting
     * "stuck" while waiting for participants to commit. Another benefit is that no special
     * state needs to encoded in the prepare message to inform the recovery handler that it
     * should act as a coordinator. Being invoked by this method alone is all that's needed.
     *
     * @param txn transaction the prepare applies to, which can be modified
     * @param message optional message
     * @throws NullPointerException if transaction or message is null
     */
    void prepareCommit(Transaction txn, byte[] message) throws IOException;
}
