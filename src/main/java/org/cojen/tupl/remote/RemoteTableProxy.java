/*
 *  Copyright (C) 2023 Cojen.org
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

import org.cojen.dirmi.Pipe;
import org.cojen.dirmi.Remote;

/**
 * Supports table operations using a remote interface that operates over pipes, permitting
 * binary encoded rows to be transported over the network. The server-side implementation is
 * provided by RemoteProxyMaker, and client-side support is provided by ClientTableHelper.
 *
 * @author Brian S O'Neill
 */
public interface RemoteTableProxy extends Remote {
    /**
     * Simplified server-side implementation:
     *
     * <ol>
     * <li>If the RemoteTransaction isn't null, then it's cast to ServerTransaction for
     * obtaining the Transaction object.
     * <li>The binary key is read from the pipe and not decoded.
     * <li>The load method is called on the table's source Index.
     * <li>If no exception, null is written to the pipe. Otherwise, the exception is written
     * and no normal response is written.
     * <li>A byte value of 0 is written if no value was loaded, or else 1 is written followed
     * by the binary encoded value.
     * <li>The pipe is flushed, recycled, and then the method returns null.
     * </ol>
     */
    public Pipe tryLoad(RemoteTransaction txn, Pipe pipe) throws IOException;

    /**
     * Simplified server-side implementation:
     *
     * <ol>
     * <li>If the RemoteTransaction isn't null, then it's cast to ServerTransaction for
     * obtaining the Transaction object.
     * <li>The binary key is read from the pipe and not decoded.
     * <li>The exists method is called on the table's source Index.
     * <li>If no exception, null is written to the pipe. Otherwise, the exception is written
     * and no normal response is written.
     * <li>A byte value of 0 is written if no value exists, or else 1 is written.
     * <li>The pipe is flushed, recycled, and then the method returns null.
     * </ol>
     */
    public Pipe exists(RemoteTransaction txn, Pipe pipe) throws IOException;

    /**
     * Simplified server-side implementation:
     *
     * <ol>
     * <li>If the RemoteTransaction isn't null, then it's cast to ServerTransaction for
     * obtaining the Transaction object.
     * <li>The binary key and value is read from the pipe and not decoded.
     * <li>The storeAndTrigger method is called on the table.
     * <li>If no exception, null is written to the pipe. Otherwise, the exception is written
     * and no normal response is written.
     * <li>A byte value of 1 is written to the pipe to indicate success.
     * <li>The pipe is flushed, recycled, and then the method returns null.
     * </ol>
     */
    public Pipe store(RemoteTransaction txn, Pipe pipe) throws IOException;

    /**
     * Simplified server-side implementation:
     *
     * <ol>
     * <li>If the RemoteTransaction isn't null, then it's cast to ServerTransaction for
     * obtaining the Transaction object.
     * <li>The binary key and value is read from the pipe and not decoded.
     * <li>The exchangeAndTrigger method is called on the table.
     * <li>If no exception, null is written to the pipe. Otherwise, the exception is written
     * and no normal response is written.
     * <li>A byte value of 0 is written if the old value was null, or else 1 is written
     * followed by the binary encoded old value.
     * <li>The pipe is flushed, recycled, and then the method returns null.
     * </ol>
     */
    public Pipe exchange(RemoteTransaction txn, Pipe pipe) throws IOException;

    /**
     * Simplified server-side implementation:
     *
     * <ol>
     * <li>If the RemoteTransaction isn't null, then it's cast to ServerTransaction for
     * obtaining the Transaction object.
     * <li>The binary key and value is read from the pipe and not decoded.
     * <li>The insertAndTrigger method is called on the table.
     * <li>If no exception, null is written to the pipe. Otherwise, the exception is written
     * and no normal response is written.
     * <li>A byte value of 0 is written if nothing was inserted, or else 1 is written.
     * <li>The pipe is flushed, recycled, and then the method returns null.
     * </ol>
     */
    public Pipe tryInsert(RemoteTransaction txn, Pipe pipe) throws IOException;

    /**
     * Simplified server-side implementation:
     *
     * <ol>
     * <li>If the RemoteTransaction isn't null, then it's cast to ServerTransaction for
     * obtaining the Transaction object.
     * <li>The binary key and value is read from the pipe and not decoded.
     * <li>The replaceAndTrigger method is called on the table.
     * <li>If no exception, null is written to the pipe. Otherwise, the exception is written
     * and no normal response is written.
     * <li>A byte value of 0 is written if nothing was replaced, or else 1 is written.
     * <li>The pipe is flushed, recycled, and then the method returns null.
     * </ol>
     */
    public Pipe tryReplace(RemoteTransaction txn, Pipe pipe) throws IOException;

    /**
     * Simplified server-side implementation:
     *
     * <ol>
     * <li>If the RemoteTransaction isn't null, then it's cast to ServerTransaction for
     * obtaining the Transaction object.
     * <li>The binary key and value is read from the pipe and not decoded.
     * <li>The updateAndTrigger method is called on the table.
     * <li>If no exception, null is written to the pipe. Otherwise, the exception is written
     * and no normal response is written.
     * <li>A byte value of 0 is written if nothing was updated, or else 1 is written.
     * <li>The pipe is flushed, recycled, and then the method returns null.
     * </ol>
     */
    public Pipe tryUpdate(RemoteTransaction txn, Pipe pipe) throws IOException;

    /**
     * Simplified server-side implementation:
     *
     * <ol>
     * <li>If the RemoteTransaction isn't null, then it's cast to ServerTransaction for
     * obtaining the Transaction object.
     * <li>The binary key and value is read from the pipe and not decoded.
     * <li>The updateAndTrigger method is called on the table.
     * <li>If no exception, null is written to the pipe. Otherwise, the exception is written
     * and no normal response is written.
     * <li>A byte value of 0 is written if nothing was merged, or else 1 is written followed by
     * the binary encoded merged value.
     * <li>The pipe is flushed, recycled, and then the method returns null.
     * </ol>
     */
    public Pipe tryMerge(RemoteTransaction txn, Pipe pipe) throws IOException;

    /**
     * Simplified server-side implementation:
     *
     * <ol>
     * <li>If the RemoteTransaction isn't null, then it's cast to ServerTransaction for
     * obtaining the Transaction object.
     * <li>The binary key is read from the pipe and not decoded.
     * <li>The deleteAndTrigger method is called on the table.
     * <li>If no exception, null is written to the pipe. Otherwise, the exception is written
     * and no normal response is written.
     * <li>A byte value of 0 is written if nothing was deleted, or else 1 is written.
     * <li>The pipe is flushed, recycled, and then the method returns null.
     * </ol>
     */
    public Pipe tryDelete(RemoteTransaction txn, Pipe pipe) throws IOException;

    /**
     * Simplified server-side implementation:
     *
     * <ol>
     * <li>The RemoteUpdater is cast to ServerUpdater for obtaining the Updater object.
     * <li>The Updater.row method is called.
     * <li>If the method returns null, then null (no exception) and a byte value of 0 are
     * written to the pipe. Jump to the flush and recycle step.
     * <li>The binary key and value of the current updater row is encoded.
     * <li>If an exception is produced while encoding (conversion error), the exception is
     * written and no normal response is written.
     * <li>A byte value 1 is written to the pipe, followed by the binary key and value.
     * <li>The pipe is flushed, recycled, and then the method returns null.
     * </ol>
     */
    public Pipe row(RemoteUpdater updater, Pipe pipe) throws IOException;

    /**
     * Simplified server-side implementation:
     *
     * <ol>
     * <li>The RemoteUpdater is cast to ServerUpdater for obtaining the Updater object.
     * <li>The Updater.step method is called.
     * <li>If the method returns null, then null (no exception) and a byte value of 0 are
     * written to the pipe. Jump to the flush and recycle step.
     * <li>The binary key and value of the next updater row is encoded.
     * <li>If an exception is produced while encoding (conversion error), the exception is
     * written and no normal response is written.
     * <li>A byte value 1 is written to the pipe, followed by the binary key and value.
     * <li>The pipe is flushed, recycled, and then the method returns null.
     * </ol>
     */
    public Pipe step(RemoteUpdater updater, Pipe pipe) throws IOException;

    /**
     * Simplified server-side implementation:
     *
     * <ol>
     * <li>The RemoteUpdater is cast to ServerUpdater for obtaining the Updater object.
     * <li>The set/dirty key columns and dirty value columns are read from the pipe and changes
     * are applied to the current updater row.
     * <li>The Updater.update method is called.
     * <li>If the method returns null, then null (no exception) and a byte value of 0 are
     * written to the pipe. Jump to the flush and recycle step.
     * <li>The binary key and value of the next updater row is encoded.
     * <li>If an exception is produced while encoding (conversion error), the exception is
     * written and no normal response is written.
     * <li>A byte value 1 is written to the pipe, followed by the binary key and value.
     * <li>The pipe is flushed, recycled, and then the method returns null.
     * </ol>
     */
    public Pipe update(RemoteUpdater updater, Pipe pipe) throws IOException;

    /**
     * Simplified server-side implementation:
     *
     * <ol>
     * <li>The RemoteUpdater is cast to ServerUpdater for obtaining the Updater object.
     * <li>The set/dirty key columns are read from the pipe and changes are applied to the
     * current updater row.
     * <li>The Updater.delete method is called.
     * <li>If the method returns null, then null (no exception) and a byte value of 0 are
     * written to the pipe. Jump to the flush and recycle step.
     * <li>The binary key and value of the next updater row is encoded.
     * <li>If an exception is produced while encoding (conversion error), the exception is
     * written and no normal response is written.
     * <li>A byte value 1 is written to the pipe, followed by the binary key and value.
     * <li>The pipe is flushed, recycled, and then the method returns null.
     * </ol>
     */
    public Pipe delete(RemoteUpdater updater, Pipe pipe) throws IOException;
}
