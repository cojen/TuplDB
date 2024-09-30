/*
 *  Copyright (C) 2024 Cojen.org
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

package org.cojen.tupl.table;

import java.io.IOException;

import org.cojen.dirmi.Pipe;

import org.cojen.tupl.Scanner;
import org.cojen.tupl.Table;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.remote.RemoteTableProxy;

import org.cojen.tupl.views.ViewUtils;

/**
 * Base class for all generated server-side table classes.
 *
 * @author Brian S. O'Neill
 */
public abstract class BaseTable<R> extends MultiCache<Object, Object, Object, IOException>
    implements Table<R>
{
    /**
     * Scan and write all rows of this table to a remote endpoint. This method doesn't flush
     * the output stream. Note: This method is overridden in StoredTable to operate against
     * binary encoded rows directly.
     */
    public void scanWrite(Transaction txn, Pipe out) throws IOException {
        var writer = new RowWriter.ForEncoder<R>(out);

        try {
            scanWrite(writer, newScanner(txn));
        } catch (RuntimeException | IOException e) {
            writer.writeTerminalException(e);
            return;
        }

        writer.writeTerminator();
    }

    /**
     * Scan and write a subset of rows from this table to a remote endpoint. This method
     * doesn't flush the output stream. Note: This method is overridden in StoredTable to
     * operate against binary encoded rows directly.
     */
    public void scanWrite(Transaction txn, Pipe out, String queryStr, Object... args)
        throws IOException
    {
        var writer = new RowWriter.ForEncoder<R>(out);

        try {
            scanWrite(writer, newScanner(txn, queryStr, args));
        } catch (RuntimeException | IOException e) {
            writer.writeTerminalException(e);
            return;
        }

        writer.writeTerminator();
    }

    private void scanWrite(RowWriter.ForEncoder<R> writer, Scanner<R> scanner) throws IOException {
        try {
            WriteRow<R> wr = WriteRow.find(rowType());
            writer.writeCharacteristics(scanner.characteristics(), scanner.estimateSize());
            for (R row = scanner.row(); row != null; row = scanner.step(row)) {
                wr.writeRow(writer, row);
            }
        } catch (Throwable e) {
            Utils.closeQuietly(scanner);
            throw e;
        }
    }

    /**
     * Note: This method is overridden in StoredTable to support evolvable tables.
     *
     * @param descriptor describes the binary row encoding; see RowHeader
     */
    public RemoteTableProxy newRemoteProxy(byte[] descriptor) throws IOException {
        return RemoteProxyMaker.make(this, 0, descriptor);
    }

    public Transaction enterScope(Transaction txn) throws IOException {
        return ViewUtils.enterScope(this, txn);
    }
}
