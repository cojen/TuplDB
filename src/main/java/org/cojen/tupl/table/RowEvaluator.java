/*
 *  Copyright (C) 2021 Cojen.org
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

import org.cojen.tupl.Cursor;
import org.cojen.tupl.LockResult;
import org.cojen.tupl.UnmodifiableViewException;

/**
 * See BasicScanner.
 *
 * @author Brian S O'Neill
 */
public interface RowEvaluator<R> extends RowDecoder<R> {
    /**
     * Returns the index id for the primary table that this evaluator is bound to, but only if
     * the table is evolvable. Zero is returned otherwise.
     */
    long evolvableTableId();

    /**
     * Returns the encoding descriptor, which is required for secondary indexes. Returns
     * null if this evaluator decodes against the primary table.
     *
     * @see RowStore#secondaryDescriptor
     */
    default byte[] secondaryDescriptor() {
        return null;
    }

    /**
     * Decodes a row unless it's filtered out. If a row instance is returned, then all
     * projected columns are marked clean.
     *
     * @param c refers the key and value to evaluate and decode; reset might be called to abort
     * the scan
     * @param result LockResult from cursor access; is only used to combine locks when joining
     * to a primary row
     * @param row can pass null to construct a new instance; can also be a RowConsumer
     * @return null if row is filtered out
     */
    R evalRow(Cursor c, LockResult result, R row) throws IOException;

    /**
     * Eval variant used when updating via a secondary index. By positioning a cursor over
     * the primary table, it can be updated directly without the cost of an additional search.
     *
     * @param secondary refers the key and value to evaluate and decode; reset might be called
     * to abort the scan
     * @param result LockResult from secondary cursor access; is only used to combine locks
     * when joining to a primary row
     * @param row can pass null to construct a new instance; can also be a RowConsumer
     * @param primary cursor is positioned as a side effect
     * @return null if row is filtered out
     */
    default R evalRow(Cursor secondary, LockResult result, R row, Cursor primary)
        throws IOException
    {
        throw new UnsupportedOperationException();
    }

    /**
     * Decode a row without filtering it out.
     *
     * @param row can pass null to construct a new instance
     * @return non-null row
     */
    @Override
    R decodeRow(R row, byte[] key, byte[] value) throws IOException;

    /**
     * Writes a header and row data via the RowWriter. This is used for remotely serializing
     * rows, and it's not intended to be used for persisting rows.
     */
    void writeRow(RowWriter writer, byte[] key, byte[] value) throws IOException;

    /**
     * Called by BasicUpdater.
     *
     * @return null if the key columns didn't change
     */
    default byte[] updateKey(R row, byte[] original) throws IOException {
        throw new UnmodifiableViewException();
    }

    /**
     * Called by BasicUpdater.
     *
     * @return non-null value
     */
    default byte[] updateValue(R row, byte[] original) throws IOException {
        throw new UnmodifiableViewException();
    }
}
