/*
 *  Copyright (C) 2025 Cojen.org
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

import org.cojen.tupl.table.BaseTable;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
final class ServerDerivedTable<R> extends ServerTable<R> implements RemoteTable {
    private final byte[] mDescriptor;

    ServerDerivedTable(BaseTable<R> table, byte[] descriptor) throws IOException {
        super(table);
        mDescriptor = descriptor;
    }

    @Override
    public byte[] descriptor() {
        return mDescriptor;
    }
}
