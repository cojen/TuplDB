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

import org.cojen.dirmi.RemoteException;

import org.cojen.tupl.Updater;

import org.cojen.tupl.table.ClientTableHelper;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class ClientUpdater<R> implements Updater<R> {
    private final ClientTableHelper<R> mHelper;
    private final RemoteTableProxy mProxy;
    private final int mCharacteristics;
    private final long mSize;

    private RemoteUpdater mUpdater;
    private R mRow;

    /**
     * @param updater can be null if no rows exists; if non-null, an initial row must be provided
     * @param row initial row
     */
    ClientUpdater(ClientTableHelper<R> helper, RemoteTableProxy proxy,
                  int characteristics, long size, RemoteUpdater updater, R row)
    {
        mHelper = helper;
        mProxy = proxy;
        mCharacteristics = characteristics;
        mSize = size;
        mUpdater = updater;
        mRow = row;
    }

    @Override
    public long estimateSize() {
        return mSize;
    }

    @Override
    public int characteristics() {
        return mCharacteristics;
    }

    @Override
    public R row() {
        return mRow;
    }

    @Override
    public R step() throws IOException {
        RemoteUpdater updater = mUpdater;
        return updater == null ? null : doStep(updater, mHelper.newRow());
    }

    @Override
    public R step(R row) throws IOException {
        RemoteUpdater updater = mUpdater;
        return updater == null ? null : doStep(updater, row);
    }

    private R doStep(RemoteUpdater updater, R row) throws IOException {
        if (!mHelper.updaterStep(row, mProxy.step(updater, null))) {
            row = null;
            dispose(updater);
        }
        mRow = row;
        return row;
    }

    @Override
    public R update() throws IOException {
        return doUpdate(updater(), mHelper.newRow());
    }

    @Override
    public R update(R row) throws IOException {
        if (row == null) {
            row = mHelper.newRow();
        }
        return doUpdate(updater(), row);
    }

    private R doUpdate(RemoteUpdater updater, R newRow) throws IOException {
        if (!mHelper.updaterUpdate(mRow, newRow, mProxy.update(updater, null))) {
            newRow = null;
            dispose(updater);
        }
        mRow = newRow;
        return newRow;
    }

    @Override
    public R delete() throws IOException {
        return doDelete(updater(), mHelper.newRow());
    }

    @Override
    public R delete(R row) throws IOException {
        if (row == null) {
            row = mHelper.newRow();
        }
        return doDelete(updater(), row);
    }

    private R doDelete(RemoteUpdater updater, R newRow) throws IOException {
        if (!mHelper.updaterDelete(mRow, newRow, mProxy.delete(updater, null))) {
            newRow = null;
            dispose(updater);
        }
        mRow = newRow;
        return newRow;
    }

    @Override
    public void close() throws IOException {
        RemoteUpdater updater = mUpdater;
        if (updater != null) {
            mUpdater = null;
            mRow = null;
            updater.close();
        }
    }

    private void dispose(RemoteUpdater updater) {
        mUpdater = null;
        try {
            updater.dispose();
        } catch (RemoteException e) {
            // Ignore.
        }
    }

    private RemoteUpdater updater() {
        RemoteUpdater updater = mUpdater;
        if (updater == null) {
            throw new IllegalStateException("No current row");
        }
        return updater;
    }
}
