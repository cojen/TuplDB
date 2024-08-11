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

package org.cojen.tupl;

/**
 * Thrown when a page of data from the database is detected as being corrupt because the
 * stored checksum doesn't match what was freshly computed.
 *
 * @author Brian S. O'Neill
 */
public class ChecksumException extends CorruptDatabaseException {
    private static final long serialVersionUID = 1L;

    private final long mStored, mComputed;

    public ChecksumException(long pageId, int storedChecksum, int computedChecksum) {
        this(pageId, storedChecksum & 0xffff_ffffL, computedChecksum & 0xffff_ffffL);
    }

    public ChecksumException(long pageId, long storedChecksum, long computedChecksum) {
        this("Checksum mismatch for page " + pageId + ": " +
             Long.toUnsignedString(storedChecksum, 16) + " != " +
             Long.toUnsignedString(computedChecksum, 16),
             storedChecksum, computedChecksum);
    }

    public ChecksumException(String message, long storedChecksum, long computedChecksum) {
        super(message);
        mStored = storedChecksum;
        mComputed = computedChecksum;
    }

    public long storedChecksum() {
        return mStored;
    }

    public long computedChecksum() {
        return mComputed;
    }

    @Override
    public boolean isRecoverable() {
        // Although the corrupt page cannot be recovered, the rest of the database might be fine.
        return true;
    }
}
