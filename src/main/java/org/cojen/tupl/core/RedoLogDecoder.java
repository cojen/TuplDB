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

package org.cojen.tupl.core;

import java.io.EOFException;
import java.io.IOException;

import org.cojen.tupl.diag.EventListener;
import org.cojen.tupl.diag.EventType;

import org.cojen.tupl.util.Latch;

/**
 * Log operations written by {@link RedoLog} encode a special terminator, as a crude way to
 * detect if the log was truncated. This class decodes and verifies the terminators.
 *
 * @author Brian S O'Neill
 */
final class RedoLogDecoder extends RedoDecoder {
    private final EventListener mListener;

    RedoLogDecoder(DataIn in, EventListener listener) {
        super(true, 0, in, new Latch());
        mListener = listener;
    }

    @Override
    boolean verifyTerminator(DataIn in) throws IOException {
        try {
            if (in.readIntLE() == Utils.nzHash(mTxnId)) {
                return true;
            }
            if (mListener != null) {
                mListener.notify(EventType.RECOVERY_REDO_LOG_CORRUPTION,
                                 "Invalid message terminator");
            }
            return false;
        } catch (EOFException e) {
            if (mListener != null) {
                mListener.notify(EventType.RECOVERY_REDO_LOG_CORRUPTION,
                                 "Unexpected end of file");
            }
            return false;
        }
    }
}
