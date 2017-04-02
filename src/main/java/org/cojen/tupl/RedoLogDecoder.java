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

package org.cojen.tupl;

import java.io.EOFException;
import java.io.IOException;

import org.cojen.tupl.util.Latch;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see RedoLog
 */
/*P*/
final class RedoLogDecoder extends RedoDecoder {
    private final RedoLog mLog;
    private final EventListener mListener;

    RedoLogDecoder(RedoLog log, DataIn in, EventListener listener) {
        super(true, 0, in, new Latch());
        mLog = log;
        mListener = listener;
    }

    @Override
    boolean verifyTerminator(DataIn in) throws IOException {
        try {
            int term = in.readIntLE();
            if (term == mLog.nextTermRnd() || term == Utils.nzHash(mTxnId)) {
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
