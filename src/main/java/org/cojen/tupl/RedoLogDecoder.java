/*
 *  Copyright 2013-2015 Cojen.org
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

import java.io.EOFException;
import java.io.IOException;

/**
 * 
 *
 * @author Brian S O'Neill
 * @see RedoLog
 */
/*P*/
final class RedoLogDecoder extends RedoDecoder {
    private final RedoLog mLog;
    private final DataIn mIn;
    private final EventListener mListener;

    RedoLogDecoder(RedoLog log, DataIn in, EventListener listener) {
        super(true, 0);
        mLog = log;
        mIn = in;
        mListener = listener;
    }

    @Override
    DataIn in() {
        return mIn;
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
