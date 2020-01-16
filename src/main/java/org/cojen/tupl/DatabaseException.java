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

import java.io.IOException;

import java.util.concurrent.TimeUnit;

import org.cojen.tupl.core.Utils;

/**
 * Exception thrown which indicates a {@linkplain Database database} problem not due to general
 * I/O problems.
 *
 * @author Brian S O'Neill
 */
public class DatabaseException extends IOException {
    private static final long serialVersionUID = 1L;

    public DatabaseException() {
    }

    public DatabaseException(String message) {
        super(message);
    }

    public DatabaseException(Throwable cause) {
        super(cause);
    }

    public DatabaseException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Returns false if database should be closed as a result of this exception.
     */
    public boolean isRecoverable() {
        return false;
    }

    /*
     * Applicable to timeout exceptions.
     */
    long timeout() {
        return 0;
    }

    /*
     * Applicable to timeout exceptions.
     */
    TimeUnit unit() {
        return null;
    }

    /*
     * Applicable to timeout exceptions.
     */
    Object ownerAttachment() {
        return null;
    }

    final String timeoutMessage(long nanosTimeout) {
        String msg;
        if (nanosTimeout == 0) {
            msg = "Never waited";
        } else if (nanosTimeout < 0) {
            msg = "Infinite wait";
        } else {
            var b = new StringBuilder("Waited ");
            Utils.appendTimeout(b, timeout(), unit());
            Object att = ownerAttachment();
            if (att != null) {
                appendAttachment(b, att);
            }
            return b.toString();
        }

        Object att = ownerAttachment();
        if (att != null) {
            var b = new StringBuilder(msg);
            appendAttachment(b, att);
            msg = b.toString();
        }

        return msg;
    }

    private static void appendAttachment(StringBuilder b, Object att) {
        b.append("; owner attachment: ").append(att);
    }
}
