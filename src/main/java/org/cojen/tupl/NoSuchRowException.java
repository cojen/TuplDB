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

package org.cojen.tupl;

/**
 * Thrown when attempting to read or write against a nonexistent row which was expected to
 * exist.
 *
 * @author Brian S. O'Neill
 */
public class NoSuchRowException extends DatabaseException {
    private static final long serialVersionUID = 1L;

    public NoSuchRowException() {
    }

    public NoSuchRowException(String message) {
        super(message);
    }

    @Override
    public boolean isRecoverable() {
        return true;
    }
}
