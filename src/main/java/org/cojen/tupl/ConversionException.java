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
 * Thrown when converting the value of a row column would result in data loss.
 *
 * @author Brian S O'Neill
 */
public class ConversionException extends RuntimeException {
    private static final long serialVersionUID = 1L;


    public ConversionException(String message) {
        super(message);
    }

    public ConversionException(String message, String columnName) {
        super(messageFor(message, columnName));
    }

    public ConversionException(String message, String columnName, Throwable cause) {
        super(messageFor(message, columnName), cause);
    }

    private static String messageFor(String message, String columnName) {
        if (message == null) {
            return columnName == null ? "" : ("column=" + columnName);
        }  else {
            return columnName == null ? message : (message + "; column=" + columnName);
        }
    }
}
