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

package org.cojen.tupl.sql;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
class SqlUtils {
    static String unquote(String str) {
        int length = str.length();
        if (length >= 2 && str.charAt(0) == '\'' && str.charAt(length - 1) == '\'') {
            str = str.substring(1, length - 1);
            if (str.indexOf("''") >= 0) {
                str = str.replace("''", "'");
            }
        }
        return str;
    }
}
