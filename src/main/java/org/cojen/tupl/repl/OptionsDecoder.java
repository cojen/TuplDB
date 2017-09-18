/*
 *  Copyright (C) 2017 Cojen.org
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

package org.cojen.tupl.repl;

import java.io.InputStream;
import java.io.IOException;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
final class OptionsDecoder extends DecodingInputStream {
    OptionsDecoder(InputStream in) throws IOException {
        super(decode(in));
    }

    private static byte[] decode(InputStream in) throws IOException {
        byte[] buf = new byte[4];
        Utils.readFully(in, buf, 0, buf.length);
        buf = new byte[Utils.decodeIntLE(buf, 0) - 4];
        Utils.readFully(in, buf, 0, buf.length);
        return buf;
    }
}
