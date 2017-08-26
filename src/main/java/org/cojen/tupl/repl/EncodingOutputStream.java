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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;

import java.nio.charset.StandardCharsets;

import java.util.Arrays;
import java.util.Map;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class EncodingOutputStream extends ByteArrayOutputStream {
    protected Writer mWriter;

    EncodingOutputStream() {
    }

    EncodingOutputStream(int size) {
        super(size);
    }

    public void encodeIntLE(int value) {
        int pos = count;
        if (pos + 4 > buf.length) {
            buf = Arrays.copyOf(buf, buf.length << 1);
        }
        Utils.encodeIntLE(buf, pos, value);
        count = pos + 4;
    }

    public void encodeLongLE(long value) {
        int pos = count;
        if (pos + 8 > buf.length) {
            buf = Arrays.copyOf(buf, buf.length << 1);
        }
        Utils.encodeLongLE(buf, pos, value);
        count = pos + 8;
    }

    public void encodeStr(String str) {
        try {
            Writer writer = mWriter;
            if (writer == null) {
                mWriter = writer = new OutputStreamWriter(this, StandardCharsets.UTF_8);
            }
            encodeIntLE(str.length());
            writer.write(str);
            writer.flush();
        } catch (IOException e) {
            // Not expected.
            throw Utils.rethrow(e);
        }
    }

    public void encodeMap(Map<String, String> map) {
        encodeIntLE(map.size());
        for (Map.Entry<String, String> e : map.entrySet()) {
            encodeStr(e.getKey());
            encodeStr(e.getValue());
        }
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        if (mWriter != null) {
            mWriter.flush();
        }
        super.writeTo(out);
    }

    protected final void defaultWriteTo(OutputStream out) throws IOException {
        super.writeTo(out);
    }
}
