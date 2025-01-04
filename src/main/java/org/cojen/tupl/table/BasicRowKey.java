/*
 *  Copyright (C) 2025 Cojen.org
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

package org.cojen.tupl.table;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import java.util.function.BiConsumer;

import org.cojen.tupl.Ordering;
import org.cojen.tupl.RowKey;

import org.cojen.tupl.util.Canonicalizer;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public final class BasicRowKey implements RowKey {
    public static RowKey empty() {
        return Empty.THE;
    }

    private static final Canonicalizer cCache = new Canonicalizer();

    /**
     * @param columnNames not cloned
     * @param orderings not cloned
     */
    public static RowKey from(String[] columnNames, Ordering[] orderings) {
        if (columnNames.length != orderings.length) {
            throw new IllegalArgumentException();
        }
        return columnNames.length == 0 ? Empty.THE : doFrom(columnNames, orderings);
    }

    private static BasicRowKey doFrom(String[] columnNames, Ordering[] orderings) {
        return cCache.apply(new BasicRowKey(columnNames, orderings));
    }

    public static RowKey from(Map<String, ColumnInfo> columns) {
        int size = columns.size();

        if (size == 0) {
            return Empty.THE;
        }

        var columnNames = new String[size];
        var orderings = new Ordering[size];

        int pos = 0;

        for (Map.Entry<String, ColumnInfo> e : columns.entrySet()) {
            columnNames[pos] = e.getKey();

            ColumnInfo info = e.getValue();
            Ordering ordering = info.isDescending() ? Ordering.DESCENDING : Ordering.ASCENDING;
            if (info.isNullLow()) {
                ordering = ordering.nullsLow();
            }

            orderings[pos] = ordering;
            pos++;
        }

        return doFrom(columnNames, orderings);
    }

    public static RowKey primaryKey(Class<?> rowType) {
        return from(RowInfo.find(rowType).keyColumns);
    }

    public static RowKey parse(String spec, boolean keepSpec) {
        try {
            return doParse(spec, keepSpec);
        } catch (IndexOutOfBoundsException e) {
            throw malformed(spec);
        }
    }

    private static RowKey doParse(String spec, boolean keepSpec) {
        int length = spec.length();

        if (length == 0) {
            return Empty.THE;
        }

        int size = 0;

        for (int ix = 0; ix < length; ix++) {
            switch (spec.charAt(ix)) {
                case '+', '-', '~' -> size++;
            }
        }

        var columnNames = new String[size];
        var orderings = new Ordering[size];

        for (int pos = 0, ix = 0; ix < length; pos++) {
            var ordering = switch (spec.charAt(ix++)) {
                case '+' -> Ordering.ASCENDING;
                case '-' -> Ordering.DESCENDING;
                case '~' -> Ordering.UNSPECIFIED;
                default -> throw malformed(spec);
            };

            int c = spec.charAt(ix);

            if (c == '!') {
                if (ordering == Ordering.UNSPECIFIED) {
                    throw malformed(spec);
                }
                ordering = ordering.nullsLow();
                ix++;
            }

            orderings[pos] = ordering;

            int end = ix;
            skip: while (end < length) {
                switch (spec.charAt(end)) {
                    case '+', '-', '~' -> { break skip; }
                }
                end++;
            }

            if (end == ix) {
                throw malformed(spec);
            }

            columnNames[pos] = spec.substring(ix, end);

            ix = end;
        }

        BasicRowKey rk = doFrom(columnNames, orderings);

        if (keepSpec) {
            rk.mSpec = spec;
        }

        return rk;
    }

    private static IllegalArgumentException malformed(String spec) {
        return new IllegalArgumentException("Malformed specification: " + spec);
    }

    private final String[] mColumnNames;
    private final Ordering[] mOrderings;

    private String mSpec;

    private BasicRowKey(String[] columnNames, Ordering[] orderings) {
        mColumnNames = columnNames;
        mOrderings = orderings;
    }

    @Override
    public int size() {
        return mColumnNames.length;
    }

    @Override
    public String column(int pos) {
        return mColumnNames[pos];
    }

    @Override
    public Ordering ordering(int i) {
        return mOrderings[i];
    }

    @Override
    public String spec() {
        String spec = mSpec;
        if (spec == null) {
            var b = new StringBuilder(size() * 10);
            appendSpec(this, b);
            mSpec = spec = b.toString().intern();
        }
        return spec;
    }

    @Override
    public void appendSpec(StringBuilder b) {
        String spec = mSpec;
        if (spec == null) {
            appendSpec(this, b);
        } else {
            b.append(spec);
        }
    }

    public static void appendSpec(RowKey rk, StringBuilder b) {
        rk.forEach((name, ordering) -> {
            b.append(switch (ordering) {
                case ASCENDING, ASCENDING_NL -> '+';
                case DESCENDING, DESCENDING_NL -> '-';
                default -> '~';
            });

            if (ordering.areNullsLow()) {
                b.append('!');
            }

            b.append(name);
        });
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(mColumnNames) ^ Arrays.hashCode(mOrderings);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (!(obj instanceof RowKey rk)) {
            return false;
        }
        if (rk instanceof BasicRowKey brk) {
            return Arrays.equals(mColumnNames, brk.mColumnNames)
                && Arrays.equals(mOrderings, brk.mOrderings);
        }
        int size = size();
        if (size != rk.size()) {
            return false;
        }
        for (int i=0; i<size; i++) {
            if (!Objects.equals(mColumnNames[i], rk.column(i)) || mOrderings[i] != rk.ordering(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return toString(this);
    }

    static String toString(RowKey rk) {
        var b = new StringBuilder(rk.size() * 10).append("RowKey").append('(');
        rk.appendSpec(b);
        return b.append(')').toString();
    }

    private static final class Empty implements RowKey {
        public static final Empty THE = new Empty();

        private Empty() {
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public String column(int pos) {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public Ordering ordering(int i) {
            throw new IndexOutOfBoundsException();
        }

        @Override
        public void forEach(BiConsumer<String, Ordering> action) {
        }

        @Override
        public String spec() {
            return "";
        }

        @Override
        public void appendSpec(StringBuilder b) {
        }

        @Override
        public int hashCode() {
            return 1345160369;
        }

        @Override
        public boolean equals(Object obj) {
            return obj == this || obj instanceof RowKey rk && rk.size() == 0;
        }

        @Override
        public String toString() {
            return BasicRowKey.toString(this);
        }
    }
}
