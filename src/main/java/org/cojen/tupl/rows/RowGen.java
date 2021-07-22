/*
 *  Copyright 2021 Cojen.org
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

package org.cojen.tupl.rows;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.cojen.maker.ClassMaker;

/**
 * Cache of objects used by code generation.
 *
 * @author Brian S O'Neill
 */
class RowGen {
    final RowInfo info;

    private volatile String[] mStateFields;
    private volatile Map<String, Integer> mColumnNumbers;
    private volatile ColumnCodec[] mKeyCodecs;
    private volatile ColumnCodec[] mValueCodecs;

    RowGen(RowInfo info) {
        this.info = info;
    }

    /**
     * @param suffix appended to class name
     */
    public ClassMaker beginClassMaker(Class<?> who, Class<?> rowType, String suffix) {
        return beginClassMaker(who, rowType, info, null, suffix);
    }

    /**
     * @param subPackage optional
     * @param suffix appended to class name
     */
    public ClassMaker beginClassMaker(Class<?> who, Class<?> rowType,
                                      String subPackage, String suffix)
    {
        return beginClassMaker(who, rowType, info, subPackage, suffix);
    }

    /**
     * @param subPackage optional
     * @param suffix appended to class name
     */
    public static ClassMaker beginClassMaker(Class<?> who, Class<?> rowType, RowInfo info,
                                             String subPackage, String suffix)
    {
        String name;
        {
            int ix = info.name.lastIndexOf('.');
            if (ix > 0) {
                if (subPackage == null) {
                    name = info.name + suffix;
                } else {
                    String packageName = info.name.substring(0, ix) + '.' + subPackage;
                    name = packageName + '.' + info.name.substring(ix + 1) + suffix;
                }
            } else if (subPackage == null) {
                name = info.name + suffix;
            } else {
                name = subPackage + '.' + info.name + suffix;
            }
        }

        ClassMaker cm = ClassMaker.begin(name, rowType.getClassLoader(), RowGen.class);

        if (who != null) {
            cm.sourceFile(who.getSimpleName());
        }

        var thisModule = RowGen.class.getModule();
        var thatModule = cm.classLoader().getUnnamedModule();

        // Generated code needs access to these non-exported packages.
        thisModule.addExports("org.cojen.tupl.core", thatModule);
        thisModule.addExports("org.cojen.tupl.filter", thatModule);
        thisModule.addExports("org.cojen.tupl.rows", thatModule);
        thisModule.addExports("org.cojen.tupl.views", thatModule);

        return cm;
    }

    /**
     * Returns the names of the generated state fields, each of which tracks up to 16 columns.
     */
    public String[] stateFields() {
        String[] fields = mStateFields;
        if (fields == null) {
            fields = makeStateFields();
        }
        return fields;
    }

    private String[] makeStateFields() {
        // One field per 16 columns (2 bits for each column).
        String[] fields = new String[(info.allColumns.size() + 15) / 16];

        for (int i=0; i<fields.length; i++) {
            fields[i] = unique("state$" + i);
        }

        mStateFields = fields;
        return fields;
    }

    /**
     * Returns a field name which doesn't clash with a column name. The given prefix must
     * already be unique among prefixes.
     */
    private String unique(String prefix) {
        String name = prefix;
        while (true) {
            if (!info.allColumns.containsKey(name)) {
                return name;
            }
            name += '$';
        }
    }

    /**
     * Returns a mapping of columns to zero-based column numbers. Keys are ordered
     * first. Within the two groups, column order matches encoding order.
     */
    public Map<String, Integer> columnNumbers() {
        Map<String, Integer> map = mColumnNumbers;
        if (map == null) {
            map = makeColumnNumbers();
        }
        return map;
    }

    private Map<String, Integer> makeColumnNumbers() {
        var map = new LinkedHashMap<String, Integer>(info.allColumns.size() << 1);

        // Keys first, to cluster their column states together.
        int num = 0;
        for (ColumnInfo info : info.keyColumns.values()) {
            map.put(info.name, num++);
        }
        for (ColumnCodec codec : valueCodecs()) { // use encoding order
            if (!(codec instanceof SchemaVersionColumnCodec)) {
                map.put(codec.mInfo.name, num++);
            }
        }

        mColumnNumbers = map;
        return map;
    }

    public String stateField(int columnNum) {
        return stateFields()[stateFieldNum(columnNum)];
    }

    public static int stateFieldNum(int columnNum) {
        return columnNum >> 4;
    }

    public static int stateFieldMask(int columnNum) {
        return stateFieldMask(columnNum, 0b11);
    }

    public static int stateFieldMask(int columnNum, int state) {
        return state << ((columnNum & 0b1111) << 1);
    }

    /**
     * Returns the value codecs in the order in which they should be encoded, which is the same
     * as RowInfo.keyColumns order (declaration order).
     */
    public ColumnCodec[] keyCodecs() {
        ColumnCodec[] codecs = mKeyCodecs;
        if (codecs == null) {
            mKeyCodecs = codecs = ColumnCodec.make(info.keyColumns, true);
        }
        return codecs;
    }

    /**
     * Returns the value codecs in the order in which they should be encoded, which can differ
     * from RowInfo.valueColumns order (lexicographical order).
     */
    public ColumnCodec[] valueCodecs() {
        ColumnCodec[] codecs = mValueCodecs;

        if (codecs == null) {
            // Encode fixed sized columns first (primitives), then nullable primitives, then
            // the rest in natural order (lexicographical).
            var infos = new ArrayList<ColumnInfo>(info.valueColumns.values());
            infos.sort((a, b) -> {
                int ap = a.plainTypeCode();
                int bp = b.plainTypeCode();

                if (ColumnInfo.isPrimitive(ap)) {
                    if (!ColumnInfo.isPrimitive(bp)) {
                        return -1;
                    }
                    if (!a.isNullable()) {
                        if (b.isNullable()) {
                            return -1;
                        }
                    } else if (!b.isNullable()) {
                        return 1;
                    }
                } else if (ColumnInfo.isPrimitive(bp)) {
                    return 1;
                }

                return 0;
            });

            mValueCodecs = codecs = ColumnCodec.make(infos, false);
        }

        return codecs;
    }
}
