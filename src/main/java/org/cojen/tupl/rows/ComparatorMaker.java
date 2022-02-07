/*
 *  Copyright (C) 2022 Cojen.org
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

package org.cojen.tupl.rows;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * @see Table#comparator
 * @author Brian S O'Neill
 */
class ComparatorMaker<R> {
    private final Class<R> mRowType;
    private final RowInfo mRowInfo;
    private final Map<String, Rule> mRules;

    /**
     * Constructor for primary key ordering.
     */
    ComparatorMaker(Class<R> rowType) {
        mRowType = rowType;
        mRowInfo = RowInfo.find(rowType);
        mRules = new LinkedHashMap<String, Rule>();
        for (ColumnInfo column : mRowInfo.keyColumns.values()) {
            mRules.put(column.name, new Rule(column, column.isDescending() ? 1 : 0));
        }
    }

    ComparatorMaker(Class<R> rowType, String spec) {
        mRowType = rowType;
        mRowInfo = RowInfo.find(rowType);
        mRules = parseRules(mRowInfo, spec);
    }

    String cleanRules() {
        var b = new StringBuilder();
        for (Rule rule : mRules.values()) {
            rule.appendTo(b);
        }
        return b.toString();
    }

    Comparator<R> finish() {
        Class rowClass = RowMaker.find(mRowType);
        ClassMaker cm = mRowInfo.rowGen().anotherClassMaker
            (ComparatorMaker.class, rowClass, "Comparator").implement(Comparator.class).final_();

        // Define a singleton cached instance.
        cm.addField(Comparator.class, "THE").private_().static_();

        MethodMaker mm = cm.addConstructor().private_();
        mm.invokeSuperConstructor();
        mm.field("THE").set(mm.this_());

        makeCompare(cm.addMethod(int.class, "compare", rowClass, rowClass).public_());

        // Now implement the bridge methods.

        mm = cm.addMethod(int.class, "compare", mRowType, mRowType).public_().bridge();
        mm.return_(mm.invoke("compare", mm.param(0).cast(rowClass), mm.param(1).cast(rowClass)));

        mm = cm.addMethod(int.class, "compare", Object.class, Object.class).public_().bridge();
        mm.return_(mm.invoke("compare", mm.param(0).cast(rowClass), mm.param(1).cast(rowClass)));

        try {
            MethodHandles.Lookup lookup = cm.finishHidden();
            MethodHandle mh = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class));
            return (Comparator<R>) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    void makeCompare(MethodMaker mm) {
        var row0 = mm.param(0);
        var row1 = mm.param(1);

        Iterator<Rule> it = mRules.values().iterator();
        while (it.hasNext()) {
            Rule rule = it.next();
            ColumnInfo column = rule.column;

            Variable field0 = row0.field(column.name);
            Variable field1 = row1.field(column.name);

            if (rule.isDescending()) {
                var temp = field0;
                field0 = field1;
                field1 = temp;
            }

            Label nextLabel = mm.label();

            Variable resultVar;

            if (column.isPrimitive()) {
                String compareName = column.isUnsignedInteger() ? "compareUnsigned" : "compare";
                resultVar = field0.invoke(compareName, field0, field1);
            } else {
                field0 = field0.get();
                field1 = field1.get();

                // Compare against nulls. Even though the column type might not support nulls,
                // be lenient. The row being compared might not be fully initialized.
                Label cont = mm.label();
                field0.ifNe(null, cont);
                field1.ifEq(null, nextLabel);
                mm.return_(rule.isNullLow() ? -1 : 1);
                cont.here();
                cont = mm.label();
                field1.ifNe(null, cont);
                mm.return_(rule.isNullLow() ? 1 : -1);
                cont.here();

                if (column.isArray()) {
                    String compareName = column.isUnsignedInteger() ? "compareUnsigned" : "compare";
                    resultVar = mm.var(Arrays.class).invoke(compareName, field0, field1);
                } else {
                    resultVar = field0.invoke("compareTo", field1);
                }
            }

            if (it.hasNext()) {
                resultVar.ifEq(0, nextLabel);
            }

            mm.return_(resultVar);

            nextLabel.here();
        }

        mm.return_(0);
    }

    private static Map<String, Rule> parseRules(RowInfo rowInfo, String spec) {
        try {
            Map<String, Rule> rules = doParseRules(rowInfo.allColumns, spec);
            if (!rules.isEmpty()) {
                return rules;
            }
        } catch (IndexOutOfBoundsException e) {
        }

        throw new IllegalArgumentException("Malformed ordering specification: " + spec);
    }

    private static Map<String, Rule> doParseRules(Map<String, ColumnInfo> columns, String spec) {
        Map<String, Rule> rules = new LinkedHashMap<>();

        int length = spec.length();
        for (int pos = 0; pos < length; ) {
            int flags = 0;

            int order = spec.charAt(pos++);
            if (order == '-') {
                flags |= 1;
            } else if (order != '+') {
                break;
            }

            if (spec.charAt(pos) == '!') {
                flags |= 2;
                pos++;
            }

            int end = pos;
            while (end < length) {
                order = spec.charAt(end);
                if (order == '-' || order == '+') {
                    break;
                }
                end++;
            }

            if (end == pos) {
                break;
            }

            String name = spec.substring(pos, end);
            ColumnInfo column = columns.get(name);
            if (column == null) {
                throw new IllegalStateException
                    ("Unknown column \"" + name + "\" in ordering specification: " + spec);
            }

            pos = end;

            if (column.isPrimitive()) {
                // Can't be null.
                flags &= ~2;
            }

            if (!rules.containsKey(name)) {
                rules.put(name, new Rule(column, flags));
            }
        }

        return rules;
    }

    // flags: bit 0: descending,  bit 1: null low
    private static record Rule(ColumnInfo column, int flags) {
        boolean isDescending() {
            return (flags & 1) != 0;
        }

        boolean isNullLow() {
            return (flags & 2) != 0;
        }

        void appendTo(StringBuilder b) {
            b.append(isDescending() ? '-' : '+');
            if (isNullLow()) {
                b.append('!');
            }
            b.append(column.name);
        }
    }
}
