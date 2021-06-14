/*
 *  Copyright (C) 2021 Cojen.org
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

import java.util.ArrayList;

import java.util.concurrent.atomic.AtomicLong;

import org.cojen.maker.AnnotationMaker;
import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;

import org.cojen.tupl.Nullable;
import org.cojen.tupl.PrimaryKey;

import org.cojen.tupl.core.TestUtils;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class RowTestUtils extends TestUtils {
    private static final AtomicLong packageNum = new AtomicLong();

    public static String newRowTypeName() {
        // Generate different packages to faciliate class unloading.
        return "test.p" + packageNum.getAndIncrement() + ".TestRow";
    }

    public static ClassMaker newRowTypeMaker() {
        return newRowTypeMaker(null);
    }

    /**
     * @param rowTypeName can be null to assign automatically
     */
    public static ClassMaker newRowTypeMaker(String rowTypeName) {
        if (rowTypeName == null) {
            rowTypeName = newRowTypeName();
        }
        return ClassMaker.begin(newRowTypeName()).public_().interface_();
    }

    /**
     * Define a new row type interface. Specification consists of alternating Class and name
     * pairs. A name suffix of '?' indicates that it's Nullable. A name prefix of '+' or '-'
     * indicates that it's part of the primary key.
     *
     * @param rowTypeName can be null to assign automatically
     */
    public static Class newRowType(String rowTypeName, Object... spec) {
        if ((spec.length & 1) != 0) {
            throw new IllegalArgumentException("Odd spec length");
        }

        ClassMaker cm = newRowTypeMaker(rowTypeName);

        var pkNames = new ArrayList<String>();

        for (int i=0; i<spec.length; i+=2) {
            var type = (Class) spec[i];
            var name = (String) spec[i + 1];

            boolean nullable = false;

            if (name.endsWith("?")) {
                nullable = true;
                name = name.substring(0, name.length() - 1);
            }

            if (name.startsWith("+") || name.startsWith("-")) {
                pkNames.add(name);
                name = name.substring(1);
            }

            MethodMaker mm = cm.addMethod(type, name).public_().abstract_();
            if (nullable) {
                mm.addAnnotation(Nullable.class, true);
            }

            cm.addMethod(void.class, name, type).public_().abstract_();
        }

        AnnotationMaker am = cm.addAnnotation(PrimaryKey.class, true);
        am.put("value", pkNames.toArray());

        return cm.finish();
    }
}
