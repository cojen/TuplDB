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

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.Objects;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;

import org.cojen.tupl.ConversionException;
import org.cojen.tupl.Mapper;
import org.cojen.tupl.Table;
import org.cojen.tupl.Untransformed;

import org.cojen.tupl.core.TupleKey;

/**
 * Makes Mappers which perform column conversions. Source to target mapping is lossy, but the
 * inverse mapping is exact and can throw a ConversionException.
 *
 * @author Brian S. O'Neill
 * @see Table#map(Class)
 */
public class MapperMaker {
    private static final WeakCache<TupleKey, Mapper, Object> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            @SuppressWarnings("unchecked")
            public Mapper newValue(TupleKey key, Object unused) {
                return doMake((Class) key.get(0), (Class) key.get(1));
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <S, T> Mapper<S, T> make(Class<S> sourceType, Class<T> targetType) {
        return cCache.obtain(TupleKey.make.with(sourceType, targetType), null);
    }

    @SuppressWarnings("unchecked")
    private static <S, T> Mapper<S, T> doMake(Class<S> sourceType, Class<T> targetType) {
        RowInfo sourceInfo = RowInfo.find(sourceType);
        RowInfo targetInfo = RowInfo.find(targetType);

        ClassMaker cm = ClassMaker.begin(Mapper.class.getName()).implement(Mapper.class).public_();
        cm.sourceFile(MapperMaker.class.getSimpleName());

        // If the source or target types are generated, then they might not be findable using
        // the hierarchical class loading technique.
        cm.installClass(sourceType);
        cm.installClass(targetType);

        // Keep a singleton instance, in order for the weakly cached reference to the Mapper to
        // stick around until the class is unloaded.
        cm.addField(Object.class, "_").private_().static_();

        cm.addConstructor().private_();

        cm.addMethod(boolean.class, "performsFiltering").public_().override().return_(false);

        addCheckMethod(cm, "checkStore");
        addCheckMethod(cm, "checkUpdate");
        addCheckMethod(cm, "checkDelete");

        MethodMaker mapMaker = cm.addMethod(Object.class, "map", Object.class, Object.class);
        mapMaker.public_().override();

        var sourceRow = mapMaker.param(0).cast(sourceType);
        var targetRow = mapMaker.param(1).cast(targetType);

        int matchedSources = 0;

        // Map source columns to matching target columns, and define inverse mapping functions.
        for (ColumnInfo sourceCol : sourceInfo.allColumns.values()) {
            String name = sourceCol.name;
            ColumnInfo targetCol = targetInfo.allColumns.get(name);
            if (targetCol == null) {
                continue;
            }

            matchedSources++;

            var sourceVar = sourceRow.invoke(name);
            var targetVar = mapMaker.var(targetCol.type);
            Converter.convertLossy(mapMaker, sourceCol, sourceVar, targetCol, targetVar);
            targetRow.invoke(name, targetVar);

            MethodMaker invMaker = cm.addMethod
                (sourceVar.classType(), name + "_to_" + name, targetVar.classType())
                .public_().static_();

            if (targetVar.classType() == sourceVar.classType()) {
                invMaker.addAnnotation(Untransformed.class, true);
                invMaker.return_(invMaker.param(0));
            } else {
                var retVar = invMaker.var(sourceVar);
                Converter.convertExact
                    (invMaker, name, targetCol, invMaker.param(0), sourceCol, retVar);
                invMaker.return_(retVar);
            }
        }

        // Check that unmatched target columns have default values.
        for (ColumnInfo targetCol : targetInfo.allColumns.values()) {
            String name = targetCol.name;
            ColumnInfo sourceCol = sourceInfo.allColumns.get(name);
            if (sourceCol != null) {
                continue;
            }

            var targetVar = mapMaker.var(targetCol.type);
            Converter.setDefault(mapMaker, targetCol, targetVar);
            targetRow.invoke(name, targetVar);

            // Define a special inverse mapping function which just checks the validity of the
            // target column value. The method name doesn't have a source column.
            MethodMaker invMaker = cm.addMethod
                (null, name + "_to_", targetVar.classType()).public_().static_();

            Converter.isDefault(invMaker.param(0), targetCol.isNullable()).ifEq(false, () -> {
                invMaker.new_(ConversionException.class, "column=" + name).throw_();
            });
        }

        mapMaker.return_(targetRow);

        // Define the source projection, which when not overridden is "all".
        if (matchedSources < sourceInfo.allColumns.size()) {
            var proj = new StringBuilder(matchedSources * 10);

            for (ColumnInfo sourceCol : sourceInfo.allColumns.values()) {
                String name = sourceCol.name;
                if (targetInfo.allColumns.containsKey(name)) {
                    if (!proj.isEmpty()) {
                        proj.append(',');
                    }
                    proj.append(name);
                }
            }

            cm.addMethod(String.class, "sourceProjection").public_().override()
                .return_(proj.toString());
        }

        MethodHandles.Lookup lookup = cm.finishHidden();
        Class<?> clazz = lookup.lookupClass();

        try {
            var mapper = (Mapper<S, T>) lookup.findConstructor
                (clazz, MethodType.methodType(void.class)).invoke();

            // Assign the singleton reference.
            lookup.findStaticVarHandle(clazz, "_", Object.class).set(mapper);

            return mapper;
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private static void addCheckMethod(ClassMaker cm, String name) {
        cm.addMethod(null, name, Table.class, Object.class).public_().override();
    }
}
