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

package org.cojen.tupl.table;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

import java.util.function.Function;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Table;
import org.cojen.tupl.Untransformed;

import org.cojen.tupl.table.expr.Parser;

import org.cojen.tupl.table.filter.ColumnFilter;
import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.ComplexFilterException;
import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;

/**
 * Base class for mapped and grouped tables.
 *
 * @author Brian S. O'Neill
 */
public abstract class AbstractMappedTable<S, T> extends WrappedTable<S, T> {
    protected AbstractMappedTable(Table<S> source) {
        super(source);
    }

    protected abstract String sourceProjection();

    /**
     * Returns a class which contains static inverse functions.
     */
    protected abstract Class<?> inverseFunctions();

    protected abstract SortPlan analyzeSort(InverseFinder finder, QuerySpec targetQuery);

    /**
     * Splits a target query into source and target components.
     */
    protected class Splitter implements Function<ColumnFilter, RowFilter> {
        private record ArgMapper(int targetArgNum, Method function) { } 

        private final RowFilter mTargetFilter;
        private final InverseFinder mFinder;

        private Map<ArgMapper, Integer> mArgMappers;
        private int mMaxArg;

        public final RowFilter mTargetRemainder;
        public final QuerySpec mSourceQuery;
        public final SortPlan mSortPlan;

        Splitter(QuerySpec targetQuery) {
            RowInfo sourceInfo = RowInfo.find(mSource.rowType());
            var finder = new InverseFinder(sourceInfo.allColumns);

            RowFilter targetFilter = targetQuery.filter();

            try {
                targetFilter = targetFilter.cnf();
            } catch (ComplexFilterException e) {
                // The split won't be as effective, and so the remainder will do more work.
            }

            mTargetFilter = targetFilter;
            mFinder = finder;

            var split = new RowFilter[2];
            targetFilter.split(this, split);

            RowFilter sourceFilter = split[0];
            mTargetRemainder = split[1];

            QuerySpec sourceQuery;

            String sourceProjection = sourceProjection();
            if (sourceProjection == null) {
                sourceQuery = new QuerySpec(null, null, sourceFilter);
            } else {
                sourceQuery = Parser.parseQuerySpec
                    (mSource.rowType(), '{' + sourceProjection + '}').withFilter(sourceFilter);
            }

            mSortPlan = analyzeSort(finder, targetQuery);

            if (mSortPlan.sourceOrder != null) {
                sourceQuery = sourceQuery.withOrderBy(mSortPlan.sourceOrder);
            } else if (sourceQuery.projection() == null && sourceQuery.filter() == TrueFilter.THE) {
                // There's no orderBy, all columns are projected, there's no filter, so just do a
                // full scan.
                sourceQuery = null;
            }

            mSourceQuery = sourceQuery;
        }

        @Override
        public RowFilter apply(ColumnFilter cf) {
            ColumnFunction source = mFinder.tryFindSource(cf.column());
            if (source == null) {
                return null;
            }

            if (cf instanceof ColumnToArgFilter c2a) {
                c2a = c2a.withColumn(source.column());
                if (source.isUntransformed()) {
                    return c2a;
                }
                if (mArgMappers == null) {
                    mArgMappers = new HashMap<>();
                    mMaxArg = mTargetFilter.maxArgument();
                }
                var mArgMapper = new ArgMapper(c2a.argument(), source.function());
                Integer sourceArg = mArgMappers.get(mArgMapper);
                if (sourceArg == null) {
                    sourceArg = ++mMaxArg;
                    mArgMappers.put(mArgMapper, sourceArg);
                }
                return c2a.withArgument(sourceArg);
            } else if (cf instanceof ColumnToColumnFilter c2c) {
                // Can only convert to a source filter when both columns are untransformed
                // and a common type exists. It's unlikely that a common type doesn't exist.
                if (!source.isUntransformed()) {
                    return null;
                }
                ColumnFunction otherSource = mFinder.tryFindSource(c2c.otherColumn());
                if (otherSource == null || !otherSource.isUntransformed()) {
                    return null;
                }
                return c2c.tryWithColumns(source.column(), otherSource.column());
            }

            return null;
        }

        void addPrepareArgsMethod(ClassMaker cm) {
            if (mArgMappers == null) {
                // No special method is needed.
                return;
            }

            MethodMaker mm = cm.addMethod(Object[].class, "prepareArgs", Object[].class)
                .varargs().static_().final_();

            // Generate the necessary source query arguments from the provided target
            // arguments.

            Label ready = mm.label();
            var argsVar = mm.param(0);
            argsVar.ifEq(null, ready);

            argsVar.set(mm.var(Arrays.class).invoke("copyOf", argsVar, mMaxArg));

            for (Map.Entry<ArgMapper, Integer> e : mArgMappers.entrySet()) {
                ArgMapper mArgMapper = e.getKey();
                var argVar = argsVar.aget(mArgMapper.targetArgNum - 1);
                Label cont = mm.label();
                argVar.ifEq(null, cont);
                Method fun = mArgMapper.function();
                var targetArgVar = ConvertCallSite.make(mm, fun.getParameterTypes()[0], argVar);
                var sourceArgVar =
                    mm.var(fun.getDeclaringClass()).invoke(fun.getName(), targetArgVar);
                argsVar.aset(e.getValue() - 1, sourceArgVar);

                cont.here();
            }

            ready.here();
            mm.return_(argsVar);
        }

        Variable prepareArgs(Variable argsVar) {
            if (mArgMappers == null) {
                // No special method was defined.
                return argsVar;
            } else {
                return argsVar.methodMaker().invoke("prepareArgs", argsVar);
            }
        }
    }

    protected record ColumnFunction(ColumnInfo column, Method function) {
        boolean isUntransformed() {
            return function.isAnnotationPresent(Untransformed.class);
        }
    }

    protected static class SortPlan {
        // Optional ordering to apply to the source scanner.
        OrderBy sourceOrder;

        // Defines a partial order, and is required when sourceOrder and sortOrder is defined.
        //Comparator sourceComparator;

        // Optional sort order to apply to the target scanner.
        OrderBy sortOrder;

        // Is required when sortOrder is defined.
        String sortOrderSpec;

        // Is required when sortOrder is defined.
        Comparator sortComparator;
    }

    /**
     * Finds inverse mapping functions defined in a Mapper or GrouperFactory implementation.
     */
    protected class InverseFinder {
        final Map<String, ColumnInfo> mSourceColumns;
        final TreeMap<String, Method> mAllMethods;

        InverseFinder(Map<String, ColumnInfo> sourceColumns) {
            mSourceColumns = sourceColumns;

            mAllMethods = new TreeMap<>();
            for (Method m : inverseFunctions().getMethods()) {
                mAllMethods.put(m.getName(), m);
            }
        }

        ColumnFunction tryFindSource(ColumnInfo targetColumn) {
            String prefix = targetColumn.name + "_to_";

            for (Method candidate : mAllMethods.tailMap(prefix).values()) {
                String name = candidate.getName();
                if (!name.startsWith(prefix)) {
                    break;
                }

                if (!Modifier.isStatic(candidate.getModifiers())) {
                    continue;
                }

                Class<?> retType = candidate.getReturnType();
                if (retType == null || retType == void.class) {
                    continue;
                }

                Class<?>[] paramTypes = candidate.getParameterTypes();
                if (paramTypes.length != 1) {
                    continue;
                }

                if (!paramTypes[0].isAssignableFrom(targetColumn.type)) {
                    continue;
                }

                String sourceName = RowMethodsMaker.unescape(name.substring(prefix.length()));

                ColumnInfo sourceColumn = ColumnSet.findColumn(mSourceColumns, sourceName);
                if (sourceColumn == null) {
                    continue;
                }

                if (!sourceColumn.type.isAssignableFrom(retType)) {
                    continue;
                }

                return new ColumnFunction(sourceColumn, candidate);
            }

            return null;
        }
    }
}
