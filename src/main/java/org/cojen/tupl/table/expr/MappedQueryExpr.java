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

package org.cojen.tupl.table.expr;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.cojen.tupl.Mapper;
import org.cojen.tupl.Table;
import org.cojen.tupl.Untransformed;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.table.MappedTable;
import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowUtils;

import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Defines a QueryExpr which relies on a Mapper to perform custom transformations and
 * filtering.
 *
 * @author Brian S. O'Neill
 * @see QueryExpr#make
 */
final class MappedQueryExpr extends QueryExpr {
    private final Expr mFilter;
    private final boolean mRequireRemap;

    /**
     * @param filter can be null if filter is TrueFilter
     * @see QueryExpr#make
     */
    MappedQueryExpr(int startPos, int endPos, TupleType type,
                    RelationExpr from, RowFilter rowFilter, Expr filter,
                    List<ProjExpr> projection, int maxArgument)
    {
        super(startPos, endPos, type, from, rowFilter, projection, maxArgument);
        mFilter = filter;
        mRequireRemap = requireRemap(filter);
    }

    private static boolean requireRemap(Expr filter) {
        return filter == null ? false
            : (filter.hasOrderDependentException() && filter.isPureFunction());
    }

    @Override
    public boolean isPureFunction() {
        return super.isPureFunction() && (mFilter == null || mFilter.isPureFunction());
    }

    @Override
    public QuerySpec querySpec(Table<?> table) {
        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected TableProvider doMakeTableProvider() {
        TableProvider source = mFrom.makeTableProvider();

        MapperFactory factory = CodeCache.obtain(makeKey(), this::makeMapper);

        Class targetClass = type().rowType().clazz();

        int argCount = maxArgument();

        if (argCount == 0) {
            return TableProvider.make
                (source.table().map(targetClass, factory.get(RowUtils.NO_ARGS)));
        }

        return new TableProvider.Wrapped(source, argCount) {
            @Override
            public Class rowType() {
                return targetClass;
            }

            @Override
            @SuppressWarnings("unchecked")
            public Table table(Object... args) {
                checkArgumentCount(args);
                return source.table(args).map(targetClass, factory.get(args));
            }
        };
    }

    public static interface MapperFactory {
        /**
         * Returns a new or singleton Mapper instance.
         */
        Mapper<?, ?> get(Object[] args);
    }

    private MapperFactory makeMapper() {
        Class<?> targetClass = type().rowType().clazz();

        ClassMaker cm = RowGen.beginClassMaker
            (MappedQueryExpr.class, targetClass, targetClass.getName(), null, "mapper")
            .implement(Mapper.class).implement(MapperFactory.class).final_();

        // Keep a reference to the factory instance, to prevent it from being garbage collected
        // as long as the generated class still exists.
        cm.addField(Object.class, "_").private_().static_();

        {
            MethodMaker ctor = cm.addConstructor().private_();
            ctor.invokeSuperConstructor();
            ctor.field("_").set(ctor.this_());
        }

        int argCount = maxArgument();

        // The Mapper is also its own MapperFactory.
        {
            MethodMaker mm = cm.addMethod(Mapper.class, "get", Object[].class).public_();

            if (argCount == 0) {
                // Just return a singleton.
                mm.return_(mm.this_());
            } else {
                cm.addField(Object[].class, "args").private_().final_();

                MethodMaker ctor = cm.addConstructor(Object[].class).private_();
                ctor.invokeSuperConstructor();
                ctor.field("args").set(ctor.param(0));

                mm.return_(mm.new_(cm, mm.param(0)));
            }
        }

        Set<Column> evalColumns = gatherEvalColumns();

        if (mRequireRemap) {
            addRemapMethod(cm, evalColumns, argCount);
        }

        addMapMethod(cm, evalColumns, argCount);

        addSourceProjectionMethod(cm, evalColumns);

        addInverseMappingFunctions(cm);

        addToStringMethod(cm);
        addPlanMethod(cm);

        // Override the check methods to do nothing. This behavior is correct for an update
        // statement, because it permits altering the row to appear outside the set of rows
        // selected by the filter. This behavior is incorrect for a view, which disallows
        // creating or altering rows such that they appear outside the view's bounds. A view
        // needs to check another filter before allowing the operation to proceed.
        addCheckMethods(cm);

        MethodHandles.Lookup lookup = cm.finishHidden();
        Class<?> clazz = lookup.lookupClass();

        try {
            MethodHandle mh = lookup.findConstructor(clazz, MethodType.methodType(void.class));
            return (MapperFactory) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }

    private Set<Column> gatherEvalColumns() {
        var evalColumns = new HashSet<Column>();

        if (mFilter != null) {
            mFilter.gatherEvalColumns(evalColumns);
        }

        for (Expr expr : mProjection) {
            expr.gatherEvalColumns(evalColumns);
        }

        return evalColumns;
    }

    private void addMapMethod(ClassMaker cm, Set<Column> evalColumns, int argCount) {
        TupleType targetType = type().rowType();

        MethodMaker mm = cm.addMethod
            (targetType.clazz(), "map", Object.class, Object.class).public_();

        Variable sourceRow;
        if (evalColumns.isEmpty()) {
            // No need to cast the sourceRow because it won't be used.
            sourceRow = mm.param(0);
        } else {
            sourceRow = mm.param(0).cast(mFrom.type().rowType().clazz());
        }

        var targetRow = mm.param(1).cast(targetType.clazz());

        var argsVar = argCount == 0 ? null : mm.field("args").get();
        var context = new EvalContext(argsVar, sourceRow);


        // Eagerly evaluate AssignExprs. The result might be needed by downstream expressions,
        // the filter, or it might throw an exception. In the unlikely case that none of these
        // conditions are met, then the AssignExprs can be evaluated after filtering.
        IdentityHashMap<AssignExpr, Variable> mProjectedVars = null;
        for (ProjExpr pe : mProjection) {
            if (pe.wrapped() instanceof AssignExpr ae) {
                Variable v = pe.makeEval(context);
                if (mProjectedVars == null) {
                    mProjectedVars = new IdentityHashMap<>();
                }
                mProjectedVars.put(ae, v);
            }
        }

        if (mRowFilter != TrueFilter.THE) {
            Label pass = mm.label();
            Label fail = mm.label();

            Label tryStart = null;
            if (mRequireRemap) {
                tryStart = mm.label().here();
            }

            mFilter.makeFilter(context, pass, fail);

            fail.here();
            mm.return_(null);

            if (mRequireRemap) {
                mm.catch_(tryStart, RuntimeException.class, exVar -> {
                    mm.return_(mm.invoke("remap", exVar, sourceRow, targetRow));
                });
            }

            pass.here();
        }

        int colNum = 0;
        for (ProjExpr pe : mProjection) {
            if (pe.hasExclude()) {
                continue;
            }
            Variable result;
            if (pe.wrapped() instanceof AssignExpr ae) {
                result = mProjectedVars.get(ae);
            } else {
                result = pe.makeEval(context);
            }
            targetRow.invoke(targetType.column(colNum++).fieldName(), result);
        }

        mm.return_(targetRow);

        // Now implement the bridge method.
        MethodMaker bridge = cm.addMethod
            (Object.class, "map", Object.class, Object.class).public_().bridge();
        bridge.return_(bridge.this_().invoke(targetType.clazz(), "map",
                                             null, bridge.param(0), bridge.param(1)));
    }

    /**
     * Defines a private variant of the map method which is only called when an exception is
     * thrown when the map method is processing the filter. The remap method doesn't generate
     * short circuit logic, and it can suppress an exception depending on the outcome of an
     * 'and' or 'or' operation. If an exception shouldn't be suppressed, the original is thrown.
     */
    private void addRemapMethod(ClassMaker cm, Set<Column> evalColumns, int argCount) {
        // Generated code needs access to the RemapUtils class in this package.
        {
            var thisModule = getClass().getModule();
            var thatModule = cm.classLoader().getUnnamedModule();
            thisModule.addExports("org.cojen.tupl.table.expr", thatModule);
        }

        TupleType targetType = type().rowType();

        Class sourceParamType = Object.class;

        if (!evalColumns.isEmpty()) {
            sourceParamType = mFrom.type().rowType().clazz();
        }

        MethodMaker mm = cm.addMethod
            (targetType.clazz(), "remap",
             RuntimeException.class, sourceParamType, targetType.clazz())
            .private_();

        var originalEx = mm.param(0);
        var sourceRow = mm.param(1);
        var targetRow = mm.param(2);

        var argsVar = argCount == 0 ? null : mm.field("args").get();
        var context = new EvalContext(argsVar, sourceRow);

        Label tryStart = mm.label().here();

        var resultVar = mFilter.makeFilterEvalRemap(context);

        mm.catch_(tryStart, Throwable.class, exVar -> {
            originalEx.throw_();
        });

        Label pass = mm.label();

        mm.var(RemapUtils.class).invoke("checkFinal", originalEx, resultVar).ifTrue(pass);
        mm.return_(null);

        pass.here();

        int numColumns = targetType.numColumns();

        for (int i=0; i<numColumns; i++) {
            Column column = targetType.column(i);
            targetRow.invoke(column.fieldName(), mProjection.get(i).makeEval(context));
        }

        mm.return_(targetRow);
    }

    private void addSourceProjectionMethod(ClassMaker cm, Set<Column> evalColumns) {
        int numColumns = evalColumns.size();

        // FIXME: might be a join; flatten to get the max
        int maxColumns = mFrom.type().rowType().numColumns();

        if (numColumns == maxColumns) {
            // The default implementation indicates that all source columns are projected.
            // FIXME: Depends on the actual paths.
            return;
        }

        if (numColumns > maxColumns) {
            // FIXME
            //throw new AssertionError();
        }

        MethodMaker mm = cm.addMethod(String.class, "sourceProjection").public_();

        if (numColumns == 0) {
            mm.return_("");
            return;
        }

        // The sourceProjection string isn't used as a cache key, so it can just be constructed
        // as needed rather than stashing a reference to a big string instance.

        Object[] toConcat = new String[numColumns + numColumns - 1];

        int i = 0;
        for (Column column : evalColumns) {
            if (i > 0) {
                toConcat[i++] = ", ";
            }
            toConcat[i++] = column.fieldName();
        }

        mm.return_(mm.concat(toConcat));
    }

    private void addInverseMappingFunctions(ClassMaker cm) {
        TupleType targetType = type().rowType();
        int numColumns = targetType.numColumns();

        for (int i=0; i<numColumns; i++) {
            if (!(mProjection.get(i).wrapped() instanceof ColumnExpr source)) {
                continue;
            }

            Class columnType = targetType.column(i).type().clazz();
            if (columnType != source.type().clazz()) {
                continue;
            }

            String sourceName = MappedTable.escape(source.column().fieldName());
            String methodName = targetType.column(i).fieldName() + "_to_" + sourceName;
            MethodMaker mm = cm.addMethod(columnType, methodName, columnType).public_().static_();
            mm.addAnnotation(Untransformed.class, true);
            mm.return_(mm.param(0));
        }
    }

    private void addToStringMethod(ClassMaker cm) {
        MethodMaker mm = cm.addMethod(String.class, "toString").public_();
        mm.return_(mm.class_().invoke("getName"));
    }

    private void addPlanMethod(ClassMaker cm) {
        if (mRowFilter == TrueFilter.THE) {
            return;
        }
        MethodMaker mm = cm.addMethod(QueryPlan.class, "plan", QueryPlan.Mapper.class).public_();
        mm.return_(mm.new_(QueryPlan.Filter.class, rowFilterString(), mm.param(0)));
    }

    protected void addCheckMethods(ClassMaker cm) {
        for (int i=0; i<3; i++) {
            String name = "check" + switch(i) {
                default -> "Store"; case 1 -> "Update"; case 2 -> "Delete";
            };
            cm.addMethod(null, name, Table.class, Object.class).public_().override();
            // The method is simply empty.
        }
    }
}
