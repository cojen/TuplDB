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

package org.cojen.tupl.model;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Mapper;
import org.cojen.tupl.Table;
import org.cojen.tupl.ViewConstraintException;

import org.cojen.tupl.diag.QueryPlan;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.rows.RowGen;
import org.cojen.tupl.rows.MappedTable;

import org.cojen.tupl.rows.filter.RowFilter;
import org.cojen.tupl.rows.filter.TrueFilter;

/**
 * Defines a SelectNode which relies on a Mapper to perform custom transformations and
 * filtering.
 *
 * @author Brian S. O'Neill
 * @see SelectNode#make
 */
final class SelectMappedNode extends SelectNode {
    private final Node mWhere;
    private final boolean mRequireRemap;

    /**
     * @param where can be null if filter is TrueFilter
     * @see SelectNode#make
     */
    SelectMappedNode(TupleType type, String name,
                     RelationNode from, RowFilter filter, Node where,
                     Node[] projection, int maxArgument)
    {
        super(type, name, from, filter, projection, maxArgument);
        mWhere = where;
        mRequireRemap = where == null ? false : where.hasOrderDependentException();
    }

    /* FIXME: Notes

       - The Mapper evaluates the "where" filter and returns null if it yields false. Any
         ColumnNodes which were projected (and have inverses) cannot be skipped because
         MappedTable doesn't always apply an appropriate filter against the source. The
         QueryPlan node should show the additional filtering which is applied.

       - The above comment might be wrong with respect to column skipping. Need to check the
         implementation of the MappedTable with respect to load, etc. If columns can be removed
         from the filter, it needs to obey the rules of RowFilter.split. It might be related to
         full inverse mapping of the primary key. If the primary key is fully (inverse) mapped,
         then CRUD operations work. It's the store operation that must always check the full
         filter.

       - If the primary key is fully mapped, CRUD operations work, and so the Mapper must apply
         the full filter. Only when CRUD operations don't work can the Mapper use a split
         remainder filter.

       - However, the MappedUpdater will call the checkUpdate and checkDelete methods. So
         perhaps always define those methods? Just checkStore can be skipped.
    */

    @Override
    public boolean isPureFunction() {
        return super.isPureFunction() && (mWhere == null || mWhere.isPureFunction());
    }

    @Override
    protected QueryFactory<?> doMakeQueryFactory() {
        QueryFactory source = mFrom.makeQueryFactory();

        int argCount = maxArgument();
        MapperFactory factory = makeMapper(argCount);

        Class targetClass = type().tupleType().clazz();

        return new QueryFactory.Wrapped(source, argCount) {
            @Override
            @SuppressWarnings("unchecked")
            public Table table(Object... args) {
                checkArgumentCount(args);
                return mSource.table(args).map(targetClass, factory.get(args));
            }
        };
    }

    public static interface MapperFactory {
        /**
         * Returns a new or singleton Mapper instance.
         */
        Mapper<?, ?> get(Object[] args);
    }

    // FIXME: I might want to cache these things. Use a string key?

    private MapperFactory makeMapper(int argCount) {
        Class<?> targetClass = type().tupleType().clazz();

        ClassMaker cm = RowGen.beginClassMaker
            (SelectMappedNode.class, targetClass, targetClass.getName(), null, "mapper")
            .implement(Mapper.class).implement(MapperFactory.class).final_();

        cm.addConstructor().private_();

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

        Set<String> evalColumns = evalColumns();

        if (mRequireRemap) {
            addRemapMethod(cm, evalColumns, argCount);
        }

        addMapMethod(cm, evalColumns, argCount);

        addSourceProjectionMethod(cm, evalColumns);

        addInverseMappingFunctions(cm);

        addToStringMethod(cm);
        addPlanMethod(cm);

        // FIXME: These only need to be added if CRUD operations work.
        if (false) {
            addCheckStoreMethod(cm);
            addCheckUpdateMethod(cm);
            addCheckDeleteMethod(cm);
        }

        MethodHandles.Lookup lookup = cm.finishHidden();
        Class<?> clazz = lookup.lookupClass();

        try {
            MethodHandle mh = lookup.findConstructor(clazz, MethodType.methodType(void.class));
            return (MapperFactory) mh.invoke();
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private Set<String> evalColumns() {
        var evalColumns = new HashSet<String>();

        if (mWhere != null) {
            mWhere.evalColumns(evalColumns);
        }

        for (Node node : mProjection) {
            node.evalColumns(evalColumns);
        }

        return evalColumns;
    }

    private void addMapMethod(ClassMaker cm, Set<String> evalColumns, int argCount) {
        TupleType targetType = type().tupleType();

        MethodMaker mm = cm.addMethod
            (targetType.clazz(), "map", Object.class, Object.class).public_();

        Variable sourceRow;
        if (evalColumns.isEmpty()) {
            // No need to cast the sourceRow because it won't be used.
            sourceRow = mm.param(0);
        } else {
            sourceRow = mm.param(0).cast(mFrom.type().tupleType().clazz());
        }

        var targetRow = mm.param(1).cast(targetType.clazz());

        var argsVar = argCount == 0 ? null : mm.field("args").get();
        var context = new EvalContext(argsVar, sourceRow);

        if (mFilter != TrueFilter.THE) {
            Label pass = mm.label();
            Label fail = mm.label();

            Label tryStart = null;
            if (mRequireRemap) {
                tryStart = mm.label().here();
            }

            mWhere.makeFilter(context, pass, fail);

            fail.here();
            mm.return_(null);

            if (mRequireRemap) {
                mm.catch_(tryStart, RuntimeException.class, exVar -> {
                    mm.return_(mm.invoke("remap", exVar, sourceRow, targetRow));
                });
            }

            pass.here();
        }

        int numColumns = targetType.numColumns();

        for (int i=0; i<numColumns; i++) {
            targetRow.invoke(targetType.field(i), mProjection[i].makeEval(context));
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
    private void addRemapMethod(ClassMaker cm, Set<String> evalColumns, int argCount) {
        // Generated code needs access to the RemapUtils class in this package.
        {
            var thisModule = getClass().getModule();
            var thatModule = cm.classLoader().getUnnamedModule();
            thisModule.addExports("org.cojen.tupl.model", thatModule);
        }

        TupleType targetType = type().tupleType();

        Class sourceParamType = Object.class;

        if (!evalColumns.isEmpty()) {
            sourceParamType = mFrom.type().tupleType().clazz();
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

        var resultVar = mWhere.makeFilterEvalRemap(context);

        mm.catch_(tryStart, Throwable.class, exVar -> {
            originalEx.throw_();
        });

        Label pass = mm.label();

        mm.var(RemapUtils.class).invoke("checkFinal", originalEx, resultVar).ifTrue(pass);
        mm.return_(null);

        pass.here();

        int numColumns = targetType.numColumns();

        for (int i=0; i<numColumns; i++) {
            targetRow.invoke(targetType.field(i), mProjection[i].makeEval(context));
        }

        mm.return_(targetRow);
    }

    private void addSourceProjectionMethod(ClassMaker cm, Set<String> evalColumns) {
        int numColumns = evalColumns.size();

        // FIXME: might be a join; flatten to get the max
        int maxColumns = mFrom.type().tupleType().numColumns();

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
        for (String column : evalColumns) {
            if (i > 0) {
                toConcat[i++] = ", ";
            }
            toConcat[i++] = column;
        }

        mm.return_(mm.concat(toConcat));
    }

    private void addInverseMappingFunctions(ClassMaker cm) {
        TupleType targetType = type().tupleType();
        int numColumns = targetType.numColumns();

        for (int i=0; i<numColumns; i++) {
            if (!(mProjection[i] instanceof ColumnNode source)) {
                continue;
            }

            Class columnType = targetType.column(i).type().clazz();
            if (columnType != source.type().clazz()) {
                continue;
            }

            String sourceName = MappedTable.escape(source.column().name());
            String methodName = targetType.field(i) + "_to_" + sourceName;
            MethodMaker mm = cm.addMethod(columnType, methodName, columnType).public_().static_();
            mm.return_(mm.param(0));
        }
    }

    private void addToStringMethod(ClassMaker cm) {
        MethodMaker mm = cm.addMethod(String.class, "toString").public_();
        mm.return_(mm.class_().invoke("getName"));
    }

    private void addPlanMethod(ClassMaker cm) {
        if (mFilter == TrueFilter.THE) {
            return;
        }
        String filterExpr = mFilter.toString();
        MethodMaker mm = cm.addMethod(QueryPlan.class, "plan", QueryPlan.Mapper.class).public_();
        mm.return_(mm.new_(QueryPlan.Filter.class, filterExpr, mm.param(0)));
    }

    // FIXME: Finish the check methods, which just applies the filter. Columns that don't need
    // to be checked effectively evaluate to true. For checkUpdate, any columns not in the
    // primary key must also call the isSet method. If it returns false, then the column check
    // passes. If true, must check the column value.

    private void addCheckStoreMethod(ClassMaker cm) {
        MethodMaker mm = cm.addMethod(null, "checkStore", Table.class, Object.class).public_();

        if (mFilter == TrueFilter.THE) {
            // Nothing to check.
            return;
        }

        // FIXME: checkStore
        mm.new_(ViewConstraintException.class).throw_();
    }

    private void addCheckUpdateMethod(ClassMaker cm) {
        MethodMaker mm = cm.addMethod(null, "checkUpdate", Table.class, Object.class).public_();

        if (mFilter == TrueFilter.THE) {
            // Nothing to check.
            return;
        }

        // FIXME: checkUpdate
        mm.new_(ViewConstraintException.class).throw_();
    }

    private void addCheckDeleteMethod(ClassMaker cm) {
        MethodMaker mm = cm.addMethod(null, "checkDelete", Table.class, Object.class).public_();

        if (mFilter == TrueFilter.THE) {
            // Nothing to check.
            return;
        }

        // FIXME: checkDelete
        mm.new_(ViewConstraintException.class).throw_();
    }
}
