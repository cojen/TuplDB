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

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;

import org.cojen.tupl.Mapper;
import org.cojen.tupl.Table;

import org.cojen.tupl.io.Utils;

import org.cojen.tupl.rows.RowGen;

/**
 * Defines a SelectNode which relies on a Mapper to perform custom transformations and
 * filtering. A call to {@link SelectNode#make} determines if a SelectMappedNode is required.
 *
 * @author Brian S. O'Neill
 */
public final class SelectMappedNode extends SelectNode {
    /**
     * @see SelectNode#make
     */
    SelectMappedNode(TupleType type, String name,
                     RelationNode from, Node where, Node[] projection)
    {
        super(type, name, from, where, projection);
    }

    /* FIXME

       - Identify all the required source columns by examining all of the ColumnNodes in
         the "where" and "projection". This forms the Mapper.sourceProjection.
 
       - Identify all the directly projected ColumnNodes to form the Mapper's
         <target_name>_to_<source_name> functions. Note that the target name and source name
         are the real column names, which might be generated. The implementations of these
         methods simply return the parameter as-is.

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
    */

    @Override
    protected Query<?> doMakeQuery() {
        Query fromQuery = mFrom.makeQuery();

        int argCount = highestParamOrdinal();
        MapperFactory factory = makeMapper(argCount);

        Class targetClass = type().tupleType().clazz();

        return new Query.Wrapped(fromQuery, argCount) {
            @Override
            @SuppressWarnings("unchecked")
            public Table asTable(Object... args) {
                checkArgumentCount(args);
                return mFromQuery.asTable(args).map(targetClass, factory.get(args));
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
            .implement(Mapper.class).implement(MapperFactory.class);

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

        addMapMethod(cm, argCount);

        // FIXME: Implement sourceProjection method.

        // FIXME: Implement inverse mapping functions.

        // FIXME: Implement the plan method, only if mWhere isn't null.

        // FIXME: Implement the check methods.

        // FIXME: Implement the toString method(?)

        MethodHandles.Lookup lookup = cm.finishHidden();
        Class<?> clazz = lookup.lookupClass();

        try {
            MethodHandle mh = lookup.findConstructor(clazz, MethodType.methodType(void.class));
            return (MapperFactory) mh.invoke();
        } catch (Throwable e) {
            throw Utils.rethrow(e);
        }
    }

    private void addMapMethod(ClassMaker cm, int argCount) {
        TupleType targetType = type().tupleType();

        MethodMaker mm = cm.addMethod
            (targetType.clazz(), "map", Object.class, Object.class).public_();

        // FIXME: If source projection is empty, no need to cast the sourceRow. It won't be used.
        Class<?> sourceClass = mFrom.type().tupleType().clazz();
        var sourceRow = mm.param(0).cast(sourceClass);

        var targetRow = mm.param(1).cast(targetType.clazz());

        var argsVar = argCount == 0 ? null : mm.field("args").get();
        var context = new MakerContext(argsVar, sourceRow);

        if (mWhere != null) {
            Label pass = mm.label();
            Label fail = mm.label();

            mWhere.makeFilter(context, pass, fail);

            fail.here();
            mm.return_(null);
            pass.here();
        }

        int numColumns = targetType.numColumns();

        for (int i=0; i<numColumns; i++) {
            targetRow.invoke(targetType.field(i), mProjection[i].makeEval(context));
        }

        mm.return_(targetRow);

        // Now implement the bridge method.
        mm = cm.addMethod(Object.class, "map", Object.class, Object.class).public_().bridge();
        mm.return_(mm.this_().invoke(targetType.clazz(), "map", null, mm.param(0), mm.param(1)));
    }
}
