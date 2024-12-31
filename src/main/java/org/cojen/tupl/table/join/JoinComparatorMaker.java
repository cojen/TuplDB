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

package org.cojen.tupl.table.join;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.util.Comparator;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;

import org.cojen.tupl.core.TupleKey;

import org.cojen.tupl.table.ComparatorMaker;
import org.cojen.tupl.table.OrderBy;
import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.RowUtils;
import org.cojen.tupl.table.WeakCache;

/**
 * @author Brian S O'Neill
 */
final class JoinComparatorMaker<J> {
    private static final WeakCache<TupleKey, Comparator<?>, Object> cCache;

    static {
        cCache = new WeakCache<>() {
            @Override
            public Comparator<?> newValue(TupleKey key, Object unused) {
                var joinType = (Class<?>) key.get(0);
                String spec = key.getString(1);
                var maker = new JoinComparatorMaker<>(joinType, spec);
                String canonical = maker.canonicalSpec();
                if (spec.equals(canonical)) {
                    return maker.finish();
                } else {
                    return obtain(TupleKey.make.with(joinType, canonical), null);
                }
            }
        };
    }

    /**
     * Returns a new or cached comparator instance.
     */
    @SuppressWarnings("unchecked")
    static <J> Comparator<J> comparator(Class<J> joinType, String spec) {
        return (Comparator<J>) cCache.obtain(TupleKey.make.with(joinType, spec), null);
    }

    private final Class<J> mJoinType;
    private final RowInfo mJoinInfo;
    private final OrderBy mOrderBy;

    private JoinComparatorMaker(Class<J> rowType, String spec) {
        mJoinType = rowType;
        mJoinInfo = RowInfo.find(rowType);
        mOrderBy = OrderBy.forSpec(mJoinInfo.allColumns, spec);
    }

    String canonicalSpec() {
        return mOrderBy.spec();
    }

    @SuppressWarnings("unchecked")
    Comparator<J> finish() {
        if (mOrderBy.isEmpty()) {
            return ComparatorMaker.zero();
        }

        ClassMaker cm = RowGen.beginClassMaker
            (JoinComparatorMaker.class, mJoinType, mJoinInfo.name, null, "comparator")
            .implement(Comparator.class).final_();

        // Keep a singleton instance, in order for a weakly cached reference to the comparator
        // to stick around until the class is unloaded.
        cm.addField(Comparator.class, "_").private_().static_();

        MethodMaker mm = cm.addConstructor().private_();
        mm.invokeSuperConstructor();
        mm.field("_").set(mm.this_());

        mm = cm.addMethod(int.class, "compare", mJoinType, mJoinType).public_();
        ComparatorMaker.makeCompare(mm, mOrderBy);

        // Now implement the bridge methods.

        mm = cm.addMethod(int.class, "compare", Object.class, Object.class).public_().bridge();
        mm.return_(mm.invoke("compare", mm.param(0).cast(mJoinType), mm.param(1).cast(mJoinType)));

        try {
            MethodHandles.Lookup lookup = cm.finishHidden();
            MethodHandle mh = lookup.findConstructor
                (lookup.lookupClass(), MethodType.methodType(void.class));
            return (Comparator<J>) mh.invoke();
        } catch (Throwable e) {
            throw RowUtils.rethrow(e);
        }
    }
}
