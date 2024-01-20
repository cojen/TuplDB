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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import java.util.function.IntUnaryOperator;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Field;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

import org.cojen.tupl.Query;
import org.cojen.tupl.Scanner;
import org.cojen.tupl.Transaction;

import org.cojen.tupl.table.ColumnInfo;
import org.cojen.tupl.table.EmptyScanner;
import org.cojen.tupl.table.RowGen;
import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.RowMaker;
import org.cojen.tupl.table.WeakCache;

import org.cojen.tupl.table.filter.ColumnToArgFilter;
import org.cojen.tupl.table.filter.ColumnToColumnFilter;
import org.cojen.tupl.table.filter.FalseFilter;
import org.cojen.tupl.table.filter.QuerySpec;
import org.cojen.tupl.table.filter.RowFilter;
import org.cojen.tupl.table.filter.TrueFilter;
import org.cojen.tupl.table.filter.Visitor;

/**
 * Implements a nested loops join.
 *
 * @author Brian S O'Neill
 */
final class JoinScannerMaker {
    private static final WeakCache<Key, Class<?>, JoinSpec> cBaseCache;

    static {
        cBaseCache = new WeakCache<>() {
            @Override
            protected Class<?> newValue(Key key, JoinSpec spec) {
                return new JoinScannerMaker(key.mJoinType, spec, null).finishBase();
            }
        };
    }

    private static final class Key {
        final Class<?> mJoinType;
        final String[] mSourceNames;

        Key(Class<?> joinType, JoinSpec spec) {
            mJoinType = joinType;
            mSourceNames = spec.copySourceNames();
            Arrays.sort(mSourceNames);
        }

        @Override
        public int hashCode() {
            return mJoinType.hashCode() + Arrays.hashCode(mSourceNames);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof Key other
                && mJoinType == other.mJoinType && Arrays.equals(mSourceNames, other.mSourceNames);
        }
    }

    private final Class<?> mJoinType;
    private final RowInfo mJoinInfo;
    private final JoinSpec mSpec;
    private final QuerySpec mQuery;

    private final Class<?> mJoinClass;

    private final Class<?> mParentClass;
    private final ClassMaker mClassMaker;

    private ClassMaker mLauncherMaker;
    private Map<String, Map<QuerySpec, MethodMaker>> mQueryMethods;

    private MethodMaker mCtorMaker;

    private JoinSpec.Source mLastSource;

    // The remaining fields are updated by the addLoopMethod and are stateful.

    private Variable mJoinRowVar;

    // Is set to null when the loop end is reached.
    private Variable mObjectResultVar;
    // Lazily defined, and is set to false when the loop end is reached.
    private Variable mBooleanResultVar;
    // Is mObjectResultVar or mBooleanResultVar.
    private Variable mChosenResultVar;

    private int mParentType;
    private JoinSpec.Source mLastExists; // is set when exists was called for the last source

    private int mReadyNum;

    private Map<RowFilter, String> mPredicateMethods;

    // Map of columns which should be projected. If null, then all columns are projected.
    private Map<String, ColumnInfo> mProjectionMap;

    /**
     * @param joinType interface which defines a join row
     */
    JoinScannerMaker(Class<?> joinType, JoinSpec spec, QuerySpec query) {
        mJoinType = joinType;
        mJoinInfo = RowInfo.find(joinType);
        mSpec = spec;
        mQuery = query;

        mJoinClass = JoinRowMaker.find(mJoinType);

        if (query == null) {
            // Generate a new sub-package to facilitate unloading.
            String subPackage = RowGen.newSubPackage();
            mClassMaker = RowGen.beginClassMaker
                (getClass(), joinType, mJoinInfo.name, subPackage, "scanner")
                .extend(JoinScanner.class).public_();
            mParentClass = null;
        } else {
            mParentClass = cBaseCache.obtain(new Key(joinType, spec), spec);
            mClassMaker = anotherClassMaker(getClass(), "scanner").extend(mParentClass);
        }
    }

    ClassMaker classMaker() {
        return mClassMaker;
    }

    /**
     * Begin defining another class in the same sub-package.
     *
     * @param who the class which is making a class (can be null)
     * @param suffix appended to class name (can be null)
     */
    ClassMaker anotherClassMaker(Class<?> who, String suffix) {
        return RowGen.anotherClassMaker(who, mJoinInfo.name, mParentClass, suffix);
    }

    /**
     * Returns a class implementing JoinScanner, which is constructed with these parameters:
     *
     *   (Transaction txn, J joinRow, JoinQueryLauncher launcher, Object... args)
     *
     * Calling this method has the side-effect of defining query returning methods inside the
     * JoinQueryLauncher being made. The JoinQueryLauncherMaker is required to implement these
     * methods. The first QuerySpec in the sub maps is the full one, which is used when no
     * arguments are null.
     *
     * @param launcherMaker class being made by JoinQueryLauncherMaker
     * @param queryMethods maps source names to required query methods
     * @see JoinScanner
     */
    Class<?> finish(ClassMaker launcherMaker,
                    Map<String, Map<QuerySpec, MethodMaker>> queryMethods)
    {
        mLauncherMaker = launcherMaker;
        mQueryMethods = queryMethods;

        buildProjectionMap();

        mClassMaker.addField(launcherMaker, "launcher").private_().final_();
        mClassMaker.addField(Object[].class, "args").private_().final_();

        mCtorMaker = mClassMaker.addConstructor
            (Transaction.class, mJoinType, launcherMaker, Object[].class).varargs();

        mCtorMaker.field("launcher").set(mCtorMaker.param(2));
        mCtorMaker.field("args").set(mCtorMaker.param(3));

        mSpec.root().accept(new JoinSpec.Visitor() {
            @Override
            public JoinSpec.Node visit(JoinSpec.Column node) {
                mLastSource = node;
                return node;
            }

            @Override
            public JoinSpec.Node visit(JoinSpec.FullJoin node) {
                mLastSource = node;
                return node;
            }
        });

        addLoopMethod();

        // Call this after defining the loop method because it might have added more code into
        // constructor which must run before the super class constructor.
        mCtorMaker.invokeSuperConstructor(mCtorMaker.param(0), mCtorMaker.param(1));

        return mClassMaker.finish();
    }

    /**
     * Returns a common base class which implements everything except the loop method.
     */
    private Class<?> finishBase() {
        mClassMaker.abstract_();
        mClassMaker.addMethod(mJoinType, "loop", mJoinType, boolean.class).protected_().abstract_();

        mClassMaker.addField(Transaction.class, "txn").protected_().final_();
        mClassMaker.addField(mJoinType, "row").protected_();

        MethodMaker ctor = mClassMaker.addConstructor(Transaction.class, mJoinType);
        ctor.invokeSuperConstructor();
        ctor.field("txn").set(ctor.param(0));

        mSpec.root().accept(new JoinSpec.Visitor() {
            @Override
            public JoinSpec.Node visit(JoinSpec.Column node) {
                doVisit(node);
                return node;
            }

            @Override
            public JoinSpec.Node visit(JoinSpec.FullJoin node) {
                doVisit(node);
                return node;
            }

            private void doVisit(JoinSpec.Source source) {
                String fieldName = source.name() + "_s";
                mClassMaker.addField(Scanner.class, fieldName).protected_();
                // The scanner instances must never be null.
                ctor.field(fieldName).set(ctor.var(EmptyScanner.class).field("THE"));
            }
        });

        addRowMethod();
        addStepMethod();

        addCloseMethod();

        Label tryStart = ctor.label().here();
        ctor.invoke("loop", ctor.param(1), false);
        ctor.return_();
        Label tryEnd = ctor.label().here();

        var exVar = ctor.catch_(tryStart, tryEnd, Throwable.class);
        ctor.invoke("close", ctor.param(1), exVar).throw_();

        return mClassMaker.finish();
    }

    private void buildProjectionMap() {
        if (mQuery.projection() == null) {
            // Everything is projected.
            mProjectionMap = null;
            return;
        }

        mProjectionMap = new LinkedHashMap<>();

        for (ColumnInfo column : mQuery.projection().values()) {
            mProjectionMap.put(column.name, column);
        }

        // Additional columns might need to be projected to process the join.

        mSpec.root().accept(new JoinSpec.Visitor() {
            @Override
            public JoinSpec.Node visit(JoinSpec.Column node) {
                projectArgs(node);
                projectFilter(node.remainder());
                return node;
            }

            @Override
            public JoinSpec.Node visit(JoinSpec.JoinOp node) {
                node.leftChild().accept(this);
                node.rightChild().accept(this);
                if (JoinSpec.isOuterJoin(node.type())) {
                    projectFilter(node.predicate());
                }
                return node;
            }

            @Override
            public JoinSpec.Node visit(JoinSpec.FullJoin node) {
                projectArgs(node);
                projectFilter(node.predicate());
                return node;
            }
        });

        projectFilter(mSpec.filter());
    }

    private void project(ColumnInfo column) {
        String prefix = column.prefix();
        if (prefix != null && mProjectionMap.containsKey(prefix)) {
            // All sub columns will be projected, so no need to add a specific one.
        } else {
            mProjectionMap.put(column.name, column);
        }
    }

    private void projectArgs(JoinSpec.Source source) {
        Map<Integer, ColumnInfo> argAssignments = source.argAssignments();
        if (argAssignments != null) {
            for (ColumnInfo assignment : argAssignments.values()) {
                project(assignment);
            }
        }
    }

    private void projectFilter(RowFilter filter) {
        if (filter != null && filter != TrueFilter.THE) {
            filter.accept(new Visitor() {
                @Override
                public void visit(ColumnToArgFilter filter) {
                    project(filter.column());
                }

                @Override
                public void visit(ColumnToColumnFilter filter) {
                    project(filter.column());
                    project(filter.otherColumn());
                }
            });
        }
    }

    private QuerySpec querySpecFor(JoinSpec.Source source, RowFilter filter) {
        // FIXME: Support orderBy.

        Map<String, ColumnInfo> queryProjection = null;

        if (mProjectionMap == null) {
            // All columns are projected.
        } else if (source instanceof JoinSpec.Column node) {
            String name = node.name();
            if (mProjectionMap.containsKey(name)) {
                // All columns are projected.
            } else {
                queryProjection = new LinkedHashMap<String, ColumnInfo>();
                for (ColumnInfo projColumn : mProjectionMap.values()) {
                    if (name.equals(projColumn.prefix())) {
                        projColumn = projColumn.tail();
                        queryProjection.put(projColumn.name, projColumn);
                    }
                }
            }
        } else if (source instanceof JoinSpec.FullJoin node) {
            queryProjection = new LinkedHashMap<String, ColumnInfo>();
            Map<String, JoinSpec.Column> columnMap = node.columnMap();
            for (ColumnInfo projColumn : mProjectionMap.values()) {
                if (columnMap.containsKey(projColumn.name) ||
                    columnMap.containsKey(projColumn.prefix()))
                {
                    queryProjection.put(projColumn.name, projColumn);
                }
            }
        } else {
            throw new AssertionError();
        }

        return new QuerySpec(queryProjection, null, filter);
    }

    /**
     * Returns the name of a method to invoke (on the launcher) which returns a Query object.
     */
    private String queryMethodFor(JoinSpec.Source source, QuerySpec spec) {
        Map<QuerySpec, MethodMaker> methods = mQueryMethods
            .computeIfAbsent(source.name(), name -> new LinkedHashMap<>());

        return methods.computeIfAbsent(spec, key -> {
            String methodName = source.name() + "_q" + (methods.size() + 1);
            return mLauncherMaker.addMethod(Query.class, methodName);
        }).name();
    }

    /**
     * Add the public row method and the bridge.
     */
    private void addRowMethod() {
        MethodMaker mm = mClassMaker.addMethod(mJoinType, "row").public_().final_();
        mm.return_(mm.field("row"));

        mm = mClassMaker.addMethod(Object.class, "row").public_().final_().bridge();
        mm.return_(mm.this_().invoke(mJoinType, "row", null));
    }

    /**
     * Add the public step method which accepts a row and the bridge method.
     */
    private void addStepMethod() {
        MethodMaker mm = mClassMaker.addMethod(mJoinType, "step", Object.class);
        mm.public_().final_();

        var newJoinRowVar = mm.param(0).cast(mJoinType);

        var newRow = mm.label();
        newJoinRowVar.ifEq(null, newRow);

        var joinRowVar = mm.field("row").get();
        Label ready = mm.label();
        newJoinRowVar.ifEq(joinRowVar, ready);

        // The join row instance changed, so update it to match the current row.
        mSpec.root().accept(new JoinSpec.Visitor() {
            @Override
            public JoinSpec.Node visit(JoinSpec.Column node) {
                ColumnInfo column = node.column();
                var columnVar = mm.field(column.name + "_s").invoke("row").cast(column.type);
                newJoinRowVar.invoke(column.name, columnVar);
                return node;
            }

            @Override
            public JoinSpec.Node visit(JoinSpec.FullJoin node) {
                // The sub scanner will detect the row instance has changed when it's stepped.
                return node;
            }
        });

        ready.goto_();

        newRow.here();
        newJoinRowVar.set(mm.new_(mJoinClass));

        ready.here();

        mm.return_(mm.invoke("loop", newJoinRowVar, true));

        var exVar = mm.catch_(ready, mm.label().here(), Throwable.class);
        mm.invoke("close", newJoinRowVar, exVar).throw_();

        // Add the bridge method.
        MethodMaker bridge = mClassMaker.addMethod
            (Object.class, "step", Object.class).public_().final_().bridge();
        bridge.return_(bridge.this_().invoke(mJoinType, "step", null, bridge.param(0)));
    }

    private void addCloseMethod() {
        MethodMaker mm = mClassMaker.addMethod(null, "close").public_().final_();
        mm.field("row").set(null);
        closeAll(mm, mSpec.root());
    }

    /**
     * Generates code to clear all the columns associated with the given node.
     */
    private static void clearAll(Variable joinRow, JoinSpec.Node node) {
        node.accept(new JoinSpec.Visitor() {
            @Override
            public JoinSpec.Node visit(JoinSpec.Column node) {
                joinRow.invoke(node.name(), (Object) null);
                return node;
            }

            @Override
            public JoinSpec.Node visit(JoinSpec.FullJoin node) {
                JoinSpec.ColumnIterator it = node.columnIterator();
                JoinSpec.Column column;
                while ((column = it.tryNext()) != null) {
                    column.accept(this);
                }
                return node;
            }
        });
    }

    /**
     * Generates code to close all the scanners associated with the given node.
     */
    private void closeAll(MethodMaker mm, JoinSpec.Node node) {
        node.accept(new JoinSpec.Visitor() {
            @Override
            public JoinSpec.Node visit(JoinSpec.Column node) {
                doVisit(node);
                return node;
            }

            @Override
            public JoinSpec.Node visit(JoinSpec.FullJoin node) {
                doVisit(node);
                return node;
            }

            private void doVisit(JoinSpec.Source source) {
                if (source == mLastExists) {
                    // If source is the last and exists was called, then no scanner was created.
                } else {
                    mm.field(source.name() + "_s").invoke("close");
                }
            }
        });
    }

    /**
     * private J loop(J joinRow, boolean jumpIn) ...
     */
    private void addLoopMethod() {
        MethodMaker mm = mClassMaker.addMethod
            (mJoinType, "loop", mJoinType, boolean.class).protected_().final_().override();

        mJoinRowVar = mm.param(0);
        mObjectResultVar = mm.var(Object.class);

        Label jumpIn = mm.label();
        mm.param(1).ifTrue(jumpIn);
        Label jumpOut = mm.label();
        loop(mSpec.root(), jumpOut, jumpIn);
        jumpOut.here();

        Label finished = mm.label();
        gotoIfLoopEnded(finished);

        RowFilter filter = mSpec.filter();
        if (filter != null && filter != TrueFilter.THE) {
            invokePredicate(filter).ifFalse(jumpIn);
        }

        mm.field("row").set(mJoinRowVar);
        mm.return_(mJoinRowVar);

        // No rows left.
        finished.here();
        mm.field("row").set(null);
        mm.return_(null);
    }

    /**
     * @param jumpOut a label which is positioned by the caller for jumping out of the loop
     * @param jumpIn a label which is positioned by this method for jumping back into the loop
     * right after the jumpOut branch
     */
    private void loop(JoinSpec.Node node, Label jumpOut, Label jumpIn) {
        node.accept(new JoinSpec.Visitor() {
            @Override
            public JoinSpec.Node visit(JoinSpec.Column node) {
                loopColumn(node, jumpOut, jumpIn);
                return node;
            }

            @Override
            public JoinSpec.Node visit(JoinSpec.JoinOp node) {
                loopJoinOp(node, jumpOut, jumpIn);
                return node;
            }

            @Override
            public JoinSpec.Node visit(JoinSpec.InnerJoins node) {
                return visit(node.toJoinOp());
            }

            @Override
            public JoinSpec.Node visit(JoinSpec.FullJoin node) {
                loopFullJoin(node, jumpOut, jumpIn);
                return node;
            }
        });
    }

    private void loopColumn(JoinSpec.Column node, Label jumpOut, Label jumpIn) {
        MethodMaker mm = mJoinRowVar.methodMaker();

        RowFilter remainder = node.remainder();
        boolean hasRemainder = remainder != null && remainder != TrueFilter.THE;

        Map<Integer, ColumnInfo> argAssignments = node.argAssignments();
        boolean hasArgAssignments = argAssignments != null && !argAssignments.isEmpty();

        if (mParentType == JoinSpec.T_LEFT_ANTI && node == mLastSource
            && !hasRemainder && !hasArgAssignments)
        {
            // Only need to check if a row exists.
            mLastExists = node;
            setBooleanResult(makeExists(node));
            jumpOut.goto_();
            jumpIn.here();
            setBooleanResult(false);
            jumpOut.goto_();
            return;
        }

        Variable scannerVar = makeNewScanner(node);

        ColumnInfo column = node.column();

        Field scannerField = mm.field(column.name + "_s");
        scannerField.set(scannerVar);

        var levelRowVar = scannerVar.invoke("row");

        Label start = mm.label().here();

        var typedLevelRowVar = levelRowVar.cast(column.type);
        mJoinRowVar.invoke(column.name, typedLevelRowVar);

        Label cont = null;

        if (hasRemainder || hasArgAssignments) {
            // If the level row is null, then the scanner has reached the end. An explicit null
            // check isn't needed when there's no remainder or arg assignments because the "if"
            // target is the same whether or not the level row is null.
            cont = mm.label();
            typedLevelRowVar.ifEq(null, cont);
        }

        Label step = null;

        if (hasRemainder) {
            // Need to apply a predicate before the level row can be accepted. When the
            // predicate returns false, jump to the location which steps ahead.
            step = mm.label();
            invokePredicate(remainder).ifFalse(step);
        }

        if (hasArgAssignments) {
            // Set the necessary arguments which are needed by other levels.
            var argsVar = mm.field("args").get();
            for (Map.Entry<Integer, ColumnInfo> e : argAssignments.entrySet()) {
                argsVar.aset(e.getKey() - 1, typedLevelRowVar.invoke(e.getValue().tail().name));
            }
        }

        if (cont != null) {
            cont.here();
        }

        setObjectResult(typedLevelRowVar);

        jumpOut.goto_();
        jumpIn.here();

        if (node == mLastSource) {
            restoreColumns();
        }

        scannerVar.set(scannerField.get());
        levelRowVar.set(scannerVar.invoke("row"));

        if (step != null) {
            step.here();
        }

        levelRowVar.set(scannerVar.invoke("step", mJoinRowVar.invoke(column.name)));

        start.goto_();
    }

    private void loopJoinOp(JoinSpec.JoinOp node, Label jumpOut, Label jumpIn) {
        MethodMaker mm = mJoinRowVar.methodMaker();

        Label step = mm.label();

        // Loop over the left child.
        {
            Label leftJumpOut = mm.label();
            loop(node.leftChild(), leftJumpOut, step);
            leftJumpOut.here();
        }

        gotoIfLoopEnded(jumpOut);

        int type = node.type();
        mParentType = type;

        // Loop over the right child.
        {
            Label rightJumpOut = mm.label();
            loop(node.rightChild(), rightJumpOut, jumpIn);
            rightJumpOut.here();
        }

        switch (type) {
            case JoinSpec.T_INNER, JoinSpec.T_STRAIGHT -> {
                // Regular inner join.
                gotoIfLoopEnded(step);
                jumpOut.goto_();
                return;
            }
            case JoinSpec.T_LEFT_OUTER, JoinSpec.T_LEFT_ANTI -> {
                // Okay.
            }
            default -> {
                throw new UnsupportedOperationException("Unsupported join: " + node);
            }
        }

        // Define a "ready" field which is set when the right child result isn't null. If the
        // result is null and "ready" is false, then produce a row with nulls.

        String readyName = addReadyField();
        var readyField = mm.field(readyName);

        Label hasResult = mm.label();
        gotoIfLoopNotEnded(hasResult);

        var readyVar = readyField.get().xor(1);
        readyField.set(readyVar);

        clearAll(mJoinRowVar, node.rightChild());

        // TODO: If 1, do any other ready fields need to be set?
        readyVar.ifEq(0, step); // step to the next row

        // Before producing a row with nulls, an additional predicate test might be needed.
        RowFilter predicate = node.predicate();
        if (predicate != null && predicate != TrueFilter.THE) {
            invokePredicate(predicate).ifFalse(step);
        }

        setObjectResult(mJoinRowVar);
        jumpOut.goto_();

        hasResult.here();
        readyField.set(1);

        if (type == JoinSpec.T_LEFT_ANTI) {
            closeAll(mm, node.rightChild());
            jumpIn.goto_();
        } else {
            jumpOut.goto_();
        }
    }

    private void loopFullJoin(JoinSpec.FullJoin node, Label jumpOut, Label jumpIn) {
        MethodMaker mm = mJoinRowVar.methodMaker();

        RowFilter predicate = node.predicate();
        boolean hasPredicate = predicate != null && predicate != TrueFilter.THE;

        Map<Integer, ColumnInfo> argAssignments = node.argAssignments();
        boolean hasArgAssignments = argAssignments != null && !argAssignments.isEmpty();

        if (mParentType == JoinSpec.T_LEFT_ANTI && node == mLastSource
            && !hasPredicate && !hasArgAssignments)
        {
            // Only need to check if a row exists.
            mLastExists = node;
            setBooleanResult(makeExists(node));
            jumpOut.goto_();
            jumpIn.here();
            setBooleanResult(false);
            jumpOut.goto_();
            return;
        }

        Variable scannerVar = makeNewScanner(node);

        Field scannerField = mm.field(node.name() + "_s");
        scannerField.set(scannerVar);

        var levelRowVar = scannerVar.invoke("row");

        Label start = mm.label().here();

        Label cont = null;

        if (hasPredicate || hasArgAssignments) {
            // If the level row is null, then the scanner has reached the end.
            cont = mm.label();
            levelRowVar.ifEq(null, cont);
        }

        Label step = null;

        if (hasPredicate) {
            // Before producing a row with nulls, an additional predicate test might be needed.
            step = mm.label();
            invokePredicate(predicate).ifFalse(step);
        }

        if (hasArgAssignments) {
            // Set the necessary arguments which are needed by other levels.
            var argsVar = mm.field("args").get();
            var typedLevelRowVar = levelRowVar.cast(mJoinType);
            for (Map.Entry<Integer, ColumnInfo> e : argAssignments.entrySet()) {
                argsVar.aset(e.getKey() - 1, accessPath(typedLevelRowVar, e.getValue()));
            }
        }

        if (cont != null) {
            cont.here();
        }

        setObjectResult(levelRowVar);

        jumpOut.goto_();
        jumpIn.here();

        if (node == mLastSource) {
            restoreColumns();
        }

        scannerVar.set(scannerField.get());
        levelRowVar.set(scannerVar.invoke("row"));

        if (step != null) {
            step.here();
        }

        levelRowVar.set(scannerVar.invoke("step", mJoinRowVar));

        start.goto_();
    }

    /**
     * Generates code which follows a path to obtain the column value, yielding null if any
     * path component is null.
     */
    private static Variable accessPath(Variable objVar, ColumnInfo ci) {
        MethodMaker mm = objVar.methodMaker();
        Variable resultVar = mm.var(Object.class);
        Label fail = mm.label();

        while (true) {
            String prefix = ci.prefix();
            if (prefix == null) {
                resultVar.set(objVar.invoke(ci.name));
                break;
            }
            objVar = objVar.invoke(prefix);
            objVar.ifEq(null, fail);
            ci = ci.tail();
        }

        Label cont = mm.label().goto_();
        fail.here();
        resultVar.set(null);
        cont.here();

        return resultVar;
    }

    private void restoreColumns() {
        // Restore join levels for all but the last source.

        mSpec.root().accept(new JoinSpec.Visitor() {
            @Override
            public JoinSpec.Node visit(JoinSpec.Column node) {
                if (node == mLastSource) {
                    return node;
                }

                MethodMaker mm = mJoinRowVar.methodMaker();
                String name = node.name();

                Label ready = mm.label();
                mJoinRowVar.invoke(name).ifNe(null, ready);

                var levelRowVar = mm.field(name + "_s").invoke("row");
                levelRowVar.ifEq(null, ready);
                Class<?> rowType = node.column().type;
                Class<?> rowClass = RowMaker.find(rowType);
                levelRowVar = levelRowVar.cast(rowClass).invoke(rowType, "clone", null);
                mJoinRowVar.invoke(name, levelRowVar);

                ready.here();

                return node;
            }

            @Override
            public JoinSpec.Node visit(JoinSpec.FullJoin node) {
                if (node == mLastSource) {
                    return node;
                }

                MethodMaker mm = mJoinRowVar.methodMaker();

                JoinSpec.ColumnIterator it = node.columnIterator();
                JoinSpec.Column column;
                while ((column = it.tryNext()) != null) {
                    String name = column.name();

                    Label ready = mm.label();
                    mJoinRowVar.invoke(name).ifNe(null, ready);

                    var levelRowVar = mm.field(node.name() + "_s").invoke("row").cast(mJoinType);
                    var subRowVar = levelRowVar.invoke(name);
                    subRowVar.ifEq(null, ready);
                    Class<?> subRowType = column.column().type;
                    Class<?> subRowClass = RowMaker.find(subRowType);
                    subRowVar = subRowVar.cast(subRowClass).invoke(subRowType, "clone", null);
                    mJoinRowVar.invoke(name, subRowVar);

                    ready.here();
                }

                return node;
            }
        });
    }

    private void setObjectResult(Object result) {
        mObjectResultVar.set(result);
        mChosenResultVar = mObjectResultVar;
    }

    private void setBooleanResult(Object result) {
        var resultVar = mBooleanResultVar;
        if (resultVar == null) {
            mBooleanResultVar = resultVar = mJoinRowVar.methodMaker().var(boolean.class);
        }
        resultVar.set(result);
        mChosenResultVar = resultVar;
    }

    private void gotoIfLoopEnded(Label target) {
        var resultVar = mChosenResultVar;
        if (resultVar.classType() == boolean.class) {
            resultVar.ifFalse(target);
        } else {
            resultVar.ifEq(null, target);
        }
    }

    private void gotoIfLoopNotEnded(Label target) {
        var resultVar = mChosenResultVar;
        if (resultVar.classType() == boolean.class) {
            resultVar.ifTrue(target);
        } else {
            resultVar.ifNe(null, target);
        }
    }

    private String addReadyField() {
        String name = "r" + mReadyNum++;
        mClassMaker.addField(int.class, name).private_();
        return name;
    }

    /**
     * Generates a predicate test method, invokes it against mJoinRowVar, and returns a boolean
     * result.
     */
    private Variable invokePredicate(RowFilter predicate) {
        Map<RowFilter, String> map = mPredicateMethods;
        String methodName;

        if (map == null) {
            mPredicateMethods = map = new HashMap<>();
            methodName = null;
        } else {
            methodName = map.get(predicate);
        }

        if (methodName == null) {
            methodName = "test" + map.size();

            MethodMaker testMaker = mClassMaker
                .addMethod(boolean.class, methodName, mJoinRowVar).private_();

            new JoinPredicateMaker(mJoinType, predicate)
                .generate(mCtorMaker, testMaker, testMaker.param(0));

            map.put(predicate, methodName);
        }

        return mJoinRowVar.methodMaker().invoke(methodName, mJoinRowVar);
    }

    /**
     * @return a Variable of type boolean
     */
    private Variable makeExists(JoinSpec.Source source) {
        return makeNewScannerOrExists(source, true);
    }

    /**
     * @return a Variable with a Scanner instance
     */
    private Variable makeNewScanner(JoinSpec.Source source) {
        return makeNewScannerOrExists(source, false);
    }

    private Variable makeNewScannerOrExists(JoinSpec.Source source, boolean exists) {
        // Define the code in a separate method, which helps with debugging because the stack
        // trace will include the name of the joined column. The code might also become quite
        // large if special null argument processing is required, and so defining it separately
        // reduces the likelihood that the loop method becomes too large.

        String name = source.name();
        Class<?> returnType;

        if (exists) {
            name += "_x";
            returnType = boolean.class;
        } else {
            name += "_s";
            returnType = Scanner.class;
        }

        MethodMaker mm = mClassMaker.addMethod(returnType, name, mJoinType).private_();
        new ScannerMaker(mm, source, exists).make();

        return mJoinRowVar.methodMaker().invoke(name, mJoinRowVar);
    }

    /**
     * Makes code that calls newScanner on the join column.
     */
    private class ScannerMaker implements IntUnaryOperator {
        private final MethodMaker mMethodMaker;
        private final JoinSpec.Source mSource;
        private final boolean mExists;
        private final Variable mLauncherVar, mTxnVar, mArgsVar, mJoinRowVar;
        private final RowFilter mFilter;

        private int[] mArgsToCheck;
        private int mNumArgsToCheck;

        /**
         * @param exists pass true to call exists instead of newScanner
         */
        ScannerMaker(MethodMaker mm, JoinSpec.Source source, boolean exists) {
            mMethodMaker = mm;
            mSource = source;
            mExists = exists;

            mTxnVar = mm.field("txn");
            mLauncherVar = mm.field("launcher");
            mArgsVar = mm.field("args");
            mJoinRowVar = mm.param(0);

            RowFilter filter = source.filter();

            if (filter == null) {
                mFilter = TrueFilter.THE;
            } else {
                mFilter = filter.replaceArguments(this);
            }
        }

        /**
         * Defined in IntUnaryOperator and used to find arguments which require null checks.
         */
        @Override
        public int applyAsInt(int argNum) {
            if (argNum < 0) {
                argNum = -argNum;
                if (mArgsToCheck == null) {
                    mArgsToCheck = new int[4];
                } else if (mNumArgsToCheck >= mArgsToCheck.length) {
                    mArgsToCheck = Arrays.copyOf(mArgsToCheck, mArgsToCheck.length << 1);
                }
                mArgsToCheck[mNumArgsToCheck++] = argNum;
            }

            return argNum;
        }

        void make() {
            doMake(mFilter, mNumArgsToCheck - 1);
        }

        /**
         * Recursively generates code which assigns arguments and returns a new scanner (or
         * calls exists).
         *
         * <p>This algorithm doesn't scale when too many arguments require null checks. If
         * there are 30 such arguments, then one billion code paths are generated (2^n), which
         * isn't going to work.
         *
         * <p>In practice, join predicates aren't expected to be that large, and filters
         * against each level aren't expected to be that complicated. The branches that handle
         * nulls operate against filters which have the null comparisons removed, and this
         * leads to filters eventually becoming "true" or "false". The branch can then
         * short-circuit, reducing the overall code that needs to be generated.
         */
        private void doMake(RowFilter filter, int checkArg) {
            while (checkArg >= 0) {
                int argNum = mArgsToCheck[checkArg];
                RowFilter asNullFilter = filter.argumentAsNull(argNum);

                if (asNullFilter == filter) {
                    break;
                }

                Label isNull = mMethodMaker.label();
                mArgsVar.aget(argNum - 1).ifEq(null, isNull);

                // Continue generating code down the branch where the argument isn't null.
                doMake(filter, checkArg - 1);

                isNull.here();

                // Continue generating code down the branch where the argument is null.
                filter = asNullFilter;
                checkArg--;
            }

            if (mExists) {
                QuerySpec spec = querySpecFor(mSource, filter);
                spec = spec.withProjection(Collections.emptyMap());
                String queryMethod = queryMethodFor(mSource, spec);

                final Variable resultVar;

                if (filter == FalseFilter.THE) {
                    resultVar = mMethodMaker.var(boolean.class).set(false);
                } else if (filter == TrueFilter.THE) {
                    resultVar = mLauncherVar.invoke(queryMethod).invoke
                        ("anyRows", levelRowVar(), mTxnVar);
                } else {
                    Map<String, JoinSpec.Source> sources = mSource.argSources();
                    if (sources != null) {
                        // If any argument source is null, return false.
                        for (Map.Entry<String, JoinSpec.Source> e : sources.entrySet()) {
                            if (e.getValue().isNullable()) {
                                Label cont = mMethodMaker.label();
                                mJoinRowVar.invoke(e.getKey()).ifNe(null, cont);
                                mMethodMaker.return_(false);
                                cont.here();
                            }
                        }
                    }

                    resultVar = mLauncherVar.invoke(queryMethod).invoke
                        ("anyRows", levelRowVar(), mTxnVar, mArgsVar);
                }

                mMethodMaker.return_(resultVar);
                return;
            }

            if (filter == FalseFilter.THE) {
                mMethodMaker.return_(mMethodMaker.var(EmptyScanner.class).field("THE"));
                return;
            }

            QuerySpec spec = querySpecFor(mSource, filter);
            String queryMethod = queryMethodFor(mSource, spec);

            if (spec.isFullScan()) {
                mMethodMaker.return_(mLauncherVar.invoke(queryMethod).invoke
                                     ("newScanner", levelRowVar(), mTxnVar));
                return;
            }

            Map<String, JoinSpec.Source> sources = mSource.argSources();
            if (sources != null) {
                // If any argument source is null, return an empty scanner.
                Label empty = null;
                for (Map.Entry<String, JoinSpec.Source> e : sources.entrySet()) {
                    if (e.getValue().isNullable()) {
                        if (empty == null) {
                            empty = mMethodMaker.label();
                        }
                        mJoinRowVar.invoke(e.getKey()).ifEq(null, empty);
                    }
                }
                if (empty != null) {
                    Label cont = mMethodMaker.label().goto_();
                    empty.here();
                    mMethodMaker.return_(mMethodMaker.var(EmptyScanner.class).field("THE"));
                    cont.here();
                }
            }

            mMethodMaker.return_(mLauncherVar.invoke(queryMethod).invoke
                                 ("newScanner", levelRowVar(), mTxnVar, mArgsVar));
        }

        private Variable levelRowVar() {
            if (mSource instanceof JoinSpec.Column c) {
                return mJoinRowVar.invoke(c.name());
            } else {
                return mJoinRowVar;
            }
        }
    }
}
