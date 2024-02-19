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

package org.cojen.tupl.model;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * String concatenation.
 *
 * @author Brian S. O'Neill
 */
public final class ConcatNode extends Node {
    public static Node make(Node left, Node right) {
        int typeCode = Type.TYPE_UTF8;

        if (left.isNullable()) {
            if (left == ConstantNode.NULL) {
                return left;
            }
            typeCode |= Type.TYPE_NULLABLE;
        }

        if (right.isNullable()) {
            if (right == ConstantNode.NULL) {
                return right;
            }
            typeCode |= Type.TYPE_NULLABLE;
        }

        return new ConcatNode(BasicType.make(String.class, typeCode), null, left, right);
    }

    private final Type mType;
    private String mName;
    private final Node mLeft, mRight;
    private Node[] mSubNodes;

    private ConcatNode(Type type, String name, Node left, Node right) {
        mType = type;
        mName = name;
        mLeft = left;
        mRight = right;
    }

    private ConcatNode(ConcatNode cn, String name) {
        this(cn.mType, name, cn.mLeft, cn.mRight);
        mSubNodes = cn.mSubNodes;
    }

    @Override
    public Type type() {
        return mType;
    }

    @Override
    public Node asType(Type type) {
        return ConversionNode.make(this, type);
    }

    @Override
    public String name() {
        if (mName == null) {
            var b = new StringBuilder();
            for (Node n : subNodes()) {
                if (!b.isEmpty()) {
                    b.append(" || ");
                }
                b.append(n.name());
            }
            mName = b.toString();
        }

        return mName;
    }

    @Override
    public Node withName(String name) {
        return name.equals(mName) ? this : new ConcatNode(this, name);
    }

    @Override
    public int maxArgument() {
        return Math.max(mLeft.maxArgument(), mRight.maxArgument());
    }

    @Override
    public boolean isPureFunction() {
        return mLeft.isPureFunction() && mRight.isPureFunction();
    }

    @Override
    public boolean isNullable() {
        return mType.isNullable();
    }

    @Override
    public void evalColumns(Set<String> columns) {
        mLeft.evalColumns(columns);
        mRight.evalColumns(columns);
    }

    @Override
    public Variable makeEval(EvalContext context) {
        EvalContext.ResultRef resultRef;

        if (isPureFunction()) {
            resultRef = context.refFor(this);
            var result = resultRef.get();
            if (result != null) {
                return result;
            }
        } else {
            resultRef = null;
        }

        MethodMaker mm = context.methodMaker();
        var resultVar = mm.var(String.class);
        Label done = mm.label();

        Node[] subNodes = subNodes();
        var subVars = new Variable[subNodes.length];

        for (int i=0; i<subVars.length; i++) {
            Node subNode = subNodes[i];
            subVars[i] = subNode.makeEval(context);
            if (subNode.isNullable()) {
                Label next = mm.label();
                subVars[i].ifNe(null, next);
                resultVar.set(null);
                done.goto_();
                next.here();
            }
        }

        resultVar.set(mm.concat((Object[]) subVars));

        done.here();

        if (resultRef != null) {
            resultVar = resultRef.set(resultVar);
        }

        return resultVar;
    }

    @Override
    public boolean canThrowRuntimeException() {
        return mLeft.canThrowRuntimeException() || mRight.canThrowRuntimeException();
    }

    @Override
    public ConcatNode replaceConstants(Map<ConstantNode, FieldNode> map, String prefix) {
        Node left = mLeft.replaceConstants(map, prefix);
        Node right = mRight.replaceConstants(map, prefix);
        if (left == mLeft && right == mRight) {
            return this;
        }
        return new ConcatNode(mType, mName, left, right);
    }

    private static final byte K_TYPE = KeyEncoder.allocType();

    @Override
    protected void encodeKey(KeyEncoder enc) {
        if (enc.encode(this, K_TYPE)) {
            enc.encodeNodes(subNodes());
        }
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(subNodes());
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ConcatNode cn && Arrays.equals(subNodes(), cn.subNodes());
    }

    private Node[] subNodes() {
        Node[] subNodes = mSubNodes;

        if (subNodes == null) {
            subNodes = new Node[countSubNodes()];
            int ix = gatherSubNodes(subNodes, 0);
            assert ix == subNodes.length;
            mSubNodes = subNodes;
        }

        return subNodes;
    }

    private int countSubNodes() {
        Node[] subNodes = mSubNodes;
        if (subNodes != null) {
            return subNodes.length;
        }
        return countSubNodes(mLeft) + countSubNodes(mRight);
    }

    private static int countSubNodes(Node subNode) {
        return subNode instanceof ConcatNode cn ? cn.countSubNodes() : 1;
    }

    private int gatherSubNodes(Node[] subNodes, int ix) {
        ix = gatherSubNodes(subNodes, ix, mLeft);
        ix = gatherSubNodes(subNodes, ix, mRight);
        return ix;
    }

    private static int gatherSubNodes(Node[] subNodes, int ix, Node subNode) {
        if (subNode instanceof ConcatNode cn) {
            return cn.gatherSubNodes(subNodes, ix);
        } else {
            subNodes[ix++] = subNode;
            return ix;
        }
    }
}
