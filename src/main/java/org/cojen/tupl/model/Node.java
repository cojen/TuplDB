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

import java.util.Map;
import java.util.Set;

import org.cojen.maker.Label;
import org.cojen.maker.Variable;

import org.cojen.tupl.table.RowInfo;
import org.cojen.tupl.table.WeakCache;

import org.cojen.tupl.table.filter.OpaqueFilter;
import org.cojen.tupl.table.filter.RowFilter;

/**
 * A node represents an AST element of a query.
 *
 * @author Brian S. O'Neill
 */
public abstract sealed class Node
    permits BinaryOpNode, CaseNode, CommandNode, ColumnNode, ConcatNode, ConstantNode,
    ConversionNode, FieldNode, ParamNode, RelationNode
{
    private static WeakCache<Object, Object, Node> cCodeCache;

    static {
        cCodeCache = new WeakCache<>() {
            @Override
            public Object newValue(Object key, Node node) {
                return node.makeCode();
            }
        };
    }

    public abstract Type type();

    /**
     * Return this or a replacement node. If a conversion is required, an exact conversion is
     * performed, which can throw an exception at runtime.
     */
    public abstract Node asType(Type type);

    /**
     * Apply an arithmetic negation operation.
     */
    public Node negate() {
        return BinaryOpNode.make(BinaryOpNode.OP_SUB, ConstantNode.make(0), this);
    }

    /**
     * Apply a boolean not operation.
     */
    public Node not() {
        return BinaryOpNode.make
            (BinaryOpNode.OP_EQ, asType(BasicType.BOOLEAN), ConstantNode.make(false));
    }

    /**
     * Returns a non-null name, which doesn't affect the functionality of this node.
     *
     * @see #equals
     */
    public abstract String name();

    public abstract Node withName(String name);

    /**
     * Returns the highest query argument needed by this node, which is zero if none are
     * needed.
     */
    public abstract int maxArgument();

    /**
     * Returns true if this node represents a pure function with respect to the current row,
     * returning the same result upon each invocation.
     */
    public abstract boolean isPureFunction();

    /**
     * Performs a best effort conversion of this node into a RowFilter. Any nodes which cannot be
     * converted are represented by OpaqueFilters which have the node attached.
     *
     * @param columns all converted columns are put into this map
     */
    public RowFilter toRowFilter(RowInfo info, Map<String, ColumnNode> columns) {
        return new OpaqueFilter(false, this);
    }

    /**
     * Returns true if this node represents a simple disjunction filter which exactly matches
     * columns to parameters or constants, and every column is matched at most once.
     *
     * @param matches the matches are put into this map; the map values are either ParamNode or
     * ConstantNode
     */
    public boolean isSimpleDisjunction(Map<ColumnNode, Node> matches) {
        return false;
    }

    /**
     * Returns true if any path element is null or the evaluated result can be null.
     */
    public abstract boolean isNullable();

    /**
     * Returns a ColumnNode if this node is a ColumnNode, or if this node applies conversion to
     * a ColumnNode. Null is returned otherwise.
     */
    public ColumnNode extractColumn() {
        return null;
    }

    /**
     * Adds into the given set the fully qualified names of all the columns that makeEval will
     * directly use.
     */
    public abstract void evalColumns(Set<String> columns);

    /**
     * Generates code which evaluates an expression. The context tracks nodes which have
     * already been evaluated and is updated by this method.
     */
    public abstract Variable makeEval(EvalContext context);

    /**
     * Generates code which evaluates an expression for branching to a pass or fail label.
     * Short-circuit logic is used, and so the expression might only be partially evaluated.
     *
     * @throws IllegalStateException if unsupported
     */
    public void makeFilter(EvalContext context, Label pass, Label fail) {
        makeEval(context).ifTrue(pass);
        fail.goto_();
    }

    /**
     * Generates filter code which returns a boolean Variable.
     *
     * @throws IllegalStateException if unsupported
     */
    public Variable makeFilterEval(EvalContext context) {
        Variable result = makeEval(context);
        if (result.classType() != boolean.class) {
            throw new IllegalStateException();
        }
        return result;
    }

    /**
     * Generates filter code for the SelectMappedNode remap method. It performs eager
     * evaluation and suppresses exceptions when possible.
     *
     * @return an assigned Object variable which references a Boolean or a RuntimeException
     */
    public Variable makeFilterEvalRemap(EvalContext context) {
        return context.methodMaker().var(Boolean.class).set(makeFilterEval(context));
    }

    /**
     * Returns true if the code generated by this node might throw a RuntimeException. A return
     * value of false doesn't indicate that a RuntimeException isn't possible, but instead
     * it indicates that it's unlikely.
     */
    public boolean canThrowRuntimeException() {
        return true;
    }

    /**
     * Returns true if the filtering code generated by this node can throw a RuntimeException
     * which might be suppressed if the evaluation order was changed. This implies that the
     * node performs short-circuit logic and that canThrowRuntimeException returns true.
     */
    public boolean hasOrderDependentException() {
        return false;
    }

    /**
     * Replace all the constants referenced by this node with fields, returning a new node. If
     * this node doesn't reference any constants, then the same node is returned.
     *
     * @param map generated field replacements are put into this map
     * @param prefix name prefix for all the generated field names
     */
    public abstract Node replaceConstants(Map<ConstantNode, FieldNode> map, String prefix);

    /**
     * Replaces the constants referenced by an array of nodes, returning a new array only if
     * any node was replaced.
     */
    public static Node[] replaceConstants(Node[] nodes, 
                                          Map<ConstantNode, FieldNode> map, String prefix)
    {
        if (nodes != null) {
            boolean cloned = false;

            for (int i=0; i<nodes.length; i++) {
                Node node = nodes[i];
                Node replaced = node.replaceConstants(map, prefix);
                if (replaced != node) {
                    if (!cloned) {
                        nodes = nodes.clone();
                        cloned = true;
                    }
                    nodes[i] = replaced;
                }
            }
        }

        return nodes;
    }

    /**
     * Returns an object representing top-level code generated by this node. The code is cached
     * using the key generated by encodeKey, and the makeCode method is called upon a cache
     * miss.
     */
    protected Object code() {
        return cCodeCache.obtain(makeKey(), this);
    }

    /**
     * Override this method if the node supports top-level code generation.
     */
    protected Object makeCode() {
        throw new UnsupportedOperationException();
    }

    /**
     * Returns a cache instance by calling encodeKey.
     */
    protected final Object makeKey() {
        var enc = new KeyEncoder();
        encodeKey(enc);
        return enc.finish();
    }

    /**
     * Encodes this Node into such that it can be used as a cache key for generated code.
     * Although the Node itself can be used as such, the cache key should have a smaller memory
     * footprint.
     *
     * Each encodable entity must define the following constant:
     *
     *     private static final byte K_TYPE = KeyEncoder.allocType();
     *
     * Simple entities can simply call encodeType(K_TYPE), but multipart entities should call
     * encode(this, K_TYPE) and only proceed with the encoding when true is returned. This
     * reduces entity encoding duplication and prevents infinite cycles.
     * 
     * Although the key doesn't need to be decoded, the encoding format should be defined as if
     * it could be decoded, to prevent false key matches. Like the equals method, the node name
     * should not be included.
     */
    protected abstract void encodeKey(KeyEncoder enc);

    /**
     * @see #equals
     */
    @Override
    public abstract int hashCode();

    /**
     * The equals and hashCode methods only compare for equivalent functionality, and thus the
     * node's name is generally excluded from the comparison.
     */
    @Override
    public abstract boolean equals(Object obj);

    @Override
    public String toString() {
        return name();
    }
}
