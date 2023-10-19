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

import java.util.List;
import java.util.Objects;

import org.cojen.maker.Label;
import org.cojen.maker.Variable;

import org.cojen.tupl.rows.ConvertCallSite;

/**
 * Defines a parameter which is passed into a query at runtime.
 *
 * @author Brian S. O'Neill
 */
public final class ParamNode extends Node {
    /**
     * @param name can be null to automatically assign a name
     */
    public static ParamNode make(String name, int ordinal) {
        return new ParamNode(name, ordinal);
    }

    private String mName;
    private final int mOrdinal;

    private ParamNode(String name, int ordinal) {
        mName = name;
        mOrdinal = ordinal;
    }

    @Override
    public Type type() {
        return BasicType.OBJECT;
    }

    @Override
    public Node asType(Type type) {
        // FIXME: runtime cast using ConvertCallSite
        throw null;
    }

    @Override
    public String name() {
        if (mName == null) {
            mName = "?" + mOrdinal;
        }
        return mName;
    }

    @Override
    public int highestParamOrdinal() {
        return mOrdinal;
    }

    @Override
    public boolean isPureFunction() {
        return true;
    }

    @Override
    public boolean isPureFilterTerm() {
        return true;
    }

    @Override
    public int appendPureFilter(StringBuilder query, List<Object> argConstants, int argOrdinal) {
        query.append('?').append(mOrdinal);
        return argOrdinal;
    }

    @Override
    public Variable makeEval(MakerContext context) {
        var resultRef = context.refFor(this);
        var result = resultRef.get();
        if (result != null) {
            return result;
        } else {
            return resultRef.set(context.argsVar.aget(mOrdinal - 1));
        }
    }

    @Override
    public void makeFilter(MakerContext context, Label pass, Label fail) {
        ConvertCallSite.make(context.methodMaker(), boolean.class, makeEval(context)).ifTrue(pass);
        fail.goto_();
    }

    public int ordinal() {
        return mOrdinal;
    }

    @Override
    public int hashCode() {
        return mOrdinal * 970840757;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ParamNode pn && mOrdinal == pn.mOrdinal;
    }
}
