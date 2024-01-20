/*
 *  Copyright 2021 Cojen.org
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
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.cojen.tupl.table;

import java.lang.invoke.CallSite;
import java.lang.invoke.ConstantCallSite;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.invoke.MutableCallSite;

import java.util.function.Supplier;

import org.cojen.maker.MethodMaker;

/**
 * Retries generating code after an exception, over and over until it succeeds.
 *
 * @author Brian S O'Neill
 */
public class ExceptionCallSite extends MutableCallSite {
    /**
     * Returns a ConstantCallSite when the generator doesn't fail, which means that it's just a
     * plain wrapper around a MethodHandle. Otherwise, returns an ExceptionCallSite which calls
     * the generator when invoked, and when the generator succeeds, the target is replaced.
     * Assuming that code inlining magic works, the extra level of indirection will eventually
     * be removed.
     *
     * @param generator returns a MethodHandle or a Failed object
     */
    static CallSite make(Supplier<Object> generator) {
        Object result = generator.get();
        if (result instanceof MethodHandle mh) {
            return new ConstantCallSite(mh);
        } else if (result instanceof Failed f) {
            return new ExceptionCallSite(generator, f);
        } else {
            throw new IllegalStateException(String.valueOf(result));
        }
    }

    record Failed(MethodType mt, MethodMaker mm, Throwable ex) { }

    private final Supplier<Object> mGenerator;
    private Throwable mException;
    private Thread mOrigin;

    private ExceptionCallSite(Supplier<Object> generator, Failed f) {
        super(f.mt);

        mGenerator = generator;
        mException = f.ex;
        mOrigin = Thread.currentThread();

        var ecs = f.mm.var(ExceptionCallSite.class).setExact(this);
        var mh = ecs.invoke("call");
        ecs.invoke("setTarget", mh);

        // Invoke the MethodHandle directly after the retry succeeds, but subsequent calls will
        // use the new target directly.

        Class<?>[] paramTypes = f.mt.parameterArray();
        var params = new Object[paramTypes.length];
        for (int i=0; i<params.length; i++) {
            params[i] = f.mm.param(i);
        }

        var result = mh.invoke(f.mt.returnType(), "invokeExact", paramTypes, params);

        if (f.mt.returnType() == void.class) {
            f.mm.return_();
        } else {
            f.mm.return_(result);
        }

        setTarget(f.mm.finish());
    }
    
    public MethodHandle call() throws Throwable {
        Throwable e = mException;
        if (e != null && mOrigin == Thread.currentThread()) {
            // Throw the initial exception, and then retry later.
            mException = null;
            mOrigin = null;
            throw e;
        }

        Object result = mGenerator.get();

        if (result instanceof MethodHandle mh) {
            return mh;
        } else if (result instanceof Failed f) {
            throw f.ex;
        } else {
            throw new IllegalStateException(String.valueOf(result));
        }
    }
}
