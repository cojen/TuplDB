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
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.MutableCallSite;
import java.lang.invoke.VarHandle;

import java.util.function.IntFunction;

import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;

/**
 * A SwitchCallSite delegates to MethodHandles selected by an int key. The cases are built
 * dynamically upon first use.
 *
 * @author Brian S O'Neill
 */
public class SwitchCallSite extends MutableCallSite {
    private static final int MAX_CASES = 100;

    private final IntFunction<Object> mGenerator;

    // Hashtable which maps int keys to MethodHandles.
    private Entry[] mEntries;
    private int mSize;

    /**
     * The first parameter of the given MethodType must be an int key. The remaining parameters
     * and return type can be anything. The generator must make MethodHandles which match the
     * given MethodType except without the key parameter.
     *
     * @param lookup typically the parameter passed to a bootstrap method
     * @param mt must be: <ret> (int key, <remaining>)
     * @param generator supplies cases for keys; supplies MethodHandle or
     * ExceptionCallSite.Failed. MethodType must omit the key: <ret> (<remaining>)
     */
    SwitchCallSite(MethodHandles.Lookup lookup, MethodType mt, IntFunction<Object> generator) {
        super(mt);
        mGenerator = generator;
        makeDelegator(lookup);
    }

    /**
     * Makes the delegator and sets it as the current target.
     *
     * @return the delegator (usually a big switch statement)
     */
    private synchronized MethodHandle makeDelegator(MethodHandles.Lookup lookup) {
        MethodMaker mm = MethodMaker.begin(lookup, "switch", type());

        if (mSize == 0) {
            makeDefault(mm);
        } else {
            MethodType mt = type().dropParameterTypes(0, 1);

            var remainingParams = new Object[mt.parameterCount()];
            for (int i=0; i<remainingParams.length; i++) {
                remainingParams[i] = mm.param(i + 1);
            }

            var keyVar = mm.param(0);

            if (mSize >= MAX_CASES) {
                // The switch statement is getting big, and rebuilding it each time gets more
                // expensive. Generate a final delegator which accesses the hashtable.
                var dcsVar = mm.var(SwitchCallSite.class).setExact(this);
                var caseVar = dcsVar.invoke("findCase", keyVar);
                Label found = mm.label();
                caseVar.ifNe(null, found);
                caseVar.set(dcsVar.invoke("newCaseDirect", keyVar));
                found.here();
                mm.return_(caseVar.invoke(mt.returnType(), "invokeExact", null, remainingParams));
                var mh = mm.finish();
                setTarget(mh);
                return mh;
            }

            var cases = new int[mSize];
            var labels = new Label[cases.length];
            var defLabel = mm.label();

            int num = 0;
            for (Entry e : mEntries) {
                while (e != null) {
                    cases[num] = e.key;
                    labels[num++] = mm.label();
                    e = e.next;
                }
            }

            keyVar.switch_(defLabel, cases, labels);

            for (int i=0; i<cases.length; i++) {
                labels[i].here();
                var result = mm.invoke(findCase(cases[i]), remainingParams);
                if (result == null) {
                    mm.return_();
                } else {
                    mm.return_(result);
                }
            }

            defLabel.here();

            // The default case is handled by a separate method, which is almost never
            // executed. This helps with inlining by keeping the core switch code small.

            MethodMaker defMaker = mm.classMaker().addMethod("default", type());
            defMaker.static_().private_();
            makeDefault(defMaker);

            var allParams = new Object[type().parameterCount()];
            for (int i=0; i<allParams.length; i++) {
                allParams[i] = mm.param(i);
            }

            var result = mm.invoke("default", allParams);

            if (result == null) {
                mm.return_();
            } else {
                mm.return_(result);
            }
        }

        var mh = mm.finish();
        setTarget(mh);
        return mh;
    }

    /**
     * @param mm first param must be the key
     */
    private void makeDefault(MethodMaker mm) {
        var scsVar = mm.var(SwitchCallSite.class).setExact(this);
        var lookupVar = mm.var(MethodHandles.class).invoke("lookup");
        var newCaseVar = scsVar.invoke("newCase", lookupVar, mm.param(0));

        var allParams = new Object[type().parameterCount()];
        for (int i=0; i<allParams.length; i++) {
            allParams[i] = mm.param(i);
        }

        var result = newCaseVar.invoke(type().returnType(), "invokeExact", null, allParams);

        if (result == null) {
            mm.return_();
        } else {
            mm.return_(result);
        }
    }

    /**
     * Is called by generated code.
     *
     * @return the delegator (usually a big switch statement)
     */
    public synchronized MethodHandle newCase(MethodHandles.Lookup lookup, int key) {
        if (mEntries != null && findCase(key) != null) {
            return getTarget();
        } else {
            CallSite cs = ExceptionCallSite.make(() -> mGenerator.apply(key));
            putCase(key, cs.dynamicInvoker());
            return makeDelegator(lookup);
        }
    }

    /**
     * Is called by generated code.
     *
     * @return the case itself
     */
    public synchronized MethodHandle newCaseDirect(int key) {
        MethodHandle caseHandle = findCase(key);
        if (caseHandle == null) {
            CallSite cs = ExceptionCallSite.make(() -> mGenerator.apply(key));
            caseHandle = cs.dynamicInvoker();
            putCase(key, caseHandle);
        }
        return caseHandle;
    }

    /** 
     * Is called by the generated delegator when the switch isn't used anymore. Note that the
     * call isn't synchronized. If the case isn't found due to a race condition, the delegator
     * calls newCaseDirect, which is synchronized and does a double check first.
     */
    public MethodHandle findCase(int key) {
        Entry[] entries = mEntries;
        if (entries != null) {
            for (Entry e = entries[key & (entries.length - 1)]; e != null; e = e.next) {
                if (e.key == key) {
                    return e.mh;
                }
            }
        }
        return null;
    }

    /**
     * Returns a case handle for the given key, generating it if necessary.
     */
    public MethodHandle getCase(MethodHandles.Lookup lookup, int key) {
        MethodHandle mh = findCase(key);

        if (mh == null) {
            synchronized (this) {
                if (mSize < MAX_CASES) {
                    // Could call newCase all the time, but then it re-generates the "final"
                    // delegator which accesses the hashtable. Harmless, but inefficient.
                    newCase(lookup, key);
                    mh = findCase(key);
                } else {
                    mh = newCaseDirect(key);
                }
            }
        }

        return mh;
    }

    /**
     * Caller must be certain that a matching entry doesn't already exist.
     */
    private void putCase(int key, MethodHandle mh) {
        Entry[] entries = mEntries;
        if (entries == null) {
            mEntries = entries = new Entry[4]; // must be power of 2 size
        } else if (mSize >= entries.length) {
            // rehash
            Entry[] newEntries = new Entry[entries.length << 1];
            for (int i=entries.length; --i>=0 ;) {
                for (Entry e = entries[i]; e != null; ) {
                    Entry next = e.next;
                    int index = e.key & (newEntries.length - 1);
                    e.next = newEntries[index];
                    newEntries[index] = e;
                    e = next;
                }
            }
            mEntries = entries = newEntries;
        }

        int index = key & (entries.length - 1);
        Entry e = new Entry(key, mh);
        e.next = entries[index];
        VarHandle.storeStoreFence(); // reduce likelihood of observing a broken chain
        entries[index] = e;
        mSize++;
    }

    private static class Entry {
        final int key;
        final MethodHandle mh;
        Entry next;

        Entry(int key, MethodHandle mh) {
            this.key = key;
            this.mh = mh;
        }
    }
}
