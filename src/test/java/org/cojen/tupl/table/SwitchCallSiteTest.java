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

import java.lang.invoke.*;

import java.util.function.IntFunction;

import org.cojen.maker.*;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class SwitchCallSiteTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(SwitchCallSiteTest.class.getName());
    }

    public static boolean cFaulty;

    @Test
    public void noParams() throws Throwable {
        MethodHandle mh = makeNoParams().dynamicInvoker();

        for (int t=0; t<2; t++) {
            for (int i=0; i<1000; i++) {
                String str = (String) mh.invokeExact(i);
                assertEquals("version " + i, str);
            }
        }
    }

    @Test
    public void noParamsFaulty() throws Throwable {
        // Tests ExceptionCallSite.

        MethodHandle mh = makeNoParams().dynamicInvoker();

        int faults = 0;

        cFaulty = true;
        try {
            for (int t=0; t<2; t++) {
                for (int i=0; i<1000; i++) {
                    try {
                        String str = (String) mh.invokeExact(i);
                        assertEquals("version " + i, str);
                    } catch (Exception e) {
                        assertEquals("faulty-" + i, e.getMessage());
                        assertTrue((i & 1) != 0);
                        faults++;
                    }
                }
            }
        } finally {
            cFaulty = false;
        }

        assertEquals(1000, faults);

        for (int t=0; t<2; t++) {
            for (int i=0; i<1000; i++) {
                String str = (String) mh.invokeExact(i);
                assertEquals("version " + i, str);
            }
        }
    }

    @Test
    public void noParamsDirect() throws Throwable {
        MethodHandles.Lookup lookup = MethodHandles.lookup();
        SwitchCallSite scs = makeNoParams();

        for (int i=0; i<1000; i++) {
            String str = (String) scs.getCase(lookup, i).invokeExact();
            assertEquals("version " + i, str);
        }

        // Verify that it still works when invoked the "normal" way.

        MethodHandle mh = scs.dynamicInvoker();

        for (int i=0; i<1000; i++) {
            String str = (String) mh.invokeExact(i);
            assertEquals("version " + i, str);
        }
    }

    @Test
    public void noParamsDirectInterleaved() throws Throwable {
        // Call the direct and "normal" way together in the same loop.

        MethodHandles.Lookup lookup = MethodHandles.lookup();
        SwitchCallSite scs = makeNoParams();
        MethodHandle mh = scs.dynamicInvoker();

        for (int i=0; i<1000; i++) {
            String str = (String) scs.getCase(lookup, i).invokeExact();
            assertEquals("version " + i, str);
            str = (String) mh.invokeExact(i);
            assertEquals("version " + i, str);
        }
    }

    @Test
    public void noParamsDirectInterleaved2() throws Throwable {
        // Same as noParamsDirectInterleaved except the invocation order is swapped.

        MethodHandles.Lookup lookup = MethodHandles.lookup();
        SwitchCallSite scs = makeNoParams();
        MethodHandle mh = scs.dynamicInvoker();

        for (int i=0; i<1000; i++) {
            String str = (String) mh.invokeExact(i);
            assertEquals("version " + i, str);
            str = (String) scs.getCase(lookup, i).invokeExact();
            assertEquals("version " + i, str);
        }
    }

    private static SwitchCallSite makeNoParams() {
        IntFunction<Object> generator = version -> {
            var mm = MethodMaker.begin(MethodHandles.lookup(), String.class, "_");
            if (!cFaulty || (version & 1) == 0) {
                mm.return_("version " + version);
                return mm.finish();
            } else {
                var mt = MethodType.methodType(String.class);
                return new ExceptionCallSite.Failed(mt, mm, new Exception("faulty-" + version));
            }
        };

        return new SwitchCallSite(MethodHandles.lookup(),
                                  MethodType.methodType(String.class, int.class), generator);
    }

    @Test
    public void oneParam() throws Throwable {
        MethodHandle mh = makeOneParam().dynamicInvoker();

        for (int t=0; t<2; t++) {
            for (int i=0; i<1000; i++) {
                String hex = Integer.toHexString(i);
                String str = (String) mh.invokeExact(i, hex);
                assertEquals("version " + i + ":" + hex, str);
            }
        }
    }

    @Test
    public void oneParamFaulty() throws Throwable {
        // Tests ExceptionCallSite.

        MethodHandle mh = makeOneParam().dynamicInvoker();

        int faults = 0;

        cFaulty = true;
        try {
            for (int t=0; t<2; t++) {
                for (int i=0; i<1000; i++) {
                    try {
                        String hex = Integer.toHexString(i);
                        String str = (String) mh.invokeExact(i, hex);
                        assertEquals("version " + i + ":" + hex, str);
                    } catch (Exception e) {
                        assertEquals("faulty-" + i, e.getMessage());
                        assertTrue((i & 1) != 0);
                        faults++;
                    }
                }
            }
        } finally {
            cFaulty = false;
        }

        assertEquals(1000, faults);

        for (int t=0; t<2; t++) {
            for (int i=0; i<1000; i++) {
                String hex = Integer.toHexString(i);
                String str = (String) mh.invokeExact(i, hex);
                assertEquals("version " + i + ":" + hex, str);
            }
        }
    }

    private static SwitchCallSite makeOneParam() {
        IntFunction<Object> generator = version -> {
            var mm = MethodMaker.begin(MethodHandles.lookup(), String.class, "_", String.class);
            if (!cFaulty || (version & 1) == 0) {
                mm.return_(mm.concat("version ", version, ":",  mm.param(0)));
                return mm.finish();
            } else {
                var mt = MethodType.methodType(String.class, String.class);
                return new ExceptionCallSite.Failed(mt, mm, new Exception("faulty-" + version));
            }
        };

        return new SwitchCallSite(MethodHandles.lookup(),
                                  MethodType.methodType(String.class, int.class, String.class),
                                  generator);
    }
}
