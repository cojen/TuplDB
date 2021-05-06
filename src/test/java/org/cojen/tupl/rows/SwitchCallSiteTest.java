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

package org.cojen.tupl.rows;

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

    private static SwitchCallSite makeNoParams() {
        IntFunction<Object> generator = version -> {
            var mm = MethodMaker.begin(MethodHandles.lookup(), String.class, "_");
            mm.return_("version " + version);
            return mm.finish();
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

    private static SwitchCallSite makeOneParam() {
        IntFunction<Object> generator = version -> {
            var mm = MethodMaker.begin(MethodHandles.lookup(), String.class, "_", String.class);
            mm.return_(mm.concat("version ", version, ":",  mm.param(0)));
            return mm.finish();
        };

        return new SwitchCallSite(MethodHandles.lookup(),
                                  MethodType.methodType(String.class, int.class, String.class),
                                  generator);
    }
}
