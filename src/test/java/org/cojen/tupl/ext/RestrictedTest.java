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

package org.cojen.tupl.ext;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;

import java.lang.reflect.InvocationTargetException;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;

import org.cojen.tupl.io.Utils;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class RestrictedTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RestrictedTest.class.getName());
    }

    @Test
    public void accessChecks() throws Exception {
        accessChecks(false);
        accessChecks(true);
    }

    private void accessChecks(boolean hidden) throws Exception {
        // Test with a generated class in a module which doesn't have native access.

        MethodHandles.Lookup lookup = org.cojen.tupl.io.RestrictedTest.newModule
            ("org.cojen.tupl.ext.test", getClass(), "org.cojen.tupl.ext");

        ClassMaker cm = ClassMaker.begin(null, lookup).public_();

        MethodMaker mm = cm.addMethod(null, "CipherCrypto_1").public_().static_();
        mm.var(CipherCrypto.class).invoke("factory", mm.new_(byte[].class, 16)).invoke("get");

        mm = cm.addMethod(null, "CipherCrypto_2").public_().static_();
        mm.var(CipherCrypto.class).invoke("factory", null, 16).invoke("get");

        mm = cm.addMethod(null, "CipherCrypto_3").public_().static_();
        mm.new_(CipherCrypto.class, 16);

        mm = cm.addMethod(null, "CipherCrypto_4").public_().static_();
        mm.new_(CipherCrypto.class, mm.new_(byte[].class, 16));

        mm = cm.addMethod(null, "CipherCrypto_5").public_().static_();
        mm.new_(CipherCrypto.class, null, 16);

        mm = cm.addMethod(null, "CipherCrypto_6").public_().static_();
        MethodType mt = MethodType.methodType(void.class, int.class);
        mm.var(MethodHandles.class).invoke("lookup")
            .invoke("findConstructor", CipherCrypto.class, mt)
            .invoke("invoke", 16);

        mm.new_(CipherCrypto.class, null, 16);

        Class<?> clazz;
        if (!hidden) {
            clazz = cm.finish();
        } else {
            clazz = cm.finishHidden().lookupClass();
        }

        invokeAndVerify(clazz, "CipherCrypto_1");
        invokeAndVerify(clazz, "CipherCrypto_2");
        invokeAndVerify(clazz, "CipherCrypto_3");
        invokeAndVerify(clazz, "CipherCrypto_4");
        invokeAndVerify(clazz, "CipherCrypto_5");
        invokeAndVerify(clazz, "CipherCrypto_6");
    }

    private static void invokeAndVerify(Class<?> clazz, String name) throws Exception {
        try {
            clazz.getMethod(name).invoke(null);
            fail();
        } catch (InvocationTargetException e) {
            verify(e);
        }
    }

    private static void verify(InvocationTargetException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IllegalCallerException) {
            assertEquals("Native access isn't enabled for module org.cojen.tupl.ext.test",
                         cause.getMessage());
        } else {
            throw Utils.rethrow(cause);
        }
    }
}
