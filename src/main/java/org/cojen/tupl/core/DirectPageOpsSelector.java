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

package org.cojen.tupl.core;

import java.lang.invoke.MethodHandles;

import java.nio.ByteOrder;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.Label;
import org.cojen.maker.MethodMaker;
import org.cojen.maker.Variable;

/**
 * Attempts to replace the DirectPageOps class before it's used for the first time.
 *
 * @author Brian S. O'Neill
 */
final class DirectPageOpsSelector {
    /**
     * Call this method before loading DirectPageOps to select a faster replacement. Returns 0
     * if the default ffm variant is used, or 1 if the internal unsafe class is used, or else 2
     * if the sun unsafe class is used.
     */
    static int kind() {
        return DirectPageOps.kind();
    }

    static String kindStr() {
        int kind = kind();
        if (kind == 2) {
            return "sun";
        } else if (kind == 1) {
            return "internal";
        } else {
            return "ffm";
        }
    }

    static volatile Object result;

    static {
        try {
            select();
        } catch (Throwable e) {
            result = e;
        }
    }

    private static void select() throws Throwable {
        // Try to override the default implementation.

        String prop = System.getProperty(DirectPageOpsSelector.class.getName());

        if ("ffm".equals(prop)) {
            // Use the existing DirectPageOps class.
            return;
        }

        Object unsafe;
        int kind;

        select: {
            Throwable ex = null;

            if (prop == null || prop.equals("internal")) {
                /*
                  Need to export the package to be available.

                  --add-exports java.base/jdk.internal.misc=org.cojen.tupl

                  or

                  --add-exports java.base/jdk.internal.misc=ALL-UNNAMED
                */

                try {
                    unsafe = Class.forName("jdk.internal.misc.Unsafe")
                        .getMethod("getUnsafe").invoke(null);
                    kind = 1;
                    break select;
                } catch (Throwable e) {
                    ex = combine(ex, e);
                    if (prop != null) {
                        throw ex;
                    }
                }
            }

            if (prop == null || prop.equals("sun")) {
                try {
                    var theUnsafe = Class.forName("sun.misc.Unsafe").getDeclaredField("theUnsafe");
                    theUnsafe.setAccessible(true);
                    unsafe = theUnsafe.get(null);
                    kind = 2;
                    break select;
                } catch (Throwable e) {
                    ex = combine(ex, e);
                    if (prop != null) {
                        throw ex;
                    }
                }
            }

            // Use the existing DirectPageOps class.

            if (ex != null) {
                throw ex;
            }

            return;
        }

        DirectPageOpsSelector.class.getModule().addReads(unsafe.getClass().getModule());

        result = unsafe;

        ClassMaker cm = ClassMaker.beginExplicit
            ("org.cojen.tupl.core.DirectPageOps", MethodHandles.lookup());

        cm.addField(unsafe.getClass(), "U").private_().static_().final_();
        cm.addField(long.class, "O").private_().static_().final_();

        MethodMaker mm = cm.addClinit();
        var unsafeVar = mm.var(DirectPageOpsSelector.class)
            .condy("U").invoke(Object.class, "U").cast(unsafe.getClass());
        mm.field("U").set(unsafeVar);
        mm.field("O").set(unsafeVar.invoke("arrayBaseOffset", byte[].class));

        mm = cm.addMethod(int.class, "kind").static_();
        mm.return_(kind);

        mm = cm.addMethod(byte.class, "p_byteGet", long.class, int.class).static_();
        mm.return_(mm.field("U").invoke("getByte", mm.param(0).add(mm.param(1))));

        mm = cm.addMethod(void.class, "p_bytePut", long.class, int.class, byte.class).static_();
        mm.field("U").invoke("putByte", mm.param(0).add(mm.param(1)), mm.param(2));

        ByteOrder bo = ByteOrder.nativeOrder();

        mm = cm.addMethod(int.class, "p_ushortGetLE", long.class, int.class).static_();
        mm.return_(le(mm.field("U").invoke("getChar", mm.param(0).add(mm.param(1))), bo));

        mm = cm.addMethod(void.class, "p_shortPutLE", long.class, int.class, int.class).static_();
        mm.field("U").invoke("putShort", mm.param(0).add(mm.param(1)),
                             le(mm.param(2).cast(short.class), bo));

        mm = cm.addMethod(int.class, "p_intGetLE", long.class, int.class).static_();
        mm.return_(le(mm.field("U").invoke("getInt", mm.param(0).add(mm.param(1))), bo));

        mm = cm.addMethod(void.class, "p_intPutLE", long.class, int.class, int.class).static_();
        mm.field("U").invoke("putInt", mm.param(0).add(mm.param(1)), le(mm.param(2), bo));

        mm = cm.addMethod(long.class, "p_longGetLE", long.class, int.class).static_();
        mm.return_(le(mm.field("U").invoke("getLong", mm.param(0).add(mm.param(1))), bo));

        mm = cm.addMethod(void.class, "p_longPutLE", long.class, int.class, long.class).static_();
        mm.field("U").invoke("putLong", mm.param(0).add(mm.param(1)), le(mm.param(2), bo));

        mm = cm.addMethod(long.class, "p_longGetBE", long.class, int.class).static_();
        mm.return_(be(mm.field("U").invoke("getLong", mm.param(0).add(mm.param(1))), bo));

        mm = cm.addMethod(void.class, "p_longPutBE", long.class, int.class, long.class).static_();
        mm.field("U").invoke("putLong", mm.param(0).add(mm.param(1)), be(mm.param(2), bo));

        mm = cm.addMethod(void.class, "p_clear", long.class, int.class, int.class).static_();
        var pageVar = mm.param(0);
        var fromIndexVar = mm.param(1);
        var toIndexVar = mm.param(2);
        var lenVar = toIndexVar.sub(fromIndexVar);
        Label done = mm.label();
        lenVar.ifLe(0, done);
        mm.field("U").invoke("setMemory", pageVar.add(fromIndexVar), lenVar, (byte) 0);
        done.here();

        mm = cm.addMethod(void.class, "p_copy",
                          byte[].class, int.class, long.class, long.class, int.class).static_();
        mm.field("U").invoke("copyMemory", mm.param(0), mm.field("O").add(mm.param(1)),
                             null, mm.param(2).add(mm.param(3)), mm.param(4));
                      
        mm = cm.addMethod(void.class, "p_copy",
                          long.class, long.class, byte[].class, int.class, int.class).static_();
        mm.field("U").invoke("copyMemory", null, mm.param(0).add(mm.param(1)),
                             mm.param(2), mm.field("O").add(mm.param(3)), mm.param(4));

        mm = cm.addMethod(void.class, "p_copy",
                          long.class, int.class, long.class, long.class, long.class).static_();
        mm.field("U").invoke("copyMemory", mm.param(0).add(mm.param(1)),
                             mm.param(2).add(mm.param(3)), mm.param(4));

        cm.finish();
    }

    private static Variable le(Variable v, ByteOrder bo) {
        if (bo != ByteOrder.LITTLE_ENDIAN) {
            v = v.invoke("reverseBytes", v);
        }
        return v;
    }

    private static Variable be(Variable v, ByteOrder bo) {
        if (bo == ByteOrder.LITTLE_ENDIAN) {
            v = v.invoke("reverseBytes", v);
        }
        return v;
    }

    private static Throwable combine(Throwable existing, Throwable additional) {
        if (existing == null) {
            return additional;
        }
        existing.addSuppressed(additional);
        return existing;
    }

    // Condy method which is called by the generated code.
    static Object U(MethodHandles.Lookup caller, String name, Class type) {
        Object unsafe = result;
        result = null;
        return unsafe;
    }
}
