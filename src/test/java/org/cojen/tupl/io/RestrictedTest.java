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

package org.cojen.tupl.io;

import java.io.File;

import java.lang.invoke.MethodHandles;

import java.lang.reflect.InvocationTargetException;

import java.net.URI;

import java.nio.ByteBuffer;

import java.util.Optional;
import java.util.Set;

import java.util.stream.Stream;

import java.lang.module.Configuration;
import java.lang.module.ModuleDescriptor;
import java.lang.module.ModuleFinder;
import java.lang.module.ModuleReader;
import java.lang.module.ModuleReference;

import org.junit.*;
import static org.junit.Assert.*;

import org.cojen.maker.ClassMaker;
import org.cojen.maker.MethodMaker;

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

        MethodHandles.Lookup lookup = newModule
            ("org.cojen.tupl.io.test", getClass(), "org.cojen.tupl.io");

        ClassMaker cm = ClassMaker.begin(null, lookup).public_();

        MethodMaker mm = cm.addMethod(null, "FileIO_1").public_().static_();
        mm.var(FileIO.class).invoke("open", mm.new_(File.class, "x"), null);

        mm = cm.addMethod(null, "FileIO_2").public_().static_();
        mm.var(FileIO.class).invoke("open", mm.new_(File.class, "x"), null, 1);

        mm = cm.addMethod(null, "FilePageArray").public_().static_();
        mm.var(FilePageArray.class)
            .invoke("factory", 4096, mm.new_(File.class, "x"), null)
            .invoke("get");

        mm = cm.addMethod(null, "MappedPageArray_1").public_().static_();
        mm.var(MappedPageArray.class)
            .invoke("factory", 4096, 100, mm.new_(File.class, "x"), null)
            .invoke("get");

        mm = cm.addMethod(null, "MappedPageArray_2").public_().static_();
        mm.var(MappedPageArray.class)
            .invoke("factory", 4096, 100, mm.new_(File.class, "x"), null, null)
            .invoke("get");

        mm = cm.addMethod(null, "StripedPageArray").public_().static_();
        var fileVar = mm.new_(File.class, "x");
        mm.var(StripedPageArray.class)
            .invoke("factory",
                    mm.var(FilePageArray.class).invoke("factory", 4096, fileVar, null),
                    mm.var(FilePageArray.class).invoke("factory", 4096, fileVar, null))
            .invoke("get");

        mm = cm.addMethod(null, "SpilloverPageArray").public_().static_();
        fileVar = mm.new_(File.class, "x");
        mm.var(SpilloverPageArray.class)
            .invoke("factory",
                    mm.var(FilePageArray.class).invoke("factory", 4096, fileVar, null),
                    4096,
                    mm.var(FilePageArray.class).invoke("factory", 4096, fileVar, null))
            .invoke("get");

        mm = cm.addMethod(null, "zlib_1").public_().static_();
        mm.var(PageCompressor.class).invoke("zlib").invoke("get");

        mm = cm.addMethod(null, "zlib_2").public_().static_();
        mm.var(PageCompressor.class).invoke("zlib", 5).invoke("get");

        mm = cm.addMethod(null, "lz4").public_().static_();
        mm.var(PageCompressor.class).invoke("lz4").invoke("get");

        Class<?> clazz;
        if (!hidden) {
            clazz = cm.finish();
        } else {
            clazz = cm.finishHidden().lookupClass();
        }

        invokeAndVerify(clazz, "FileIO_1");
        invokeAndVerify(clazz, "FileIO_2");
        invokeAndVerify(clazz, "FilePageArray");
        invokeAndVerify(clazz, "MappedPageArray_1");
        invokeAndVerify(clazz, "MappedPageArray_2");
        invokeAndVerify(clazz, "StripedPageArray");
        invokeAndVerify(clazz, "SpilloverPageArray");
        invokeAndVerify(clazz, "zlib_1");
        invokeAndVerify(clazz, "zlib_2");
        invokeAndVerify(clazz, "lz4");
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
            assertEquals("Native access isn't enabled for module org.cojen.tupl.io.test",
                         cause.getMessage());
        } else {
            throw Utils.rethrow(cause);
        }
    }

    public static MethodHandles.Lookup newModule(String moduleName,
                                                 Class<?> exporter, String... exportedPackages)
        throws Exception
    {
        // There's got to be an easier way to do this.

        String pkgName = moduleName;
        String className = pkgName + ".Bootstrap";

        byte[] classBytes;
        {
            ClassMaker cm = ClassMaker.beginExternal(className).public_();
            MethodMaker mm = cm.addMethod(MethodHandles.Lookup.class, "boot", Module.class)
                .public_().static_();
            mm.class_().invoke("getModule").invoke("addReads", mm.param(0));
            mm.return_(mm.var(MethodHandles.class).invoke("lookup"));
            classBytes = cm.finishBytes();
        }

        ModuleDescriptor desc = ModuleDescriptor.newModule(moduleName).exports(pkgName).build();

        var finder = new ModuleFinder() {
            @Override
            public Optional<ModuleReference> find(String name) {
                if (!name.equals(moduleName)) {
                    return Optional.empty();
                }

                return Optional.of(new ModuleReference(desc, null) {
                    @Override
                    public ModuleReader open() {
                        return new ModuleReader() {
                            @Override
                            public Optional<ByteBuffer> read(String name) {
                                if (name.equals(className.replace('.', '/') + ".class")) {
                                    return Optional.of(ByteBuffer.wrap(classBytes));
                                }
                                return Optional.empty();
                            }

                            @Override
                            public void close() {
                            }

                            public Optional<URI> find(String name) {
                                return Optional.empty();
                            }

                            public Stream<String> list() {
                                return Stream.empty();
                            }
                        };
                    }
                });
            }

            @Override
            public Set<ModuleReference> findAll() {
                return Set.of(find(moduleName).get());
            }
        };

        ModuleLayer boot = ModuleLayer.boot();
        Configuration config = boot.configuration()
            .resolve(finder, ModuleFinder.of(), Set.of(moduleName));
        ModuleLayer layer = boot.defineModulesWithOneLoader(config, exporter.getClassLoader());

        Class<?> bootClass = layer.findLoader(moduleName).loadClass(className);

        Module exporterModule = exporter.getModule();

        var lookup = (MethodHandles.Lookup) bootClass
            .getMethod("boot", Module.class).invoke(null, exporterModule);

        Module newModule = bootClass.getModule();

        for (String exported : exportedPackages) {
            exporterModule.addExports(exported, newModule);
        }

        return lookup;
    }
}
