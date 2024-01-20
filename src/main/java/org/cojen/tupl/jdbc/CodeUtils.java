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

package org.cojen.tupl.jdbc;

import org.cojen.maker.ClassMaker;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class CodeUtils extends org.cojen.tupl.table.CodeUtils {
    private static final Object MAKER_KEY = new Object();

    /**
     * @param who the class which is making a class (can be null)
     * @param peer defines the ClassLoader to use and helps define the new class name
     * @param subPackage optional (can be null)
     * @param suffix appended to the new class name (can be null)
     */
    static ClassMaker beginClassMaker(Class<?> who, Class<?> peer,
                                      String subPackage, String suffix)
    {
        final var bob = new StringBuilder(peer.getPackageName()).append('.');

        if (subPackage != null) {
            bob.append(subPackage).append('.');
        }

        final String name = peer.getSimpleName();

        // If found, prune off the generated number suffix.
        int ix = name.lastIndexOf('-');
        if (ix < 0) {
            bob.append(name);
        } else {
            bob.append(name.substring(0, ix));
        }

        if (suffix != null && !suffix.isEmpty()) {
            bob.append('-').append(suffix);
        }

        final ClassMaker cm = ClassMaker.begin(bob.toString(), peer.getClassLoader(), MAKER_KEY);

        if (who != null) {
            cm.sourceFile(who.getSimpleName());
        }

        var thisModule = CodeUtils.class.getModule();
        var thatModule = cm.classLoader().getUnnamedModule();

        // Generated code needs access to these non-exported packages.
        thisModule.addExports("org.cojen.tupl.jdbc", thatModule);

        return cm;
    }
}
