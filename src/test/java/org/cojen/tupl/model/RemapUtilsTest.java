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

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
public class RemapUtilsTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(RemapUtilsTest.class.getName());
    }

    @Test
    public void checkAnd() {
        /*
           left  right  outcome
           --------------------
            f      f    f
            f      t    f
            f      x    f (suppress)

            t      f    f
            t      t    t
            t      x    x

            x      f    f (suppress)
            x      t    x
            x      x    x
        */

        var f = Boolean.FALSE;
        var t = Boolean.TRUE;
        var x = new RuntimeException();

        assertEquals(f, RemapUtils.checkAnd(f, f));
        assertEquals(f, RemapUtils.checkAnd(f, t));
        assertEquals(f, RemapUtils.checkAnd(f, x));

        assertEquals(f, RemapUtils.checkAnd(t, f));
        assertEquals(t, RemapUtils.checkAnd(t, t));
        try {
            RemapUtils.checkAnd(t, x);
            fail();
        } catch (RuntimeException ex) {
            assertSame(x, ex);
        }

        assertEquals(f, RemapUtils.checkAnd(x, f));
        try {
            RemapUtils.checkAnd(x, t);
            fail();
        } catch (RuntimeException ex) {
            assertSame(x, ex);
        }
        try {
            RemapUtils.checkAnd(x, x);
        } catch (RuntimeException ex) {
            assertSame(x, ex);
        }
    }

    @Test
    public void checkOr() {
        /*
           left  right  outcome
           --------------------
            f      f    f
            f      t    t
            f      x    x

            t      f    t
            t      t    t
            t      x    t (suppress)

            x      f    x
            x      t    t (suppress)
            x      x    x
        */

        var f = Boolean.FALSE;
        var t = Boolean.TRUE;
        var x = new RuntimeException();

        assertEquals(f, RemapUtils.checkOr(f, f));
        assertEquals(t, RemapUtils.checkOr(f, t));
        try {
            RemapUtils.checkOr(f, x);
            fail();
        } catch (RuntimeException ex) {
            assertSame(x, ex);
        }

        assertEquals(t, RemapUtils.checkOr(t, f));
        assertEquals(t, RemapUtils.checkOr(t, t));
        assertEquals(t, RemapUtils.checkOr(t, x));

        try {
            RemapUtils.checkOr(x, f);
            fail();
        } catch (RuntimeException ex) {
            assertSame(x, ex);
        }
        assertEquals(t, RemapUtils.checkOr(x, t));
        try {
            RemapUtils.checkOr(x, x);
            fail();
        } catch (RuntimeException ex) {
            assertSame(x, ex);
        }
    }
}
