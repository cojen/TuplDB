/*
 *  Copyright (C) 2011-2017 Cojen.org
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

package org.cojen.tupl;

import java.io.*;
import java.util.*;

import org.junit.*;
import static org.junit.Assert.*;

/**
 * 
 *
 * @author Brian S O'Neill
 */
public class AAA_PageAccessTransformerTest {
    public static void main(String[] args) throws Exception {
        org.junit.runner.JUnitCore.main(AAA_PageAccessTransformerTest.class.getName());
    }

    @Test
    public void generateAndCompare() throws Exception {
        File src = TestUtils.findSourceDirectory();

        File dst = new File(System.getProperty("java.io.tmpdir"), "tupl");
        dst = new File(dst, "generated");

        TestUtils.deleteRecursively(dst);

        PageAccessTransformer pa = new PageAccessTransformer(src, dst);
        pa.findFiles();
        Collection<String> generated = pa.transform();

        for (String name : generated) {
            if (!sourceMatches(new File(src, name), new File(dst, name))) {
                fail("Generated code doesn't match: " + name +
                     ". Run PageAccessTransformer or copy the files from " + dst + " to " + src);
            }
        }

        TestUtils.deleteRecursively(dst);
    }

    private static boolean sourceMatches(File a, File b) throws IOException {
        // Compare lines and ignore CR/LF differences.

        try {
            try (BufferedReader ar = new BufferedReader(new FileReader(a))) {
                try (BufferedReader br = new BufferedReader(new FileReader(b))) {
                    while (true) {
                        String aline = ar.readLine();
                        String bline = br.readLine();
                        if (!Objects.equals(aline, bline)) {
                            return false;
                        }
                        if (aline == null) {
                            break;
                        }
                    }
                }
            }
        } catch (FileNotFoundException e) {
            return false;
        }

        return true;
    }
}
