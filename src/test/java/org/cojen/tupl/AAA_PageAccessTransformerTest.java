/*
 *  Copyright 2016 Cojen.org
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
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
