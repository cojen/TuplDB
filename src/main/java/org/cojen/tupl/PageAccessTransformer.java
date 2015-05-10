/*
 *  Copyright 2015 Brian S O'Neill
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

/**
 * 
 *
 * @author Brian S O'Neill
 */
class PageAccessTransformer {
    public static void main(String[] args) throws Exception {
        File src = new File(args[0]);
        File dst = new File(args[1]);
        transform(src, dst);
    }

    private static void transform(File src, File dst) throws IOException {
        if (src.isDirectory()) {
            String[] files = src.list();
            if (files != null) {
                for (String f : files) {
                    transform(new File(src, f), new File(dst, f));
                }
            }
            return;
        }

        if (!src.isFile() || !src.getName().endsWith(".java")) {
            return;
        }

        boolean skip = src.getName().equals("PageAccessTransformer.java")
            || src.getName().endsWith("PageOps.java");

        try (BufferedReader in = new BufferedReader(new FileReader(src))) {
            dst.getParentFile().mkdirs();
            try (BufferedWriter out = new BufferedWriter(new FileWriter(dst))) {
                String line;
                while ((line = in.readLine()) != null) {
                    if (!skip) {
                        line = transform(line);
                    }
                    out.write(line);
                    out.newLine();
                }
            }
        }
    }

    private static String transform(String line) {
        while (true) {
            int index = line.indexOf("/*P*/ byte[]");
            if (index >= 0) {
                line = line.substring(0, index + 6) + "long" + line.substring(index + 12);
                continue;
            }

            index = line.indexOf("PageOps");
            if (index >= 0) {
                line = line.substring(0, index) + "Direct" + line.substring(index);
            }

            return line;
        }
    }
}
