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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 
 *
 * @author Brian S O'Neill
 */
class PageAccessTransformer {
    public static void main(String[] args) throws Exception {
        File src = new File(args[0]);
        File dst = new File(args[1]);
        new PageAccessTransformer().transform(src, dst);
    }

    private static final int STATE_NORMAL = 0, STATE_DISABLE = 1, STATE_ENABLE = 2;

    private int mState;

    private final Pattern mDisablePattern = Pattern.compile("\\s+");

    private void transform(File src, File dst) throws IOException {
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
                mState = STATE_NORMAL;
                String line;
                while ((line = in.readLine()) != null) {
                    if (!skip) {
                        line = transform(line);
                    }
                    if (line != null) {
                        out.write(line);
                        out.newLine();
                    }
                }
            }
        }
    }

    private String transform(String line) {
        while (true) {
            int index = line.indexOf("/*P*/ ");

            if (index < 0) {
                if (mState == STATE_DISABLE) {
                    Matcher m = mDisablePattern.matcher(line);
                    if (m.find()) {
                        line = m.group() + m.replaceFirst("// ");
                    }
                } else {
                    index = line.indexOf("PageOps");
                    if (index >= 0) {
                        line = line.substring(0, index) + "Direct" + line.substring(index);
                    }
                }
                return line;
            }

            int typeIndex = index + 6;

            if (line.indexOf("byte[]", typeIndex) == typeIndex) {
                line = line.substring(0, index) + "long" + line.substring(typeIndex + 6);
                continue;
            }

            if (line.indexOf("// ", typeIndex) == typeIndex) {
                int tagIndex = typeIndex + 3;
                if (tagIndex < line.length()) {
                    switch (line.charAt(tagIndex)) {
                    case '[':
                        if (mState != STATE_NORMAL) {
                            throw new IllegalStateException();
                        }
                        if (++tagIndex < line.length() && line.charAt(tagIndex) == '|') {
                            mState = STATE_ENABLE;
                        } else {
                            mState = STATE_DISABLE;
                        }
                        return line;
                    case '|':
                        if (mState != STATE_DISABLE) {
                            throw new IllegalStateException();
                        }
                        mState = STATE_ENABLE;
                        return line;
                    case ']':
                        if (mState == STATE_NORMAL) {
                            throw new IllegalStateException();
                        }
                        mState = STATE_NORMAL;
                        return line;
                    }
                }

                if (mState == STATE_ENABLE) {
                    line = line.substring(0, index) + line.substring(tagIndex);
                    continue;
                }
            }

            return line;
        }
    }
}
