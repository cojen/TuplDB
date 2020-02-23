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

package org.cojen.tupl.tools;

import java.io.*;
import java.util.*;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utility which generates files that access pages directly (unsafe). A direct page is native
 * memory not managed by the JVM. The utility non-recursively scans all java source files in a
 * directory, looking for the magic *P* comment which indicates a page access operation.
 *
 * <p>The basic transformation which is applied converts all "byte[]" page references to "long"
 * references. The long is used as raw memory pointer type. Another transformation pattern
 * allows for simple #ifdef style preprocessing behavior.
 *
 * @author Brian S O'Neill
 * @hidden
 */
public class PageAccessTransformer {
    /**
     * @param args [0]: source directory, [1]: destination directory
     */
    public static void main(String[] args) throws Exception {
        var src = new File(args[0]);
        var dst = new File(args[1]);
        var pa = new PageAccessTransformer(src, dst);
        pa.findFiles();
        pa.transform();
    }

    private static final int STATE_NORMAL = 0, STATE_DISABLE = 1, STATE_ENABLE = 2;

    private int mState;

    private final Pattern mDisablePattern = Pattern.compile("\\s+");

    private final File mSrc;
    private final File mDst;

    private Map<String, Pattern> mNames;

    public PageAccessTransformer(File src, File dst) {
        dirCheck(src);
        dirCheck(dst);
        mSrc = src;
        mDst = dst;
    }

    private static void dirCheck(File dir) {
        if (dir.exists() && !dir.isDirectory()) {
            throw new IllegalArgumentException("Not a directory: " + dir);
        }
    }

    public void findFiles() throws IOException {
        var names = new HashMap<String, Pattern>();

        File[] files = mSrc.listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.isFile() && requiresTransform(f)) {
                    String name = f.getName();
                    name = name.substring(0, name.length() - 5);
                    names.put(name, Pattern.compile("\\b" + name + "\\b"));
                }
            }
        }

        mNames = names;
    }

    /**
     * @return set of generated file names
     */
    public Collection<String> transform() throws IOException {
        var all = new ArrayList<String>(mNames.size());

        for (String name : mNames.keySet()) {
            String newName = "_" + name + ".java";
            transform(new File(mSrc, name + ".java"), new File(mDst, newName));
            all.add(newName);
        }

        return all;
    }

    private void transform(File src, File dst) throws IOException {
        try (var in = new BufferedReader(new FileReader(src))) {
            dst.getParentFile().mkdirs();
            try (var out = new BufferedWriter(new FileWriter(dst))) {
                mState = STATE_NORMAL;
                String line;
                while ((line = in.readLine()) != null) {
                    int index = line.indexOf("@author ");
                    if (index > 0) {
                        line = line.substring(0, index + 8) +
                            "Generated by PageAccessTransformer from " + src.getName();
                    } else {
                        line = transform(line);
                    }

                    if (line != null) {
                        out.write(line);
                        out.write('\n');
                    }
                }
            }
        }
    }

    private String transform(String line) {
        line = replaceNames(line);

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
                    if (index >= 0
                        && (index < 6 || !line.regionMatches(index - 6, "Direct", 0, 6)))
                    {
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

    private String replaceNames(String line) {
        for (Map.Entry<String, Pattern> e : mNames.entrySet()) {
            String line2 = e.getValue().matcher(line).replaceAll("_" + e.getKey());
            if (!line2.equals(line) && line2.indexOf("\"_") <= 0) {
                line = line2;
            }
        }
        return line;
    }

    private static boolean requiresTransform(File file) throws IOException {
        String name = file.getName();

        if (!name.endsWith(".java") ||
            name.equals("PageAccessTransformer.java") ||
            name.startsWith("_") ||
            name.endsWith("PageOps.java"))
        {
            return false;
        }

        try (var in = new BufferedReader(new FileReader(file))) {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.indexOf("/*P*/") >= 0) {
                    return true;
                }
            }
        }

        return false;
    }
}