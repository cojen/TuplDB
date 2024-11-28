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

package org.cojen.tupl.table;

/**
 * Base class for simple parsers.
 *
 * @author Brian S O'Neill
 */
public abstract class SimpleParser {
    /** The text being parsed. */
    protected final String mText;

    /** The position of the next character to examine. */
    protected int mPos;

    public SimpleParser(String text) {
        mText = text;
    }

    /**
     * Returns the character at mPos and increments it by one. If mPos is past the end, then -1
     * is returned, and mPos is still incremented by one.
     */
    protected int nextChar() {
        String text = mText;
        int pos = mPos;
        int c = (pos >= text.length()) ? -1 : text.charAt(pos);
        mPos = pos + 1;
        return c;
    }

    /**
     * Returns the next non-whitespace character, starting at mPos. As a side effect, mPos is
     * set to the next character to examine. If no non-whitespace characters remain, then -1 is
     * returned and mPos is set past the end.
     */
    protected int nextCharIgnoreWhitespace() {
        int c;
        while ((c = nextChar()) >= 0) {
            switch (c) {
            case ' ': case '\r': case '\n': case '\t': case '\0':
                break;
            default:
                if (Character.isWhitespace(c)) {
                    break;
                }
                return c;
            }
        }
        return c;
    }

    /**
     * Skips the character that would be returned by nextCharIgnoreWhitespace and returns the
     * non-whitespace character immediately after it. As a side effect, mPos is set to the
     * position of character which was returned. If no non-whitespace characters remain, then
     * -1 is returned and mPos is set past the end.
     */
    protected int skipCharIgnoreWhitespace() {
        mPos++;
        int c = nextCharIgnoreWhitespace();
        mPos--;
        return c;
    }

    protected void skipWhitespace() {
        nextCharIgnoreWhitespace();
        mPos--;
    }

    protected IllegalArgumentException error(String message) {
        return error(message, mPos);
    }

    /**
     * @param pos start position of text to include in the error message
     */
    protected IllegalArgumentException error(String message, int pos) {
        if (pos <= 0 || mText.isEmpty()) {
            message += " (at start of " + describe() + ')';
        } else if (pos >= mText.length()) {
            message += " (at end of " + describe() + ')';
        } else {
            // Show the next 20 characters, or show 17 + ellipsis if more than 20.
            int remaining = mText.length() - pos;
            if (remaining <= 20) {
                message = message + " (at \"" + mText.substring(pos) + "\")";
            } else {
                message = message + " (at \"" + mText.substring(pos, pos + 17) + "...\")";
            }
        }
        // TODO: More specific exception type?
        return new IllegalArgumentException(message);
    }

    /**
     * Returns a short description of what this parser examines, to be used by error messages.
     */
    protected abstract String describe();
}
