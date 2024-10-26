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

package org.cojen.tupl.table.expr;

import java.math.BigDecimal;
import java.math.BigInteger;

import java.io.IOException;
import java.io.PushbackReader;
import java.io.Reader;
import java.io.StringReader;

/**
 * 
 *
 * @author Brian S. O'Neill
 */
final class Tokenizer {
    private final PushbackReader mIn;

    // Temporary space.
    private final StringBuilder mWord = new StringBuilder();

    private int mPos;

    Tokenizer(String source) {
        this(new StringReader(source));
    }

    Tokenizer(Reader in) {
        mIn = new PushbackReader(in, 2);
        mPos = -1;
    }

    /**
     * @return T_EOF if none left
     */
    public Token next() throws IOException {
        try {
            return next(read());
        } catch (IOException e) {
            try {
                mIn.close();
            } catch (IOException e2) {
            }
            throw e;
        }
    }

    private Token eof() {
        return new Token(mPos, mPos, Token.T_EOF);
    }

    private Token next(int c) throws IOException {
        int type;
        int extraWidth = 0;

        loop: while (true) {
            if (c < 0) {
                mIn.close();
                return eof();
            }

            switch (c) {
            case '\n': case '\r': case ' ': case '\t': case '\0':
                break;

            case '(':
                type = Token.T_LPAREN;
                break loop;

            case ')':
                type = Token.T_RPAREN;
                break loop;

            case '{':
                type = Token.T_LBRACE;
                break loop;

            case '}':
                type = Token.T_RBRACE;
                break loop;

            case '=':
                int next = read();
                if (next == '=') {
                    type = Token.T_EQ;
                    extraWidth = 1;
                } else {
                    type = Token.T_ASSIGN;
                    unread(next);
                }
                break loop;

            case '!':
                next = read();
                if (next == '=') {
                    type = Token.T_NE;
                    extraWidth = 1;
                } else {
                    type = Token.T_NOT;
                    unread(next);
                }
                break loop;

            case '>':
                next = read();
                if (next == '=') {
                    type = Token.T_GE;
                    extraWidth = 1;
                } else {
                    type = Token.T_GT;
                    unread(next);
                }
                break loop;

            case '<':
                next = read();
                if (next == '=') {
                    type = Token.T_LE;
                    extraWidth = 1;
                } else {
                    type = Token.T_LT;
                    unread(next);
                }
                break loop;

            case '|':
                next = read();
                if (next == '|') {
                    type = Token.T_LOR;
                    extraWidth = 1;
                } else {
                    type = Token.T_OR;
                    unread(next);
                }
                break loop;

            case '^':
                type = Token.T_XOR;
                break loop;

            case '&':
                next = read();
                if (next == '&') {
                    type = Token.T_LAND;
                    extraWidth = 1;
                } else {
                    type = Token.T_AND;
                    unread(next);
                }
                break loop;

            case '~':
                type = Token.T_TILDE;
                break loop;

            case ',':
                type = Token.T_COMMA;
                break loop;

            case ';':
                type = Token.T_SEMI;
                break loop;

            case ':':
                type = Token.T_COLON;
                break loop;

            case '?':
                type = Token.T_ARG;
                break loop;

            case '+':
                type = Token.T_PLUS;
                break loop;

            case '-':
                type = Token.T_MINUS;
                break loop;

            case '*':
                type = Token.T_STAR;
                break loop;

            case '/':
                type = Token.T_DIV;
                break loop;

            case '%':
                type = Token.T_REM;
                break loop;

            case '"': case '\'':
                return parseQuoted(Token.T_STRING, c);

            case '`':
                return parseQuoted(Token.T_IDENTIFIER, c);

            case '0': case '1': case '2': case '3': case '4':
            case '5': case '6': case '7': case '8': case '9':
                return parseNumber(c);

            case '.':
                next = read();
                if (isDigit(next)) {
                    unread(next);
                    return parseNumber(c);
                }
                if (next == '.') {
                    type = Token.T_DOTDOT;
                    extraWidth = 1;
                } else {
                    type = Token.T_DOT;
                    unread(next);
                }
                break loop;

            case 'a': case 'b': case 'c': case 'd': case 'e': case 'f': case 'g': case 'h':
            case 'i': case 'j': case 'k': case 'l': case 'm': case 'n': case 'o': case 'p':
            case 'q': case 'r': case 's': case 't': case 'u': case 'v': case 'w': case 'x':
            case 'y': case 'z':

            case 'A': case 'B': case 'C': case 'D': case 'E': case 'F': case 'G': case 'H':
            case 'I': case 'J': case 'K': case 'L': case 'M': case 'N': case 'O': case 'P':
            case 'Q': case 'R': case 'S': case 'T': case 'U': case 'V': case 'W': case 'X':
            case 'Y': case 'Z':

                return parseIdentifierOrKeyword(c);

            default:
                if (Character.isWhitespace(c)) {
                    break;
                }
                if (Character.isJavaIdentifierStart(c)) {
                    return parseIdentifierOrKeyword(c);
                }
                return new Token.Text(mPos, mPos + 1, Token.T_UNKNOWN, String.valueOf((char) c));
            }

            c = read();
        }

        return new Token(mPos - extraWidth, mPos + 1, type);
    }

    private int read() throws IOException {
        mPos++;
        return mIn.read();
    }

    private int peek() throws IOException {
        int c = mIn.read();
        if (c >= 0) {
            mIn.unread(c);
        }
        return c;
    }

    private void unread(int c) throws IOException {
        mPos--;
        if (c >= 0) {
            mIn.unread(c);
        }
    }

    private Token parseQuoted(int type, int delimiter) throws IOException {
        // TODO: Support unicode escapes. Don't bother supporting unicode escapes at a sooner
        // phase like Java does, because UTF-8 is now ubiquitous.

        final int startPos = mPos;
        mWord.setLength(0);

        while (true) {
            int c = read();
            if (c < 0) {
                unread(c);
                break;
            } else if (c == delimiter) {
                break;
            } else if (c == '\r') {
                int next = read();
                if (next == '\n') {
                    c = next;
                } else {
                    unread(next);
                }
            } if (c == '\\') {
                int next = read();
                switch (next) {
                case '0':
                    c = '\0';
                    break;
                case 'b':
                    c = '\b';
                    break;
                case 't':
                    c = '\t';
                    break;
                case 'n':
                    c = '\n';
                    break;
                case 'f':
                    c = '\f';
                    break;
                case 'r':
                    c = '\r';
                    break;
                case '\\':
                    c = '\\';
                    break;
                case '\'':
                    c = '\'';
                    break;
                case '\"':
                    c = '\"';
                    break;
                case '`':
                    c = '`';
                    break;
                default:
                    unread(next);
                    break;
                }
            }

            mWord.append((char) c);
        }

        return new Token.Text(startPos, mPos + 1, type, mWord.toString());
    }

    private Token parseNumber(int c) throws IOException {
        final int startPos = mPos;
        mWord.setLength(0);

        boolean bin = false;
        boolean hex = false;
        boolean fp = false;
        boolean exp = false;

        altRadix: if (c == '0') {
            int peek = peek();
            if (peek == 'x' || peek == 'X') {
                hex = true;
            } else if (peek == 'b' || peek == 'B') {
                bin = true;
            } else {
                break altRadix;
            }
            read(); // skip the radix prefix
            c = read();
        }

        for (;; c = read()) {
            if (c < 0) {
                unread(c);
                break;
            }

            switch (c) {
            case '.':
                if (!bin && !fp && !exp && peek() != '.') {
                    fp = true;
                    mWord.append((char) c);
                    continue;
                }
                break;

            case '0': case '1':
                mWord.append((char) c);
                continue;

            case '2': case '3': case '4': case '5': case '6': case '7': case '8': case '9':
                if (!bin) {
                    mWord.append((char) c);
                    continue;
                }
                break;

            case 'a': case 'b': case 'c': case 'd': case 'e': case 'f':
            case 'A': case 'B': case 'C': case 'D': case 'E': case 'F':
                if (hex) {
                    mWord.append((char) c);
                    continue;
                }
                int expChar;
                if (!bin && !exp && (c == 'e' || c == 'E') && (expChar = isExponentStart()) >= 0) {
                    fp = true;
                    exp = true;
                    mWord.append((char) c);
                    mWord.append((char) expChar);
                    continue;
                }
                break;

            case 'p': case 'P':
                if (hex && !exp && (expChar = isExponentStart()) >= 0) {
                    fp = true;
                    exp = true;
                    mWord.append((char) c);
                    mWord.append((char) expChar);
                    continue;
                }
                break;

            case '_':
                continue;
            }

            unread(c);
            break;
        }

        int length = mWord.length();

        if (length == 0) {
            int prefix;
            if (bin) {
                prefix = 'b';
            } else if (hex) {
                prefix = 'x';
            } else {
                throw new AssertionError();
            }
            unread(prefix);
            return new Token.Int(startPos, startPos + 1, 0);
        }

        int suffix = read();

        switch (suffix) {
            case 'f', 'F' -> {
                suffix = 'f';
                fp = true;
            }

            case 'd', 'D' -> {
                suffix = 'd';
                fp = true;
            }

            case 'l', 'L' -> {
                if (fp) {
                    unread(suffix);
                    suffix = 0;
                } else {
                    suffix = 'l';
                }
            }

            case 'g', 'G' -> {
                if (fp && (bin || hex)) {
                    unread(suffix);
                    suffix = 0;
                } else {
                    suffix = 'g';
                }
            }

            default -> {
                unread(suffix);
                suffix = 0;
            }
        }

        if (fp) {
            if (hex) {
                mWord.insert(0, "0x");
                if (!exp) {
                    mWord.append("p0");
                }
            }

            String str = mWord.toString();

            switch (suffix) {
            case 'd': default: return new Token.Double(startPos, mPos + 1, Double.parseDouble(str));
            case 'f': return new Token.Float(startPos, mPos + 1, Float.parseFloat(str));
            case 'g': return new Token.BigDec(startPos, mPos + 1, new BigDecimal(str));
            }
        }

        int radix = bin ? 2 : (hex ? 16 : 10);

        switch (suffix) {
        default:
            try {
                return new Token.Int(startPos, mPos + 1, Integer.parseInt(mWord, 0, length, radix));
            } catch (NumberFormatException e) {
                // Fallthrough to the next case.
            }
        case 'l':
            try {
                return new Token.Long(startPos, mPos + 1, Long.parseLong(mWord, 0, length, radix));
            } catch (NumberFormatException e) {
                // Fallthrough to the next case.
            }
        case 'g':
            return new Token.BigInt(startPos, mPos + 1, new BigInteger(mWord.toString(), radix));
        }
    }

    private static boolean isDigit(int c) {
        return c >= '0' && c <= '9';
    }

    /**
     * @return exponent sign character or else -1
     */
    private int isExponentStart() throws IOException {
        int c = read();
        if (c == '-' || c == '+') {
            if (isDigit(peek())) {
                return c;
            }
            unread(c);
            return -1;
        } else {
            unread(c);
            return isDigit(c) ? '+' : -1;
        }
    }

    private Token parseIdentifierOrKeyword(int c) throws IOException {
        final int startPos = mPos;
        mWord.setLength(0);
        mWord.append((char) c);

        while (true) {
            c = read();
            if (c <= 0) {
                unread(c);
                break;
            }
            if (!Character.isJavaIdentifierPart(c)) {
                unread(c);
                break;
            }
            mWord.append((char) c);
        }

        String str = mWord.toString();

        final int type;

        ident: {
            switch (str) {
            case "false":
                type = Token.T_FALSE;
                break ident;
            case "true":
                type = Token.T_TRUE;
                break ident;
            case "null":
                type = Token.T_NULL;
                break ident;
            case "in":
                type = Token.T_IN;
                break ident;
            }

            return new Token.Text(startPos, mPos + 1, Token.T_IDENTIFIER, str);
        }

        return new Token(startPos, mPos + 1, type);
    }
}
