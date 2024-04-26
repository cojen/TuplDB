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

/**
 * 
 *
 * @author Brian S. O'Neill
 */
sealed class Token {
    static final int T_EOF = -1;

    // Note: BinaryOpExpr and FilterExpr depend on the values and ordering of these type code.
    // Also, the relational type codes must exactly match the corresponding ColumnFilter.OP
    // constants.
    static final int T_EQ = 0, T_NE = 1, T_GE = 2, T_LT = 3, T_LE = 4, T_GT = 5;
    static final int T_LAND = 6, T_LOR = 7, T_AND = 8, T_OR = 9, T_NOT = 10;
    static final int T_PLUS = 11, T_MINUS = 12, T_STAR = 13, T_DIV = 14, T_REM = 15;

    static final int T_LPAREN = 16, T_RPAREN = 17, T_LBRACE = 18, T_RBRACE = 19;

    static final int T_ASSIGN = 20, T_TILDE = 21, T_COMMA = 22, T_DOT = 23, T_ARG = 24;

    // Keywords.
    static final int T_FALSE = 25, T_TRUE = 26, T_NULL = 27;

    // Tokens which have a text value.
    static final int T_IDENTIFIER = 28, T_STRING = 29;

    // Numerical constants.
    static final int T_INT = 30, T_LONG = 31, T_BIGINT = 32,
        T_FLOAT = 33, T_DOUBLE = 34, T_BIGDEC = 35;

    private final int mStartPos, mEndPos, mType;

    /**
     * @param startPos source code start position, zero-based, inclusive; is -1 if not applicable
     * @param endPos source code end position, zero-based, exclusive; is -1 if not applicable
     * @param type see T_* constants
     */
    Token(int startPos, int endPos, int type) {
        mStartPos = startPos;
        mEndPos = endPos;
        mType = type;
    }

    /**
     * @return source code start position, zero-based, inclusive; is -1 if not applicable
     */
    final int startPos() {
        return mStartPos;
    }

    /**
     * @return source code end position, zero-based, exclusive; is -1 if not applicable
     */
    final int endPos() {
        return mEndPos;
    }

    final int type() {
        return mType;
    }

    static final class Text extends Token {
        final String mText;

        Text(int startPos, int endPos, int type, String text) {
            super(startPos, endPos, type);
            mText = text;
        }
    }

    static final class Int extends Token {
        final int mValue;

        Int(int startPos, int endPos, int value) {
            super(startPos, endPos, T_INT);
            mValue = value;
        }
    }

    static final class Long extends Token {
        final long mValue;

        Long(int startPos, int endPos, long value) {
            super(startPos, endPos, T_LONG);
            mValue = value;
        }
    }

    static final class BigInt extends Token {
        final BigInteger mValue;

        BigInt(int startPos, int endPos, BigInteger value) {
            super(startPos, endPos, T_BIGINT);
            mValue = value;
        }
    }

    static final class Float extends Token {
        final float mValue;

        Float(int startPos, int endPos, float value) {
            super(startPos, endPos, T_FLOAT);
            mValue = value;
        }
    }

    static final class Double extends Token {
        final double mValue;

        Double(int startPos, int endPos, double value) {
            super(startPos, endPos, T_DOUBLE);
            mValue = value;
        }
    }

    static final class BigDec extends Token {
        final BigDecimal mValue;

        BigDec(int startPos, int endPos, BigDecimal value) {
            super(startPos, endPos, T_BIGDEC);
            mValue = value;
        }
    }
}
