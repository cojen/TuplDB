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

package org.cojen.tupl.sql;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;

import java.math.BigDecimal;

import java.net.URL;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import java.util.Calendar;
import java.util.Map;

/**
 * Implements as many sensible ResultSet defaults as possible.
 *
 * @author Brian S O'Neill
 */
public interface BaseResultSet extends ResultSet {
    /*
    @Override
    public default void close() throws SQLException {

    @Override
    public default boolean isClosed() throws SQLException {

    @Override
    public default ResultSetMetaData getMetaData() throws SQLException {

    @Override
    public default int findColumn(String columnLabel) throws SQLException {

    @Override
    public default boolean wasNull() throws SQLException {
    */

    @Override
    public default boolean next() throws SQLException {
        close();
        return false;
    }

    @Override
    public default SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public default void clearWarnings() throws SQLException {
    }

    @Override
    public default String getCursorName() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default boolean isBeforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default boolean isAfterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default boolean isFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default boolean isLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void beforeFirst() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void afterLast() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /*
    @Override
    public default boolean first() throws SQLException {
    */

    @Override
    public default boolean last() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default int getRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default boolean absolute(int row) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default boolean relative(int rows) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default boolean previous() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void setFetchDirection(int direction) throws SQLException {
        if (direction != FETCH_FORWARD) {
            throw new SQLException();
        }
    }

    @Override
    public default int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    public default void setFetchSize(int rows) throws SQLException {
    }

    @Override
    public default int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public default int getType() throws SQLException {
        return TYPE_FORWARD_ONLY;
    }

    @Override
    public default int getConcurrency() throws SQLException {
        return CONCUR_READ_ONLY;
    }

    @Override
    public default boolean rowUpdated() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default boolean rowInserted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default boolean rowDeleted() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void insertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void deleteRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void refreshRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void cancelRowUpdates() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void moveToInsertRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void moveToCurrentRow() throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default Statement getStatement() throws SQLException {
        return null;
    }

    @Override
    public default int getHoldability() throws SQLException {
        return CLOSE_CURSORS_AT_COMMIT;
    }

    @Override
    public default <T> T unwrap(Class<T> iface) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default boolean isWrapperFor(Class<?> iface) throws SQLException {
        return false;
    }

    // Get columns by index...

    /*
    @Override
    public default Object getObject(int columnIndex) throws SQLException {
    */

    @Override
    public default <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default Object getObject(int columnIndex, Map<String, Class<?>> map)
        throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
    @Override
    public default String getString(int columnIndex) throws SQLException {

    @Override
    public default boolean getBoolean(int columnIndex) throws SQLException {

    @Override
    public default byte getByte(int columnIndex) throws SQLException {

    @Override
    public default short getShort(int columnIndex) throws SQLException {

    @Override
    public default int getInt(int columnIndex) throws SQLException {

    @Override
    public default long getLong(int columnIndex) throws SQLException {

    @Override
    public default float getFloat(int columnIndex) throws SQLException {

    @Override
    public default double getDouble(int columnIndex) throws SQLException {

    @Override
    public default BigDecimal getBigDecimal(int columnIndex) throws SQLException {
    */

    @Override
    @Deprecated
    public default BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    /*
    @Override
    public default byte[] getBytes(int columnIndex) throws SQLException {
    */

    @Override
    public default Date getDate(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default Date getDate(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default Time getTime(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default Time getTime(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default Timestamp getTimestamp(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default URL getURL(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default Ref getRef(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default Blob getBlob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default Clob getClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default Array getArray(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default RowId getRowId(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    @Deprecated
    public default InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default InputStream getBinaryStream(int columnIndex) throws SQLException {
        return new ByteArrayInputStream(getBytes(columnIndex));
    }

    @Override
    public default Reader getCharacterStream(int columnIndex) throws SQLException {
        return new StringReader(getString(columnIndex));
    }

    @Override
    public default NClob getNClob(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default String getNString(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    // Update columns by index...

    @Override
    public default void updateNull(int columnIndex) throws SQLException {
        updateObject(columnIndex, null);
    }

    /*
    @Override
    public default void updateObject(int columnIndex, Object x) throws SQLException {
    */

    @Override
    public default void updateObject(int columnIndex, Object x, int scaleOrLength)
        throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
    @Override
    public default void updateString(int columnIndex, String x) throws SQLException {

    @Override
    public default void updateBoolean(int columnIndex, boolean x) throws SQLException {

    @Override
    public default void updateByte(int columnIndex, byte x) throws SQLException {

    @Override
    public default void updateShort(int columnIndex, short x) throws SQLException {

    @Override
    public default void updateInt(int columnIndex, int x) throws SQLException {

    @Override
    public default void updateLong(int columnIndex, long x) throws SQLException {

    @Override
    public default void updateFloat(int columnIndex, float x) throws SQLException {

    @Override
    public default void updateDouble(int columnIndex, double x) throws SQLException {

    @Override
    public default void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {

    @Override
    public default void updateBytes(int columnIndex, byte[] x) throws SQLException {
    */

    @Override
    public default void updateDate(int columnIndex, Date x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateTime(int columnIndex, Time x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateRef(int columnIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateBlob(int columnIndex, InputStream inputStream, long length)
        throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateClob(int columnIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateClob(int columnIndex, Reader reader, long length)
        throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateArray(int columnIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateAsciiStream(int columnIndex, InputStream x, int length)
        throws SQLException
    {
        updateAsciiStream(columnIndex, x, (long) length);
    }

    @Override
    public default void updateAsciiStream(int columnIndex, InputStream x, long length)
        throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateBinaryStream(int columnIndex, InputStream x, int length)
        throws SQLException
    {
        updateBinaryStream(columnIndex, x, (long) length);
    }

    @Override
    public default void updateBinaryStream(int columnIndex, InputStream x, long length)
        throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateCharacterStream(int columnIndex, Reader x, int length)
        throws SQLException
    {
        updateCharacterStream(columnIndex, x, (long) length);
    }

    @Override
    public default void updateCharacterStream(int columnIndex, Reader x, long length)
        throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateNClob(int columnIndex, Reader reader, long length)
        throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateNString(int columnIndex, String nString) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateNCharacterStream(int columnIndex, Reader x)
        throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateNCharacterStream(int columnIndex, Reader x, long length)
        throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    @Override
    public default void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    // The remaining methods are all for accessing columns by name and need not be overridden.

    // Get columns by name...

    @Override
    public default Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public default <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel), type);
    }

    @Override
    public default Object getObject(String columnLabel, Map<String, Class<?>> map)
        throws SQLException
    {
        return getObject(findColumn(columnLabel), map);
    }

    @Override
    public default String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public default boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public default byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public default short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public default int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public default long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public default float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public default double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public default BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    @Deprecated
    public default BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel), scale);
    }

    @Override
    public default byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public default Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public default Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel), cal);
    }

    @Override
    public default Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public default Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel), cal);
    }

    @Override
    public default Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public default Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel), cal);
    }

    @Override
    public default URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public default Ref getRef(String columnLabel) throws SQLException {
        return getRef(findColumn(columnLabel));
    }

    @Override
    public default Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public default Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public default Array getArray(String columnLabel) throws SQLException {
        return getArray(findColumn(columnLabel));
    }

    @Override
    public default RowId getRowId(String columnLabel) throws SQLException {
        return getRowId(findColumn(columnLabel));
    }

    @Override
    public default InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    @Deprecated
    public default InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public default InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public default Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public default NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public default String getNString(String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    @Override
    public default Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    @Override
    public default SQLXML getSQLXML(String columnLabel) throws SQLException {
        return getSQLXML(findColumn(columnLabel));
    }

    // Update columns by name...

    @Override
    public default void updateNull(String columnLabel) throws SQLException {
        updateNull(findColumn(columnLabel));
    }

    @Override
    public default void updateObject(String columnLabel, Object x, int scaleOrLength)
        throws SQLException
    {
        updateObject(findColumn(columnLabel), x, scaleOrLength);
    }

    @Override
    public default void updateObject(String columnLabel, Object x) throws SQLException {
        updateObject(findColumn(columnLabel), x);
    }

    @Override
    public default void updateString(String columnLabel, String x) throws SQLException {
        updateString(findColumn(columnLabel), x);
    }

    @Override
    public default void updateBoolean(String columnLabel, boolean x) throws SQLException {
        updateBoolean(findColumn(columnLabel), x);
    }

    @Override
    public default void updateByte(String columnLabel, byte x) throws SQLException {
        updateByte(findColumn(columnLabel), x);
    }

    @Override
    public default void updateShort(String columnLabel, short x) throws SQLException {
        updateShort(findColumn(columnLabel), x);
    }

    @Override
    public default void updateInt(String columnLabel, int x) throws SQLException {
        updateInt(findColumn(columnLabel), x);
    }

    @Override
    public default void updateLong(String columnLabel, long x) throws SQLException {
        updateLong(findColumn(columnLabel), x);
    }

    @Override
    public default void updateFloat(String columnLabel, float x) throws SQLException {
        updateFloat(findColumn(columnLabel), x);
    }

    @Override
    public default void updateDouble(String columnLabel, double x) throws SQLException {
        updateDouble(findColumn(columnLabel), x);
    }

    @Override
    public default void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        updateBigDecimal(findColumn(columnLabel), x);
    }

    @Override
    public default void updateBytes(String columnLabel, byte[] x) throws SQLException {
        updateBytes(findColumn(columnLabel), x);
    }

    @Override
    public default void updateDate(String columnLabel, Date x) throws SQLException {
        updateDate(findColumn(columnLabel), x);
    }

    @Override
    public default void updateTime(String columnLabel, Time x) throws SQLException {
        updateTime(findColumn(columnLabel), x);
    }

    @Override
    public default void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        updateTimestamp(findColumn(columnLabel), x);
    }

    @Override
    public default void updateRef(String columnLabel, Ref x) throws SQLException {
        updateRef(findColumn(columnLabel), x);
    }

    @Override
    public default void updateBlob(String columnLabel, Blob x) throws SQLException {
        updateBlob(findColumn(columnLabel), x);
    }

    @Override
    public default void updateBlob(String columnLabel, InputStream inputStream)
        throws SQLException
    {
        updateBlob(findColumn(columnLabel), inputStream);
    }

    @Override
    public default void updateBlob(String columnLabel, InputStream inputStream, long length)
        throws SQLException
    {
        updateBlob(findColumn(columnLabel), inputStream, length);
    }

    @Override
    public default void updateClob(String columnLabel, Clob x) throws SQLException {
        updateClob(findColumn(columnLabel), x);
    }

    @Override
    public default void updateClob(String columnLabel, Reader reader) throws SQLException {
        updateClob(findColumn(columnLabel), reader);
    }

    @Override
    public default void updateClob(String columnLabel, Reader reader, long length)
        throws SQLException
    {
        updateClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public default void updateArray(String columnLabel, Array x) throws SQLException {
        updateArray(findColumn(columnLabel), x);
    }

    @Override
    public default void updateRowId(String columnLabel, RowId x) throws SQLException {
        updateRowId(findColumn(columnLabel), x);
    }

    @Override
    public default void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        updateAsciiStream(findColumn(columnLabel), x);
    }

    @Override
    public default void updateAsciiStream(String columnLabel, InputStream x, int length)
        throws SQLException
    {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public default void updateAsciiStream(String columnLabel, InputStream x, long length)
        throws SQLException
    {
        updateAsciiStream(findColumn(columnLabel), x, length);
    }

    @Override
    public default void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        updateBinaryStream(findColumn(columnLabel), x);
    }

    @Override
    public default void updateBinaryStream(String columnLabel, InputStream x, int length)
        throws SQLException
    {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public default void updateBinaryStream(String columnLabel, InputStream x, long length)
        throws SQLException
    {
        updateBinaryStream(findColumn(columnLabel), x, length);
    }

    @Override
    public default void updateCharacterStream(String columnLabel, Reader reader)
        throws SQLException
    {
        updateCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public default void updateCharacterStream(String columnLabel, Reader reader, int length)
        throws SQLException
    {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public default void updateCharacterStream(String columnLabel, Reader reader, long length)
        throws SQLException
    {
        updateCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public default void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        updateNClob(findColumn(columnLabel), nClob);
    }

    @Override
    public default void updateNClob(String columnLabel, Reader reader) throws SQLException {
        updateNClob(findColumn(columnLabel), reader);
    }

    @Override
    public default void updateNClob(String columnLabel, Reader reader, long length)
        throws SQLException
    {
        updateNClob(findColumn(columnLabel), reader, length);
    }

    @Override
    public default void updateNString(String columnLabel, String nString) throws SQLException {
        updateNString(findColumn(columnLabel), nString);
    }

    @Override
    public default void updateNCharacterStream(String columnLabel, Reader reader)
        throws SQLException
    {
        updateNCharacterStream(findColumn(columnLabel), reader);
    }

    @Override
    public default void updateNCharacterStream(String columnLabel, Reader reader, long length)
        throws SQLException
    {
        updateNCharacterStream(findColumn(columnLabel), reader, length);
    }

    @Override
    public default void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        updateSQLXML(findColumn(columnLabel), xmlObject);
    }
}
