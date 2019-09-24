/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME AG, Zurich, Switzerland
 *
 * You may not modify, publish, transmit, transfer or sell, reproduce,
 * create derivative works from, distribute, perform, display, or in
 * any way exploit any of the content, in whole or in part, except as
 * otherwise expressly permitted in writing by the copyright owner or
 * as specified in the license file distributed with this product.
 *
 * If you have any questions please contact the copyright holder:
 * website: www.knime.com
 * email: contact@knime.com
 * ---------------------------------------------------------------------
 *
 * History
 *   Created on 06.02.2018 by oole
 */
package org.knime.bigdata.spark.local.database;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

import org.apache.hive.jdbc.HiveQueryResultSet;
import org.knime.bigdata.spark.local.node.create.LocalEnvironmentCreatorNodeFactory;


/**
 * This class wraps the {@link HiveQueryResultSet}. It overrides the {@link HiveQueryResultSet#getString(String)} to
 * enable metadata collection from the local thrift server instance set up by {@link LocalEnvironmentCreatorNodeFactory}
 *
 * @author Ole Ostergaard, KNIME AG, Konstanz, Germany
 */
public class LocalHiveQueryResultSet implements ResultSet {

	HiveQueryResultSet m_resultset;

	/**
	 * Constructor.
	 * 
	 * @param resultSet The underlying hive result set.
	 */
	public LocalHiveQueryResultSet(HiveQueryResultSet resultSet) {
		m_resultset = resultSet;
	}

	@Override
	public int hashCode() {
		return m_resultset.hashCode();
	}

	@Override
	public boolean absolute(int row) throws SQLException {
		return m_resultset.absolute(row);
	}

	@Override
	public void afterLast() throws SQLException {
		m_resultset.afterLast();
	}

	@Override
	public void cancelRowUpdates() throws SQLException {
		m_resultset.cancelRowUpdates();
	}

	@Override
	public void deleteRow() throws SQLException {
		m_resultset.deleteRow();
	}

	@Override
	public int findColumn(String columnName) throws SQLException {
		return m_resultset.findColumn(columnName);
	}

	@Override
	public boolean first() throws SQLException {
		return m_resultset.first();
	}

	@Override
	public Array getArray(int i) throws SQLException {
		return m_resultset.getArray(i);
	}

	@Override
	public boolean equals(Object obj) {
		return m_resultset.equals(obj);
	}

	@Override
	public Array getArray(String colName) throws SQLException {
		return m_resultset.getArray(colName);
	}

	@Override
	public InputStream getAsciiStream(int columnIndex) throws SQLException {
		return m_resultset.getAsciiStream(columnIndex);
	}

	@Override
	public InputStream getAsciiStream(String columnName) throws SQLException {
		return m_resultset.getAsciiStream(columnName);
	}

	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return m_resultset.getBigDecimal(columnIndex);
	}

	@Override
	public BigDecimal getBigDecimal(String columnName) throws SQLException {
		return m_resultset.getBigDecimal(columnName);
	}

	@Deprecated
    @Override
	public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
		return m_resultset.getBigDecimal(columnIndex, scale);
	}

	@Deprecated
    @Override
	public BigDecimal getBigDecimal(String columnName, int scale) throws SQLException {
		return m_resultset.getBigDecimal(columnName, scale);
	}

	@Override
	public InputStream getBinaryStream(int columnIndex) throws SQLException {
		return m_resultset.getBinaryStream(columnIndex);
	}

	@Override
	public InputStream getBinaryStream(String columnName) throws SQLException {
		return m_resultset.getBinaryStream(columnName);
	}

	@Override
	public Blob getBlob(int i) throws SQLException {
		return m_resultset.getBlob(i);
	}

	@Override
	public Blob getBlob(String colName) throws SQLException {
		return m_resultset.getBlob(colName);
	}

	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		return m_resultset.getBoolean(columnIndex);
	}

	@Override
	public boolean getBoolean(String columnName) throws SQLException {
		return m_resultset.getBoolean(columnName);
	}

	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return m_resultset.getByte(columnIndex);
	}

	@Override
	public byte getByte(String columnName) throws SQLException {
		return m_resultset.getByte(columnName);
	}

	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		return m_resultset.getBytes(columnIndex);
	}

	@Override
	public byte[] getBytes(String columnName) throws SQLException {
		return m_resultset.getBytes(columnName);
	}

	@Override
	public Reader getCharacterStream(int columnIndex) throws SQLException {
		return m_resultset.getCharacterStream(columnIndex);
	}

	@Override
	public Reader getCharacterStream(String columnName) throws SQLException {
		return m_resultset.getCharacterStream(columnName);
	}

	@Override
	public Clob getClob(int i) throws SQLException {
		return m_resultset.getClob(i);
	}

	@Override
	public Clob getClob(String colName) throws SQLException {
		return m_resultset.getClob(colName);
	}

	@Override
	public int getConcurrency() throws SQLException {
		return m_resultset.getConcurrency();
	}

	@Override
	public String getCursorName() throws SQLException {
		return m_resultset.getCursorName();
	}

	@Override
	public Date getDate(int columnIndex) throws SQLException {
		return m_resultset.getDate(columnIndex);
	}

	@Override
	public Date getDate(String columnName) throws SQLException {
		return m_resultset.getDate(columnName);
	}

	@Override
	public Date getDate(int columnIndex, Calendar cal) throws SQLException {
		return m_resultset.getDate(columnIndex, cal);
	}

	@Override
	public Date getDate(String columnName, Calendar cal) throws SQLException {
		return m_resultset.getDate(columnName, cal);
	}

	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return m_resultset.getDouble(columnIndex);
	}

	@Override
	public String toString() {
		return m_resultset.toString();
	}

	@Override
	public double getDouble(String columnName) throws SQLException {
		return m_resultset.getDouble(columnName);
	}

	@Override
	public int getFetchDirection() throws SQLException {
		return m_resultset.getFetchDirection();
	}

	@Override
	public float getFloat(int columnIndex) throws SQLException {
		return m_resultset.getFloat(columnIndex);
	}

	@Override
	public float getFloat(String columnName) throws SQLException {
		return m_resultset.getFloat(columnName);
	}

	@Override
	public int getHoldability() throws SQLException {
		return m_resultset.getHoldability();
	}

	@Override
	public int getInt(int columnIndex) throws SQLException {
		return m_resultset.getInt(columnIndex);
	}

	@Override
	public void close() throws SQLException {
		m_resultset.close();
	}

	@Override
	public int getInt(String columnName) throws SQLException {
		return m_resultset.getInt(columnName);
	}

	@Override
	public long getLong(int columnIndex) throws SQLException {
		return m_resultset.getLong(columnIndex);
	}

	@Override
	public long getLong(String columnName) throws SQLException {
		return m_resultset.getLong(columnName);
	}

	@Override
	public boolean next() throws SQLException {
		return m_resultset.next();
	}

	@Override
	public Reader getNCharacterStream(int arg0) throws SQLException {
		return m_resultset.getNCharacterStream(arg0);
	}

	@Override
	public Reader getNCharacterStream(String arg0) throws SQLException {
		return m_resultset.getNCharacterStream(arg0);
	}

	@Override
	public NClob getNClob(int arg0) throws SQLException {
		return m_resultset.getNClob(arg0);
	}

	@Override
	public NClob getNClob(String columnLabel) throws SQLException {
		return m_resultset.getNClob(columnLabel);
	}

	@Override
	public String getNString(int columnIndex) throws SQLException {
		return m_resultset.getNString(columnIndex);
	}

	@Override
	public String getNString(String columnLabel) throws SQLException {
		return m_resultset.getNString(columnLabel);
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return m_resultset.getMetaData();
	}

	@Override
	public void setFetchSize(int rows) throws SQLException {
		m_resultset.setFetchSize(rows);
	}

	@Override
	public int getType() throws SQLException {
		return m_resultset.getType();
	}

	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return m_resultset.getObject(columnIndex);
	}

	@Override
	public int getFetchSize() throws SQLException {
		return m_resultset.getFetchSize();
	}

	@Override
	public Object getObject(String columnName) throws SQLException {
		return m_resultset.getObject(columnName);
	}

	@Override
	public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
		return m_resultset.getObject(columnLabel, type);
	}

	@Override
	public Object getObject(int i, Map<String, Class<?>> map) throws SQLException {
		return m_resultset.getObject(i, map);
	}

	@Override
	public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
		return m_resultset.getObject(columnIndex, type);
	}

	@Override
	public Object getObject(String colName, Map<String, Class<?>> map) throws SQLException {
		return m_resultset.getObject(colName, map);
	}

	@Override
	public void beforeFirst() throws SQLException {
		m_resultset.beforeFirst();
	}

	@Override
	public Ref getRef(int i) throws SQLException {
		return m_resultset.getRef(i);
	}

	@Override
	public Ref getRef(String colName) throws SQLException {
		return m_resultset.getRef(colName);
	}

	@Override
	public RowId getRowId(int columnIndex) throws SQLException {
		return m_resultset.getRowId(columnIndex);
	}

	@Override
	public RowId getRowId(String columnLabel) throws SQLException {
		return m_resultset.getRowId(columnLabel);
	}

	@Override
	public boolean isBeforeFirst() throws SQLException {
		return m_resultset.isBeforeFirst();
	}

	@Override
	public SQLXML getSQLXML(int columnIndex) throws SQLException {
		return m_resultset.getSQLXML(columnIndex);
	}

	@Override
	public int getRow() throws SQLException {
		return m_resultset.getRow();
	}

	@Override
	public SQLXML getSQLXML(String columnLabel) throws SQLException {
		return m_resultset.getSQLXML(columnLabel);
	}

	@Override
	public short getShort(int columnIndex) throws SQLException {
		return m_resultset.getShort(columnIndex);
	}

	@Override
	public short getShort(String columnName) throws SQLException {
		return m_resultset.getShort(columnName);
	}

	@Override
	public Statement getStatement() throws SQLException {
		return m_resultset.getStatement();
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		return m_resultset.getString(columnIndex);
	}

	@Override
	public String getString(String columnName) throws SQLException {
		if ("TABLE_NAME".equals(columnName)) {
			columnName = "tablename";
		}
		return m_resultset.getString(columnName);
	}

	@Override
	public Time getTime(int columnIndex) throws SQLException {
		return m_resultset.getTime(columnIndex);
	}

	@Override
	public Time getTime(String columnName) throws SQLException {
		return m_resultset.getTime(columnName);
	}

	@Override
	public Time getTime(int columnIndex, Calendar cal) throws SQLException {
		return m_resultset.getTime(columnIndex, cal);
	}

	@Override
	public Time getTime(String columnName, Calendar cal) throws SQLException {
		return m_resultset.getTime(columnName, cal);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex) throws SQLException {
		return m_resultset.getTimestamp(columnIndex);
	}

	@Override
	public Timestamp getTimestamp(String columnName) throws SQLException {
		return m_resultset.getTimestamp(columnName);
	}

	@Override
	public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
		return m_resultset.getTimestamp(columnIndex, cal);
	}

	@Override
	public Timestamp getTimestamp(String columnName, Calendar cal) throws SQLException {
		return m_resultset.getTimestamp(columnName, cal);
	}

	@Override
	public URL getURL(int columnIndex) throws SQLException {
		return m_resultset.getURL(columnIndex);
	}

	@Override
	public URL getURL(String columnName) throws SQLException {
		return m_resultset.getURL(columnName);
	}

	@Deprecated
    @Override
	public InputStream getUnicodeStream(int columnIndex) throws SQLException {
		return m_resultset.getUnicodeStream(columnIndex);
	}

	@Deprecated
    @Override
	public InputStream getUnicodeStream(String columnName) throws SQLException {
		return m_resultset.getUnicodeStream(columnName);
	}

	@Override
	public void insertRow() throws SQLException {
		m_resultset.insertRow();
	}

	@Override
	public boolean isAfterLast() throws SQLException {
		return m_resultset.isAfterLast();
	}

	@Override
	public boolean isClosed() throws SQLException {
		return m_resultset.isClosed();
	}

	@Override
	public boolean isFirst() throws SQLException {
		return m_resultset.isFirst();
	}

	@Override
	public boolean isLast() throws SQLException {
		return m_resultset.isLast();
	}

	@Override
	public boolean last() throws SQLException {
		return m_resultset.last();
	}

	@Override
	public void moveToCurrentRow() throws SQLException {
		m_resultset.moveToCurrentRow();
	}

	@Override
	public void moveToInsertRow() throws SQLException {
		m_resultset.moveToInsertRow();
	}

	@Override
	public boolean previous() throws SQLException {
		return m_resultset.previous();
	}

	@Override
	public void refreshRow() throws SQLException {
		m_resultset.refreshRow();
	}

	@Override
	public boolean relative(int rows) throws SQLException {
		return m_resultset.relative(rows);
	}

	@Override
	public boolean rowDeleted() throws SQLException {
		return m_resultset.rowDeleted();
	}

	@Override
	public boolean rowInserted() throws SQLException {
		return m_resultset.rowInserted();
	}

	@Override
	public boolean rowUpdated() throws SQLException {
		return m_resultset.rowUpdated();
	}

	@Override
	public void setFetchDirection(int direction) throws SQLException {
		m_resultset.setFetchDirection(direction);
	}

	@Override
	public void updateArray(int columnIndex, Array x) throws SQLException {
		m_resultset.updateArray(columnIndex, x);
	}

	@Override
	public void updateArray(String columnName, Array x) throws SQLException {
		m_resultset.updateArray(columnName, x);
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
		m_resultset.updateAsciiStream(columnIndex, x);
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
		m_resultset.updateAsciiStream(columnLabel, x);
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
		m_resultset.updateAsciiStream(columnIndex, x, length);
	}

	@Override
	public void updateAsciiStream(String columnName, InputStream x, int length) throws SQLException {
		m_resultset.updateAsciiStream(columnName, x, length);
	}

	@Override
	public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
		m_resultset.updateAsciiStream(columnIndex, x, length);
	}

	@Override
	public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
		m_resultset.updateAsciiStream(columnLabel, x, length);
	}

	@Override
	public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
		m_resultset.updateBigDecimal(columnIndex, x);
	}

	@Override
	public void updateBigDecimal(String columnName, BigDecimal x) throws SQLException {
		m_resultset.updateBigDecimal(columnName, x);
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
		m_resultset.updateBinaryStream(columnIndex, x);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
		m_resultset.updateBinaryStream(columnLabel, x);
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
		m_resultset.updateBinaryStream(columnIndex, x, length);
	}

	@Override
	public void updateBinaryStream(String columnName, InputStream x, int length) throws SQLException {
		m_resultset.updateBinaryStream(columnName, x, length);
	}

	@Override
	public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
		m_resultset.updateBinaryStream(columnIndex, x, length);
	}

	@Override
	public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
		m_resultset.updateBinaryStream(columnLabel, x, length);
	}

	@Override
	public void updateBlob(int columnIndex, Blob x) throws SQLException {
		m_resultset.updateBlob(columnIndex, x);
	}

	@Override
	public void updateBlob(String columnName, Blob x) throws SQLException {
		m_resultset.updateBlob(columnName, x);
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
		m_resultset.updateBlob(columnIndex, inputStream);
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
		m_resultset.updateBlob(columnLabel, inputStream);
	}

	@Override
	public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
		m_resultset.updateBlob(columnIndex, inputStream, length);
	}

	@Override
	public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
		m_resultset.updateBlob(columnLabel, inputStream, length);
	}

	@Override
	public void updateBoolean(int columnIndex, boolean x) throws SQLException {
		m_resultset.updateBoolean(columnIndex, x);
	}

	@Override
	public void updateBoolean(String columnName, boolean x) throws SQLException {
		m_resultset.updateBoolean(columnName, x);
	}

	@Override
	public void updateByte(int columnIndex, byte x) throws SQLException {
		m_resultset.updateByte(columnIndex, x);
	}

	@Override
	public void updateByte(String columnName, byte x) throws SQLException {
		m_resultset.updateByte(columnName, x);
	}

	@Override
	public void updateBytes(int columnIndex, byte[] x) throws SQLException {
		m_resultset.updateBytes(columnIndex, x);
	}

	@Override
	public void updateBytes(String columnName, byte[] x) throws SQLException {
		m_resultset.updateBytes(columnName, x);
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
		m_resultset.updateCharacterStream(columnIndex, x);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
		m_resultset.updateCharacterStream(columnLabel, reader);
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
		m_resultset.updateCharacterStream(columnIndex, x, length);
	}

	@Override
	public void updateCharacterStream(String columnName, Reader reader, int length) throws SQLException {
		m_resultset.updateCharacterStream(columnName, reader, length);
	}

	@Override
	public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		m_resultset.updateCharacterStream(columnIndex, x, length);
	}

	@Override
	public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		m_resultset.updateCharacterStream(columnLabel, reader, length);
	}

	@Override
	public void updateClob(int columnIndex, Clob x) throws SQLException {
		m_resultset.updateClob(columnIndex, x);
	}

	@Override
	public void updateClob(String columnName, Clob x) throws SQLException {
		m_resultset.updateClob(columnName, x);
	}

	@Override
	public void updateClob(int columnIndex, Reader reader) throws SQLException {
		m_resultset.updateClob(columnIndex, reader);
	}

	@Override
	public void updateClob(String columnLabel, Reader reader) throws SQLException {
		m_resultset.updateClob(columnLabel, reader);
	}

	@Override
	public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
		m_resultset.updateClob(columnIndex, reader, length);
	}

	@Override
	public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
		m_resultset.updateClob(columnLabel, reader, length);
	}

	@Override
	public void updateDate(int columnIndex, Date x) throws SQLException {
		m_resultset.updateDate(columnIndex, x);
	}

	@Override
	public void updateDate(String columnName, Date x) throws SQLException {
		m_resultset.updateDate(columnName, x);
	}

	@Override
	public void updateDouble(int columnIndex, double x) throws SQLException {
		m_resultset.updateDouble(columnIndex, x);
	}

	@Override
	public void updateDouble(String columnName, double x) throws SQLException {
		m_resultset.updateDouble(columnName, x);
	}

	@Override
	public void updateFloat(int columnIndex, float x) throws SQLException {
		m_resultset.updateFloat(columnIndex, x);
	}

	@Override
	public void updateFloat(String columnName, float x) throws SQLException {
		m_resultset.updateFloat(columnName, x);
	}

	@Override
	public void updateInt(int columnIndex, int x) throws SQLException {
		m_resultset.updateInt(columnIndex, x);
	}

	@Override
	public void updateInt(String columnName, int x) throws SQLException {
		m_resultset.updateInt(columnName, x);
	}

	@Override
	public void updateLong(int columnIndex, long x) throws SQLException {
		m_resultset.updateLong(columnIndex, x);
	}

	@Override
	public void updateLong(String columnName, long x) throws SQLException {
		m_resultset.updateLong(columnName, x);
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
		m_resultset.updateNCharacterStream(columnIndex, x);
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
		m_resultset.updateNCharacterStream(columnLabel, reader);
	}

	@Override
	public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
		m_resultset.updateNCharacterStream(columnIndex, x, length);
	}

	@Override
	public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
		m_resultset.updateNCharacterStream(columnLabel, reader, length);
	}

	@Override
	public void updateNClob(int columnIndex, NClob clob) throws SQLException {
		m_resultset.updateNClob(columnIndex, clob);
	}

	@Override
	public void updateNClob(String columnLabel, NClob clob) throws SQLException {
		m_resultset.updateNClob(columnLabel, clob);
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		m_resultset.updateNClob(columnIndex, reader);
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader) throws SQLException {
		m_resultset.updateNClob(columnLabel, reader);
	}

	@Override
	public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
		m_resultset.updateNClob(columnIndex, reader, length);
	}

	@Override
	public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
		m_resultset.updateNClob(columnLabel, reader, length);
	}

	@Override
	public void updateNString(int columnIndex, String string) throws SQLException {
		m_resultset.updateNString(columnIndex, string);
	}

	@Override
	public void updateNString(String columnLabel, String string) throws SQLException {
		m_resultset.updateNString(columnLabel, string);
	}

	@Override
	public void updateNull(int columnIndex) throws SQLException {
		m_resultset.updateNull(columnIndex);
	}

	@Override
	public void updateNull(String columnName) throws SQLException {
		m_resultset.updateNull(columnName);
	}

	@Override
	public void updateObject(int columnIndex, Object x) throws SQLException {
		m_resultset.updateObject(columnIndex, x);
	}

	@Override
	public void updateObject(String columnName, Object x) throws SQLException {
		m_resultset.updateObject(columnName, x);
	}

	@Override
	public void updateObject(int columnIndex, Object x, int scale) throws SQLException {
		m_resultset.updateObject(columnIndex, x, scale);
	}

	@Override
	public void updateObject(String columnName, Object x, int scale) throws SQLException {
		m_resultset.updateObject(columnName, x, scale);
	}

	@Override
	public void updateRef(int columnIndex, Ref x) throws SQLException {
		m_resultset.updateRef(columnIndex, x);
	}

	@Override
	public void updateRef(String columnName, Ref x) throws SQLException {
		m_resultset.updateRef(columnName, x);
	}

	@Override
	public void updateRow() throws SQLException {
		m_resultset.updateRow();
	}

	@Override
	public void updateRowId(int columnIndex, RowId x) throws SQLException {
		m_resultset.updateRowId(columnIndex, x);
	}

	@Override
	public void updateRowId(String columnLabel, RowId x) throws SQLException {
		m_resultset.updateRowId(columnLabel, x);
	}

	@Override
	public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
		m_resultset.updateSQLXML(columnIndex, xmlObject);
	}

	@Override
	public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
		m_resultset.updateSQLXML(columnLabel, xmlObject);
	}

	@Override
	public void updateShort(int columnIndex, short x) throws SQLException {
		m_resultset.updateShort(columnIndex, x);
	}

	@Override
	public void updateShort(String columnName, short x) throws SQLException {
		m_resultset.updateShort(columnName, x);
	}

	@Override
	public void updateString(int columnIndex, String x) throws SQLException {
		m_resultset.updateString(columnIndex, x);
	}

	@Override
	public void updateString(String columnName, String x) throws SQLException {
		m_resultset.updateString(columnName, x);
	}

	@Override
	public void updateTime(int columnIndex, Time x) throws SQLException {
		m_resultset.updateTime(columnIndex, x);
	}

	@Override
	public void updateTime(String columnName, Time x) throws SQLException {
		m_resultset.updateTime(columnName, x);
	}

	@Override
	public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
		m_resultset.updateTimestamp(columnIndex, x);
	}

	@Override
	public void updateTimestamp(String columnName, Timestamp x) throws SQLException {
		m_resultset.updateTimestamp(columnName, x);
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		return m_resultset.getWarnings();
	}

	@Override
	public void clearWarnings() throws SQLException {
		m_resultset.clearWarnings();
	}

	@Override
	public boolean wasNull() throws SQLException {
		return m_resultset.wasNull();
	}

	@Override
	public boolean isWrapperFor(Class<?> iface) throws SQLException {
		return m_resultset.isWrapperFor(iface);
	}

	@Override
	public <T> T unwrap(Class<T> iface) throws SQLException {
		return m_resultset.unwrap(iface);
	}
}
