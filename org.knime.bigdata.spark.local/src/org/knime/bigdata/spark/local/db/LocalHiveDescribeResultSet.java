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
 *   Created on Jun 20, 2019 by mareike
 */
package org.knime.bigdata.spark.local.db;

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
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * This class converts the Local ThriftServer Result Set for the DESCRIBE FORMATTED Query in the form of the usual Hive
 * for the list of columns. It parses the original result and overwrites the
 * {@link LocalHiveDescribeResultSet#getString(int)} and {@link LocalHiveDescribeResultSet#next()} methods.
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 *
 */
public class LocalHiveDescribeResultSet implements ResultSet {

    private final ResultSet m_resultSet;

    private final List<String> m_namesList = new ArrayList<>();

    private final List<String> m_typesList = new ArrayList<>();

    private int m_position = -1;


    /**
     * The ResultSet from the local Server differs in to points: 1. It misses a leading "# col_name" 2. The column name
     * list also includes the partitioning columns The constructor of the LocalHiveDescribeResultSet parses the result
     * and holds all necessary information in Queues
     *
     * @param resultSet the original result from the local server
     * @throws SQLException
     */
    public LocalHiveDescribeResultSet(final ResultSet resultSet) throws SQLException {
        m_resultSet = resultSet;
        final List<String> partitionNames = new ArrayList<>();
        m_namesList.add("# col_name");
        m_typesList.add("");
        //First search the normal columns
        while (resultSet.next()) {
            final String name = resultSet.getString(1);
            if (name.startsWith("#")) {
                partitionNames.add(name);
                m_typesList.add("");
                break;
            } else {
                if (!name.isEmpty()) {
                    m_namesList.add(name);
                    m_typesList.add(resultSet.getString(2));
                }
            }
        }
        //Now search for the partition columns
        if (partitionNames.get(0).startsWith("# Partition Information")) {
            while (resultSet.next()) {
                final String name = resultSet.getString(1);
                if (name.startsWith("#")) {
                    partitionNames.add(name);
                    m_typesList.add("");
                    if (!name.startsWith("# col_name")) {
                        break;
                    }
                } else {
                    if (!name.isEmpty()) {
                        //We have to remove the partitioning column from the list of normal columns names and types
                        final int index = m_namesList.lastIndexOf(name);
                        m_namesList.remove(index);
                        m_typesList.remove(index);
                        partitionNames.add(name);
                        m_typesList.add(resultSet.getString(2));
                    }
                }
            }
        }

        m_namesList.addAll(partitionNames);

    }

    @Override
    public int hashCode() {
        return m_resultSet.hashCode();
    }

    @Override
    public boolean absolute(final int row) throws SQLException {
        return m_resultSet.absolute(row);
    }

    @Override
    public void afterLast() throws SQLException {
        m_resultSet.afterLast();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        m_resultSet.cancelRowUpdates();
    }

    @Override
    public void deleteRow() throws SQLException {
        m_resultSet.deleteRow();
    }

    @Override
    public int findColumn(final String columnName) throws SQLException {
        return m_resultSet.findColumn(columnName);
    }

    @Override
    public boolean first() throws SQLException {
        return m_resultSet.first();
    }

    @Override
    public Array getArray(final int i) throws SQLException {
        return m_resultSet.getArray(i);
    }

    @Override
    public boolean equals(final Object obj) {
        return m_resultSet.equals(obj);
    }

    @Override
    public Array getArray(final String colName) throws SQLException {
        return m_resultSet.getArray(colName);
    }

    @Override
    public InputStream getAsciiStream(final int columnIndex) throws SQLException {
        return m_resultSet.getAsciiStream(columnIndex);
    }

    @Override
    public InputStream getAsciiStream(final String columnName) throws SQLException {
        return m_resultSet.getAsciiStream(columnName);
    }

    @Override
    public BigDecimal getBigDecimal(final int columnIndex) throws SQLException {
        return m_resultSet.getBigDecimal(columnIndex);
    }

    @Override
    public BigDecimal getBigDecimal(final String columnName) throws SQLException {
        return m_resultSet.getBigDecimal(columnName);
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(final int columnIndex, final int scale) throws SQLException {
        return m_resultSet.getBigDecimal(columnIndex, scale);
    }

    @Deprecated
    @Override
    public BigDecimal getBigDecimal(final String columnName, final int scale) throws SQLException {
        return m_resultSet.getBigDecimal(columnName, scale);
    }

    @Override
    public InputStream getBinaryStream(final int columnIndex) throws SQLException {
        return m_resultSet.getBinaryStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(final String columnName) throws SQLException {
        return m_resultSet.getBinaryStream(columnName);
    }

    @Override
    public Blob getBlob(final int i) throws SQLException {
        return m_resultSet.getBlob(i);
    }

    @Override
    public Blob getBlob(final String colName) throws SQLException {
        return m_resultSet.getBlob(colName);
    }

    @Override
    public boolean getBoolean(final int columnIndex) throws SQLException {
        return m_resultSet.getBoolean(columnIndex);
    }

    @Override
    public boolean getBoolean(final String columnName) throws SQLException {
        return m_resultSet.getBoolean(columnName);
    }

    @Override
    public byte getByte(final int columnIndex) throws SQLException {
        return m_resultSet.getByte(columnIndex);
    }

    @Override
    public byte getByte(final String columnName) throws SQLException {
        return m_resultSet.getByte(columnName);
    }

    @Override
    public byte[] getBytes(final int columnIndex) throws SQLException {
        return m_resultSet.getBytes(columnIndex);
    }

    @Override
    public byte[] getBytes(final String columnName) throws SQLException {
        return m_resultSet.getBytes(columnName);
    }

    @Override
    public Reader getCharacterStream(final int columnIndex) throws SQLException {
        return m_resultSet.getCharacterStream(columnIndex);
    }

    @Override
    public Reader getCharacterStream(final String columnName) throws SQLException {
        return m_resultSet.getCharacterStream(columnName);
    }

    @Override
    public Clob getClob(final int i) throws SQLException {
        return m_resultSet.getClob(i);
    }

    @Override
    public Clob getClob(final String colName) throws SQLException {
        return m_resultSet.getClob(colName);
    }

    @Override
    public int getConcurrency() throws SQLException {
        return m_resultSet.getConcurrency();
    }

    @Override
    public String getCursorName() throws SQLException {
        return m_resultSet.getCursorName();
    }

    @Override
    public Date getDate(final int columnIndex) throws SQLException {
        return m_resultSet.getDate(columnIndex);
    }

    @Override
    public Date getDate(final String columnName) throws SQLException {
        return m_resultSet.getDate(columnName);
    }

    @Override
    public Date getDate(final int columnIndex, final Calendar cal) throws SQLException {
        return m_resultSet.getDate(columnIndex, cal);
    }

    @Override
    public Date getDate(final String columnName, final Calendar cal) throws SQLException {
        return m_resultSet.getDate(columnName, cal);
    }

    @Override
    public double getDouble(final int columnIndex) throws SQLException {
        return m_resultSet.getDouble(columnIndex);
    }

    @Override
    public String toString() {
        return m_resultSet.toString();
    }

    @Override
    public double getDouble(final String columnName) throws SQLException {
        return m_resultSet.getDouble(columnName);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return m_resultSet.getFetchDirection();
    }

    @Override
    public float getFloat(final int columnIndex) throws SQLException {
        return m_resultSet.getFloat(columnIndex);
    }

    @Override
    public float getFloat(final String columnName) throws SQLException {
        return m_resultSet.getFloat(columnName);
    }

    @Override
    public int getHoldability() throws SQLException {
        return m_resultSet.getHoldability();
    }

    @Override
    public int getInt(final int columnIndex) throws SQLException {
        return m_resultSet.getInt(columnIndex);
    }

    @Override
    public void close() throws SQLException {
        m_resultSet.close();
    }

    @Override
    public int getInt(final String columnName) throws SQLException {
        return m_resultSet.getInt(columnName);
    }

    @Override
    public long getLong(final int columnIndex) throws SQLException {
        return m_resultSet.getLong(columnIndex);
    }

    @Override
    public long getLong(final String columnName) throws SQLException {
        return m_resultSet.getLong(columnName);
    }

    @Override
    public boolean next() throws SQLException {
        m_position++;
        if (!(m_position >= m_namesList.size())) {
            return true;
        }
        return m_resultSet.next();
    }

    @Override
    public Reader getNCharacterStream(final int arg0) throws SQLException {
        return m_resultSet.getNCharacterStream(arg0);
    }

    @Override
    public Reader getNCharacterStream(final String arg0) throws SQLException {
        return m_resultSet.getNCharacterStream(arg0);
    }

    @Override
    public NClob getNClob(final int arg0) throws SQLException {
        return m_resultSet.getNClob(arg0);
    }

    @Override
    public NClob getNClob(final String columnLabel) throws SQLException {
        return m_resultSet.getNClob(columnLabel);
    }

    @Override
    public String getNString(final int columnIndex) throws SQLException {
        return m_resultSet.getNString(columnIndex);
    }

    @Override
    public String getNString(final String columnLabel) throws SQLException {
        return m_resultSet.getNString(columnLabel);
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return m_resultSet.getMetaData();
    }

    @Override
    public void setFetchSize(final int rows) throws SQLException {
        m_resultSet.setFetchSize(rows);
    }

    @Override
    public int getType() throws SQLException {
        return m_resultSet.getType();
    }

    @Override
    public Object getObject(final int columnIndex) throws SQLException {
        return m_resultSet.getObject(columnIndex);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return m_resultSet.getFetchSize();
    }

    @Override
    public Object getObject(final String columnName) throws SQLException {
        return m_resultSet.getObject(columnName);
    }

    @Override
    public <T> T getObject(final String columnLabel, final Class<T> type) throws SQLException {
        return m_resultSet.getObject(columnLabel, type);
    }

    @Override
    public Object getObject(final int i, final Map<String, Class<?>> map) throws SQLException {
        return m_resultSet.getObject(i, map);
    }

    @Override
    public <T> T getObject(final int columnIndex, final Class<T> type) throws SQLException {
        return m_resultSet.getObject(columnIndex, type);
    }

    @Override
    public Object getObject(final String colName, final Map<String, Class<?>> map) throws SQLException {
        return m_resultSet.getObject(colName, map);
    }

    @Override
    public void beforeFirst() throws SQLException {
        m_resultSet.beforeFirst();
    }

    @Override
    public Ref getRef(final int i) throws SQLException {
        return m_resultSet.getRef(i);
    }

    @Override
    public Ref getRef(final String colName) throws SQLException {
        return m_resultSet.getRef(colName);
    }

    @Override
    public RowId getRowId(final int columnIndex) throws SQLException {
        return m_resultSet.getRowId(columnIndex);
    }

    @Override
    public RowId getRowId(final String columnLabel) throws SQLException {
        return m_resultSet.getRowId(columnLabel);
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        return m_resultSet.isBeforeFirst();
    }

    @Override
    public SQLXML getSQLXML(final int columnIndex) throws SQLException {
        return m_resultSet.getSQLXML(columnIndex);
    }

    @Override
    public int getRow() throws SQLException {
        return m_resultSet.getRow();
    }

    @Override
    public SQLXML getSQLXML(final String columnLabel) throws SQLException {
        return m_resultSet.getSQLXML(columnLabel);
    }

    @Override
    public short getShort(final int columnIndex) throws SQLException {
        return m_resultSet.getShort(columnIndex);
    }

    @Override
    public short getShort(final String columnName) throws SQLException {
        return m_resultSet.getShort(columnName);
    }

    @Override
    public Statement getStatement() throws SQLException {
        return m_resultSet.getStatement();
    }

    @Override
    public String getString(final int columnIndex) throws SQLException {
        if (m_position >= m_namesList.size()) {
            return m_resultSet.getString(columnIndex);
        }
        if (columnIndex == 1) {
            return m_namesList.get(m_position);
        } else if (columnIndex == 2) {
            return m_typesList.get(m_position);
        } else {
            throw new ArrayIndexOutOfBoundsException();
        }
    }

    @Override
    public String getString(final String columnName) throws SQLException {
        return m_resultSet.getString(columnName);
    }

    @Override
    public Time getTime(final int columnIndex) throws SQLException {
        return m_resultSet.getTime(columnIndex);
    }

    @Override
    public Time getTime(final String columnName) throws SQLException {
        return m_resultSet.getTime(columnName);
    }

    @Override
    public Time getTime(final int columnIndex, final Calendar cal) throws SQLException {
        return m_resultSet.getTime(columnIndex, cal);
    }

    @Override
    public Time getTime(final String columnName, final Calendar cal) throws SQLException {
        return m_resultSet.getTime(columnName, cal);
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex) throws SQLException {
        return m_resultSet.getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(final String columnName) throws SQLException {
        return m_resultSet.getTimestamp(columnName);
    }

    @Override
    public Timestamp getTimestamp(final int columnIndex, final Calendar cal) throws SQLException {
        return m_resultSet.getTimestamp(columnIndex, cal);
    }

    @Override
    public Timestamp getTimestamp(final String columnName, final Calendar cal) throws SQLException {
        return m_resultSet.getTimestamp(columnName, cal);
    }

    @Override
    public URL getURL(final int columnIndex) throws SQLException {
        return m_resultSet.getURL(columnIndex);
    }

    @Override
    public URL getURL(final String columnName) throws SQLException {
        return m_resultSet.getURL(columnName);
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(final int columnIndex) throws SQLException {
        return m_resultSet.getUnicodeStream(columnIndex);
    }

    @Deprecated
    @Override
    public InputStream getUnicodeStream(final String columnName) throws SQLException {
        return m_resultSet.getUnicodeStream(columnName);
    }

    @Override
    public void insertRow() throws SQLException {
        m_resultSet.insertRow();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        return m_resultSet.isAfterLast();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return m_resultSet.isClosed();
    }

    @Override
    public boolean isFirst() throws SQLException {
        return m_resultSet.isFirst();
    }

    @Override
    public boolean isLast() throws SQLException {
        return m_resultSet.isLast();
    }

    @Override
    public boolean last() throws SQLException {
        return m_resultSet.last();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        m_resultSet.moveToCurrentRow();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        m_resultSet.moveToInsertRow();
    }

    @Override
    public boolean previous() throws SQLException {
        return m_resultSet.previous();
    }

    @Override
    public void refreshRow() throws SQLException {
        m_resultSet.refreshRow();
    }

    @Override
    public boolean relative(final int rows) throws SQLException {
        return m_resultSet.relative(rows);
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return m_resultSet.rowDeleted();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return m_resultSet.rowInserted();
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return m_resultSet.rowUpdated();
    }

    @Override
    public void setFetchDirection(final int direction) throws SQLException {
        m_resultSet.setFetchDirection(direction);
    }

    @Override
    public void updateArray(final int columnIndex, final Array x) throws SQLException {
        m_resultSet.updateArray(columnIndex, x);
    }

    @Override
    public void updateArray(final String columnName, final Array x) throws SQLException {
        m_resultSet.updateArray(columnName, x);
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x) throws SQLException {
        m_resultSet.updateAsciiStream(columnIndex, x);
    }

    @Override
    public void updateAsciiStream(final String columnLabel, final InputStream x) throws SQLException {
        m_resultSet.updateAsciiStream(columnLabel, x);
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x, final int length) throws SQLException {
        m_resultSet.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateAsciiStream(final String columnName, final InputStream x, final int length) throws SQLException {
        m_resultSet.updateAsciiStream(columnName, x, length);
    }

    @Override
    public void updateAsciiStream(final int columnIndex, final InputStream x, final long length) throws SQLException {
        m_resultSet.updateAsciiStream(columnIndex, x, length);
    }

    @Override
    public void updateAsciiStream(final String columnLabel, final InputStream x, final long length)
            throws SQLException {
        m_resultSet.updateAsciiStream(columnLabel, x, length);
    }

    @Override
    public void updateBigDecimal(final int columnIndex, final BigDecimal x) throws SQLException {
        m_resultSet.updateBigDecimal(columnIndex, x);
    }

    @Override
    public void updateBigDecimal(final String columnName, final BigDecimal x) throws SQLException {
        m_resultSet.updateBigDecimal(columnName, x);
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x) throws SQLException {
        m_resultSet.updateBinaryStream(columnIndex, x);
    }

    @Override
    public void updateBinaryStream(final String columnLabel, final InputStream x) throws SQLException {
        m_resultSet.updateBinaryStream(columnLabel, x);
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x, final int length) throws SQLException {
        m_resultSet.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(final String columnName, final InputStream x, final int length) throws SQLException {
        m_resultSet.updateBinaryStream(columnName, x, length);
    }

    @Override
    public void updateBinaryStream(final int columnIndex, final InputStream x, final long length) throws SQLException {
        m_resultSet.updateBinaryStream(columnIndex, x, length);
    }

    @Override
    public void updateBinaryStream(final String columnLabel, final InputStream x, final long length)
            throws SQLException {
        m_resultSet.updateBinaryStream(columnLabel, x, length);
    }

    @Override
    public void updateBlob(final int columnIndex, final Blob x) throws SQLException {
        m_resultSet.updateBlob(columnIndex, x);
    }

    @Override
    public void updateBlob(final String columnName, final Blob x) throws SQLException {
        m_resultSet.updateBlob(columnName, x);
    }

    @Override
    public void updateBlob(final int columnIndex, final InputStream inputStream) throws SQLException {
        m_resultSet.updateBlob(columnIndex, inputStream);
    }

    @Override
    public void updateBlob(final String columnLabel, final InputStream inputStream) throws SQLException {
        m_resultSet.updateBlob(columnLabel, inputStream);
    }

    @Override
    public void updateBlob(final int columnIndex, final InputStream inputStream, final long length)
            throws SQLException {
        m_resultSet.updateBlob(columnIndex, inputStream, length);
    }

    @Override
    public void updateBlob(final String columnLabel, final InputStream inputStream, final long length)
            throws SQLException {
        m_resultSet.updateBlob(columnLabel, inputStream, length);
    }

    @Override
    public void updateBoolean(final int columnIndex, final boolean x) throws SQLException {
        m_resultSet.updateBoolean(columnIndex, x);
    }

    @Override
    public void updateBoolean(final String columnName, final boolean x) throws SQLException {
        m_resultSet.updateBoolean(columnName, x);
    }

    @Override
    public void updateByte(final int columnIndex, final byte x) throws SQLException {
        m_resultSet.updateByte(columnIndex, x);
    }

    @Override
    public void updateByte(final String columnName, final byte x) throws SQLException {
        m_resultSet.updateByte(columnName, x);
    }

    @Override
    public void updateBytes(final int columnIndex, final byte[] x) throws SQLException {
        m_resultSet.updateBytes(columnIndex, x);
    }

    @Override
    public void updateBytes(final String columnName, final byte[] x) throws SQLException {
        m_resultSet.updateBytes(columnName, x);
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x) throws SQLException {
        m_resultSet.updateCharacterStream(columnIndex, x);
    }

    @Override
    public void updateCharacterStream(final String columnLabel, final Reader reader) throws SQLException {
        m_resultSet.updateCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x, final int length) throws SQLException {
        m_resultSet.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(final String columnName, final Reader reader, final int length)
            throws SQLException {
        m_resultSet.updateCharacterStream(columnName, reader, length);
    }

    @Override
    public void updateCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {
        m_resultSet.updateCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateCharacterStream(final String columnLabel, final Reader reader, final long length)
            throws SQLException {
        m_resultSet.updateCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateClob(final int columnIndex, final Clob x) throws SQLException {
        m_resultSet.updateClob(columnIndex, x);
    }

    @Override
    public void updateClob(final String columnName, final Clob x) throws SQLException {
        m_resultSet.updateClob(columnName, x);
    }

    @Override
    public void updateClob(final int columnIndex, final Reader reader) throws SQLException {
        m_resultSet.updateClob(columnIndex, reader);
    }

    @Override
    public void updateClob(final String columnLabel, final Reader reader) throws SQLException {
        m_resultSet.updateClob(columnLabel, reader);
    }

    @Override
    public void updateClob(final int columnIndex, final Reader reader, final long length) throws SQLException {
        m_resultSet.updateClob(columnIndex, reader, length);
    }

    @Override
    public void updateClob(final String columnLabel, final Reader reader, final long length) throws SQLException {
        m_resultSet.updateClob(columnLabel, reader, length);
    }

    @Override
    public void updateDate(final int columnIndex, final Date x) throws SQLException {
        m_resultSet.updateDate(columnIndex, x);
    }

    @Override
    public void updateDate(final String columnName, final Date x) throws SQLException {
        m_resultSet.updateDate(columnName, x);
    }

    @Override
    public void updateDouble(final int columnIndex, final double x) throws SQLException {
        m_resultSet.updateDouble(columnIndex, x);
    }

    @Override
    public void updateDouble(final String columnName, final double x) throws SQLException {
        m_resultSet.updateDouble(columnName, x);
    }

    @Override
    public void updateFloat(final int columnIndex, final float x) throws SQLException {
        m_resultSet.updateFloat(columnIndex, x);
    }

    @Override
    public void updateFloat(final String columnName, final float x) throws SQLException {
        m_resultSet.updateFloat(columnName, x);
    }

    @Override
    public void updateInt(final int columnIndex, final int x) throws SQLException {
        m_resultSet.updateInt(columnIndex, x);
    }

    @Override
    public void updateInt(final String columnName, final int x) throws SQLException {
        m_resultSet.updateInt(columnName, x);
    }

    @Override
    public void updateLong(final int columnIndex, final long x) throws SQLException {
        m_resultSet.updateLong(columnIndex, x);
    }

    @Override
    public void updateLong(final String columnName, final long x) throws SQLException {
        m_resultSet.updateLong(columnName, x);
    }

    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x) throws SQLException {
        m_resultSet.updateNCharacterStream(columnIndex, x);
    }

    @Override
    public void updateNCharacterStream(final String columnLabel, final Reader reader) throws SQLException {
        m_resultSet.updateNCharacterStream(columnLabel, reader);
    }

    @Override
    public void updateNCharacterStream(final int columnIndex, final Reader x, final long length) throws SQLException {
        m_resultSet.updateNCharacterStream(columnIndex, x, length);
    }

    @Override
    public void updateNCharacterStream(final String columnLabel, final Reader reader, final long length)
            throws SQLException {
        m_resultSet.updateNCharacterStream(columnLabel, reader, length);
    }

    @Override
    public void updateNClob(final int columnIndex, final NClob clob) throws SQLException {
        m_resultSet.updateNClob(columnIndex, clob);
    }

    @Override
    public void updateNClob(final String columnLabel, final NClob clob) throws SQLException {
        m_resultSet.updateNClob(columnLabel, clob);
    }

    @Override
    public void updateNClob(final int columnIndex, final Reader reader) throws SQLException {
        m_resultSet.updateNClob(columnIndex, reader);
    }

    @Override
    public void updateNClob(final String columnLabel, final Reader reader) throws SQLException {
        m_resultSet.updateNClob(columnLabel, reader);
    }

    @Override
    public void updateNClob(final int columnIndex, final Reader reader, final long length) throws SQLException {
        m_resultSet.updateNClob(columnIndex, reader, length);
    }

    @Override
    public void updateNClob(final String columnLabel, final Reader reader, final long length) throws SQLException {
        m_resultSet.updateNClob(columnLabel, reader, length);
    }

    @Override
    public void updateNString(final int columnIndex, final String string) throws SQLException {
        m_resultSet.updateNString(columnIndex, string);
    }

    @Override
    public void updateNString(final String columnLabel, final String string) throws SQLException {
        m_resultSet.updateNString(columnLabel, string);
    }

    @Override
    public void updateNull(final int columnIndex) throws SQLException {
        m_resultSet.updateNull(columnIndex);
    }

    @Override
    public void updateNull(final String columnName) throws SQLException {
        m_resultSet.updateNull(columnName);
    }

    @Override
    public void updateObject(final int columnIndex, final Object x) throws SQLException {
        m_resultSet.updateObject(columnIndex, x);
    }

    @Override
    public void updateObject(final String columnName, final Object x) throws SQLException {
        m_resultSet.updateObject(columnName, x);
    }

    @Override
    public void updateObject(final int columnIndex, final Object x, final int scale) throws SQLException {
        m_resultSet.updateObject(columnIndex, x, scale);
    }

    @Override
    public void updateObject(final String columnName, final Object x, final int scale) throws SQLException {
        m_resultSet.updateObject(columnName, x, scale);
    }

    @Override
    public void updateRef(final int columnIndex, final Ref x) throws SQLException {
        m_resultSet.updateRef(columnIndex, x);
    }

    @Override
    public void updateRef(final String columnName, final Ref x) throws SQLException {
        m_resultSet.updateRef(columnName, x);
    }

    @Override
    public void updateRow() throws SQLException {
        m_resultSet.updateRow();
    }

    @Override
    public void updateRowId(final int columnIndex, final RowId x) throws SQLException {
        m_resultSet.updateRowId(columnIndex, x);
    }

    @Override
    public void updateRowId(final String columnLabel, final RowId x) throws SQLException {
        m_resultSet.updateRowId(columnLabel, x);
    }

    @Override
    public void updateSQLXML(final int columnIndex, final SQLXML xmlObject) throws SQLException {
        m_resultSet.updateSQLXML(columnIndex, xmlObject);
    }

    @Override
    public void updateSQLXML(final String columnLabel, final SQLXML xmlObject) throws SQLException {
        m_resultSet.updateSQLXML(columnLabel, xmlObject);
    }

    @Override
    public void updateShort(final int columnIndex, final short x) throws SQLException {
        m_resultSet.updateShort(columnIndex, x);
    }

    @Override
    public void updateShort(final String columnName, final short x) throws SQLException {
        m_resultSet.updateShort(columnName, x);
    }

    @Override
    public void updateString(final int columnIndex, final String x) throws SQLException {
        m_resultSet.updateString(columnIndex, x);
    }

    @Override
    public void updateString(final String columnName, final String x) throws SQLException {
        m_resultSet.updateString(columnName, x);
    }

    @Override
    public void updateTime(final int columnIndex, final Time x) throws SQLException {
        m_resultSet.updateTime(columnIndex, x);
    }

    @Override
    public void updateTime(final String columnName, final Time x) throws SQLException {
        m_resultSet.updateTime(columnName, x);
    }

    @Override
    public void updateTimestamp(final int columnIndex, final Timestamp x) throws SQLException {
        m_resultSet.updateTimestamp(columnIndex, x);
    }

    @Override
    public void updateTimestamp(final String columnName, final Timestamp x) throws SQLException {
        m_resultSet.updateTimestamp(columnName, x);
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return m_resultSet.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        m_resultSet.clearWarnings();
    }

    @Override
    public boolean wasNull() throws SQLException {
        return m_resultSet.wasNull();
    }

    @Override
    public boolean isWrapperFor(final Class<?> iface) throws SQLException {
        return m_resultSet.isWrapperFor(iface);
    }

    @Override
    public <T> T unwrap(final Class<T> iface) throws SQLException {
        return m_resultSet.unwrap(iface);
    }

}
