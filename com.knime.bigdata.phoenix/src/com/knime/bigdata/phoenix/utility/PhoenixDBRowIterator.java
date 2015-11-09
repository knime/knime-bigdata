/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on 07.11.2015 by koetter
 */
package com.knime.bigdata.phoenix.utility;

import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.phoenix.schema.types.PhoenixArray;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.collection.CollectionCellFactory;
import org.knime.core.data.date.DateAndTimeCell;
import org.knime.core.data.def.BooleanCell.BooleanCellFactory;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.port.database.reader.DBReaderImpl;
import org.knime.core.node.port.database.reader.DBRowIteratorImpl;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PhoenixDBRowIterator extends DBRowIteratorImpl {

    /**
     * @param databaseReaderConnection
     * @param result
     * @param useDbRowId
     */
    PhoenixDBRowIterator(final DBReaderImpl databaseReaderConnection, final ResultSet result, final boolean useDbRowId) {
        super(databaseReaderConnection, result, useDbRowId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected DataCell readArray(final int i) throws SQLException {
        // treat arrays special
        final Array array = m_result.getArray(i + 1);
        if (wasNull() || array == null) {
            return DataType.getMissingCell();
        }
        final PhoenixArray pArray = (PhoenixArray)array;
        final int baseType = pArray.getBaseType();
        final Collection<DataCell> cells;
        switch (baseType) {
            case Types.BIT:
            case Types.BOOLEAN:
                cells = getBooleanCells(pArray);
                break;
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                cells = getIntegerCells(pArray);
                break;
            case Types.BIGINT:
                cells = getLongCells(pArray);
                break;
            case Types.REAL:
            case Types.FLOAT:
            case Types.DOUBLE:
            case Types.DECIMAL:
            case Types.NUMERIC:
                cells = getDoubleCells(pArray);
                break;
            case Types.DATE:
                cells = getDateCells(pArray);
                break;
            case Types.TIME:
                cells = getTimeCells(pArray);
                break;
            case Types.TIMESTAMP:
                cells = getTimestampCells(pArray);
                break;
            default:
                cells = getStringCells(pArray);
        }
        return CollectionCellFactory.createListCell(cells);
    }

    /**
     * @param pArray
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getTimestampCells(final PhoenixArray pArray) throws SQLException {
    	//TODO:ADD TIMEZONE HANDLING
        final Timestamp[] vals = (Timestamp[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Timestamp val : vals) {
            cells.add(val == null ? DataType.getMissingCell() :
                new DateAndTimeCell(val.getTime(), true, true, true));
        }
        return cells;
    }

    /**
     * @param pArray
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getTimeCells(final PhoenixArray pArray) throws SQLException {
        //TODO:ADD TIMEZONE HANDLING
        final Time[] vals = (Time[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Time val : vals) {
            cells.add(val == null ? DataType.getMissingCell() :
                new DateAndTimeCell(val.getTime(), true, true, false));
        }
        return cells;
    }

    /**
     * @param pArray
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getDateCells(final PhoenixArray pArray) throws SQLException {
        //TODO:ADD TIMEZONE HANDLING
        final Date[] vals = (Date[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Date val : vals) {
            cells.add(val == null ? DataType.getMissingCell() :
                new DateAndTimeCell(val.getTime(), true, false, false));
        }
        return cells;
    }

    /**
     * @param pArray
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getLongCells(final PhoenixArray pArray) throws SQLException {
        final long[] vals = (long[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Long val : vals) {
            cells.add(val == null ? DataType.getMissingCell() : new LongCell(val));
        }
        return cells;
    }

    /**
     * @param pArray
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getIntegerCells(final PhoenixArray pArray) throws SQLException {
        final int[] vals = (int[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Integer val : vals) {
            cells.add(val == null ? DataType.getMissingCell() : new IntCell(val));
        }
        return cells;
    }

    /**
     * @param pArray
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getBooleanCells(final PhoenixArray pArray) throws SQLException {
        final boolean[] vals = (boolean[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Boolean val : vals) {
            cells.add(val == null ? DataType.getMissingCell() : BooleanCellFactory.create(val));
        }
        return cells;
    }

    /**
     * @param vals
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getDoubleCells(final PhoenixArray pArray) throws SQLException {
        final double[] vals = (double[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Double val : vals) {
            cells.add(val == null ? DataType.getMissingCell() : new DoubleCell(val));
        }
        return cells;
    }

    /**
     * @param vals
     * @return
     */
    private Collection<DataCell> getStringCells(final PhoenixArray pArray) throws SQLException {
        final Object[] vals = (Object[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Object val : vals) {
            cells.add(val == null ? DataType.getMissingCell() : new StringCell(val.toString()));
        }
        return cells;
    }

}
