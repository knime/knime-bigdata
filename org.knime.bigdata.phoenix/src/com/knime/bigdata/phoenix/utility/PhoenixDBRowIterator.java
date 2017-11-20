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
 *   Created on 07.11.2015 by koetter
 */
package com.knime.bigdata.phoenix.utility;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.blob.BinaryObjectCellFactory;
import org.knime.core.data.collection.CollectionCellFactory;
import org.knime.core.data.def.BooleanCell.BooleanCellFactory;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.reader.DBRowIteratorImpl;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PhoenixDBRowIterator extends DBRowIteratorImpl {

    /**
     * @param spec {@link DataTableSpec}
     * @param conn {@link DatabaseConnectionSettings}
     * @param blobFactory {@link BinaryObjectCellFactory}
     * @param result {@link ResultSet}
     * @param useDbRowId <code>true</code> if the db row id should be used
     */
    protected PhoenixDBRowIterator(final DataTableSpec spec, final DatabaseConnectionSettings conn,
        final BinaryObjectCellFactory blobFactory, final ResultSet result, final boolean useDbRowId) {
        super(spec, conn, blobFactory, result, useDbRowId);
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
        final int baseType = array.getBaseType();
        final Collection<DataCell> cells;
        switch (baseType) {
            case Types.BIT:
            case Types.BOOLEAN:
                cells = getBooleanCells(array);
                break;
            case Types.TINYINT:
                cells = getTinyIntegerCells(array);
                break;
            case Types.SMALLINT:
                cells = getSmallIntegerCells(array);
                break;
            case Types.INTEGER:
                cells = getIntegerCells(array);
                break;
            case Types.BIGINT:
                cells = getLongCells(array);
                break;
            case Types.FLOAT:
                cells = getFloatCells(array);
                break;
            case Types.REAL:
            case Types.DOUBLE:
                cells = getDoubleCells(array);
                break;
            case Types.NUMERIC:
                cells = getDoubleCells(array);
                break;
            case Types.DECIMAL:
                cells = getDecimalCells(array);
                break;
            case Types.DATE:
                cells = getDateCells(array);
                break;
            case Types.TIME:
                cells = getTimeCells(array);
                break;
            case Types.TIMESTAMP:
                cells = getTimestampCells(array);
                break;
            default:
                cells = getStringCells(array);
        }
        return CollectionCellFactory.createListCell(cells);
    }

    /**
     * @param pArray
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getTimestampCells(final Array pArray) throws SQLException {
        final Timestamp[] vals = (Timestamp[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Timestamp val : vals) {
            cells.add(val == null ? DataType.getMissingCell() :
                createDateCell(val, true, true, true));
        }
        return cells;
    }

    /**
     * @param pArray
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getTimeCells(final Array pArray) throws SQLException {
        final Time[] vals = (Time[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Time val : vals) {
            cells.add(val == null ? DataType.getMissingCell() :
                createDateCell(val, false, true, false));
        }
        return cells;
    }

    /**
     * @param pArray
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getDateCells(final Array pArray) throws SQLException {
        final Date[] vals = (Date[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Date val : vals) {
            cells.add(val == null ? DataType.getMissingCell() :
                createDateCell(val, true, false, false));
        }
        return cells;
    }

    /**
     * @param pArray
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getLongCells(final Array pArray) throws SQLException {
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
    private Collection<DataCell> getTinyIntegerCells(final Array pArray) throws SQLException {
        final byte[] vals = (byte[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Byte val : vals) {
            cells.add(val == null ? DataType.getMissingCell() : new IntCell(val));
        }
        return cells;
    }

    /**
     * @param pArray
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getSmallIntegerCells(final Array pArray) throws SQLException {
        final short[] vals = (short[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Short val : vals) {
            cells.add(val == null ? DataType.getMissingCell() : new IntCell(val));
        }
        return cells;
    }

    /**
     * @param pArray
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getIntegerCells(final Array pArray) throws SQLException {
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
    private Collection<DataCell> getBooleanCells(final Array pArray) throws SQLException {
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
    private Collection<DataCell> getDecimalCells(final Array pArray) throws SQLException {
        final BigDecimal[] vals = (BigDecimal[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (BigDecimal val : vals) {
            cells.add(val == null ? DataType.getMissingCell() : new DoubleCell(val.doubleValue()));
        }
        return cells;
    }

    /**
     * @param vals
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getFloatCells(final Array pArray) throws SQLException {
        final float[] vals = (float[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Float val : vals) {
            cells.add(val == null ? DataType.getMissingCell() : new DoubleCell(val));
        }
        return cells;
    }

    /**
     * @param vals
     * @return
     * @throws SQLException
     */
    private Collection<DataCell> getDoubleCells(final Array pArray) throws SQLException {
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
    private Collection<DataCell> getStringCells(final Array pArray) throws SQLException {
        final Object[] vals = (Object[])pArray.getArray();
        final Collection<DataCell>cells = new ArrayList<>(vals.length);
        for (Object val : vals) {
            cells.add(val == null ? DataType.getMissingCell() : new StringCell(val.toString()));
        }
        return cells;
    }

}
