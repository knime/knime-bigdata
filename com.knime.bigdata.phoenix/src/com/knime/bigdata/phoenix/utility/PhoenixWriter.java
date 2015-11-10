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
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Spliterator;
import java.util.TimeZone;
import java.util.function.Consumer;

import org.knime.core.data.BooleanValue;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.IntValue;
import org.knime.core.data.LongValue;
import org.knime.core.data.collection.CollectionDataValue;
import org.knime.core.data.date.DateAndTimeValue;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.writer.DBWriterImpl;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PhoenixWriter extends DBWriterImpl {

    /**
     * @param dbConn
     */
    public PhoenixWriter(final DatabaseConnectionSettings dbConn) {
        super(dbConn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected String createInsertStatment(final String table, final String columnNames, final int[] mapping,
        final boolean insertNullForMissingCols) {
        // // creates the wild card string based on the number of columns
        // this string it used every time an new row is inserted into the db
        final StringBuilder wildcard = new StringBuilder("(");
        boolean first = true;
        for (int i = 0; i < mapping.length; i++) {
            if (mapping[i] >= 0 || insertNullForMissingCols) {
                    //insert only a ? if the column is available in the input table or the insert null for missing
                    //columns option is enabled
                if (first) {
                    first = false;
                } else {
                    wildcard.append(", ");
                }
                wildcard.append("?");
            }
        }
        wildcard.append(")");
        // create table meta data with empty column information
        final String query = "UPSERT INTO " + table + " " + columnNames + " VALUES " + wildcard;
        return query;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void fillArray(final PreparedStatement stmt, final int dbIdx, final DataCell cell, final TimeZone tz) throws SQLException {
        if (cell.isMissing()) {
            stmt.setNull(dbIdx, Types.ARRAY);
        } else {
            final CollectionDataValue collection = (CollectionDataValue)cell;
            final Spliterator<DataCell> spliterator = collection.spliterator();
            final DataType baseType = cell.getType().getCollectionElementType();
            final Object[] vals = new Object[collection.size()];
            final String type;
            if (baseType.isCompatible(BooleanValue.class)) {
                type = "BOOLEAN";
                spliterator.forEachRemaining(new Consumer<DataCell>() {
                    int idx = 0;
                    @Override
                    public void accept(final DataCell t) {
                        vals[idx++] = t.isMissing() ? null : ((BooleanValue)t).getBooleanValue();
                    }
                });
            } else if (baseType.isCompatible(IntValue.class)) {
                type = "INTEGER";
                spliterator.forEachRemaining(new Consumer<DataCell>() {
                    int idx = 0;
                    @Override
                    public void accept(final DataCell t) {
                        vals[idx++] = t.isMissing() ? null : ((IntValue)t).getIntValue();
                    }
                });
            } else if (baseType.isCompatible(LongValue.class)) {
                type = "BIGINT";
                spliterator.forEachRemaining(new Consumer<DataCell>() {
                    int idx = 0;
                    @Override
                    public void accept(final DataCell t) {
                        vals[idx++] = t.isMissing() ? null : ((LongValue)t).getLongValue();
                    }
                });
            } else if (baseType.isCompatible(DoubleValue.class)) {
                type = "DOUBLE";
                spliterator.forEachRemaining(new Consumer<DataCell>() {
                    int idx = 0;
                    @Override
                    public void accept(final DataCell t) {
                        if (t.isMissing()) {
                            vals[idx++] = null;
                        } else {
                            final double value = ((DoubleValue)t).getDoubleValue();
                            vals[idx++] = Double.isNaN(value) ? null : value;
                        }
                    }
                });
            } else if (baseType.isCompatible(DateAndTimeValue.class)) {
                type = "TIMESTAMP";
                spliterator.forEachRemaining(new Consumer<DataCell>() {
                    int idx = 0;
                    @Override
                    public void accept(final DataCell t) {
                        if (t.isMissing()) {
                            vals[idx++] = null;
                        } else {
                            final DateAndTimeValue dateCell = (DateAndTimeValue) t;
                            final long corrDate =
                                    dateCell.getUTCTimeInMillis() - tz.getOffset(dateCell.getUTCTimeInMillis());
                            vals[idx++] = new java.sql.Timestamp(corrDate);
                        }
                    }
                });
            } else {
                type = "VARCHAR";
                spliterator.forEachRemaining(new Consumer<DataCell>() {
                    int idx = 0;
                    @Override
                    public void accept(final DataCell t) {
                        vals[idx++] = t.isMissing() ? null : t.toString();
                    }
                });
            }
            @SuppressWarnings("resource") //will be closed by the framework
            final Connection conn = stmt.getConnection();
            final Array array = conn.createArrayOf(type, vals);
            stmt.setArray(dbIdx, array);
        }
    }
}
