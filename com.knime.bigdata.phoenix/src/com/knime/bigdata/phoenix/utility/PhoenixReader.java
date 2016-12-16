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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;

import org.knime.core.data.DataCell;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.RowIterator;
import org.knime.core.data.blob.BinaryObjectCellFactory;
import org.knime.core.data.collection.ListCell;
import org.knime.core.data.date.DateAndTimeCell;
import org.knime.core.data.def.BooleanCell;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.data.def.IntCell;
import org.knime.core.data.def.LongCell;
import org.knime.core.data.def.StringCell;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.reader.DBReaderImpl;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class PhoenixReader extends DBReaderImpl {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(PhoenixReader.class);

    /**
     * @param conn
     */
    public PhoenixReader(final DatabaseQueryConnectionSettings conn) {
        super(conn);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected RowIterator createDBRowIterator(final DataTableSpec spec, final DatabaseQueryConnectionSettings conn,
        final BinaryObjectCellFactory blobFactory, final boolean useDbRowId, final ResultSet result) throws SQLException {
        return new PhoenixDBRowIterator(spec, conn, blobFactory, result, useDbRowId);
    }

    /**
     * {@inheritDoc}
     * @throws SQLException
     */
    @Override
    protected DataType getKNIMEType(final int type, final ResultSetMetaData meta, final int dbIdx) throws SQLException {
        if (Types.ARRAY == type) {
            //we need to treat arrays special
            final String typeName = meta.getColumnTypeName(dbIdx).toUpperCase();
            final DataType elementType;
            switch (typeName.replaceAll("\\s*ARRAY\\s*", "")) {
                case "INTEGER":
                case "TINYINT":
                case "SMALLINT":
                case "UNSIGNED_INT":
                case "UNSIGNED_TINYINT":
                case "UNSIGNED_SMALLINT":
                    elementType = IntCell.TYPE; break;
                case "BIGINT":
                case "UNSIGNED_LONG":
                    elementType = LongCell.TYPE; break;
                case "FLOAT":
                case "DOUBLE":
                case "DECIMAL":
                case "UNSIGNED_FLOAT":
                case "UNSIGNED_DOUBLE":
                    elementType = DoubleCell.TYPE; break;
                case "BOOLEAN":
                    elementType = BooleanCell.TYPE; break;
                case "TIME":
                case "DATE":
                case "TIMESTAMP":
                case "UNSIGNED_TIME":
                case "UNSIGNED_DATE":
                case "UNSIGNED_TIMESTAMP":
                    elementType = DateAndTimeCell.TYPE; break;
                case "VARCHAR":
                case "CHAR":
                    elementType = StringCell.TYPE; break;
                default:
                    LOGGER.debug("Using generic DataCell type for Phoenix type name: " + typeName);
                    elementType = DataType.getType(DataCell.class);
            }
            return ListCell.getCollectionType(elementType);
        }
        return super.getKNIMEType(type, meta, dbIdx);
    }
}
