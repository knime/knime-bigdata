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
 *   Created on 23.06.2016 by oole
 */
package com.knime.bigdata.spark.node.io.impala.writer;

import java.sql.Connection;
import java.sql.SQLException;

import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;

import com.knime.bigdata.impala.utility.ImpalaUtility;
import com.knime.bigdata.spark.node.io.hive.writer.Spark2HiveNodeModel;

/**
 *
 * @author Ole Ostergaard, KNIME.com
 */
public class Spark2ImpalaNodeModel extends Spark2HiveNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Spark2ImpalaNodeModel.class);

    @Override
    protected void checkDatabaseIdentifier(final DatabaseConnectionPortObjectSpec spec) throws InvalidSettingsException {
        if (!ImpalaUtility.DATABASE_IDENTIFIER.equals(spec.getDatabaseIdentifier())) {
            throw new InvalidSettingsException("Input must be a Impala connection");
        }
    }

    /**
     * Metadata is held in memory, thus after creation of the impala table an "INVALIDATE METADATA table_name" statement has to
     * to be issued to make the table visible in Impala.
     * @throws SQLException
     */
    @Override
    protected void postProcessing(final Connection connection, final String tableName, final ExecutionContext exec) throws SQLException {

        exec.setMessage("Invalidating impala metadata");
        final String invalidateStatement = "INVALIDATE METADATA " + tableName;
        LOGGER.info("Executing: \"" + invalidateStatement + "\"");
        connection.createStatement().execute(invalidateStatement);
        final String describeStatement = "DESCRIBE " + tableName;
        LOGGER.info("Executing: \"" + describeStatement + "\"");
        connection.createStatement().execute(describeStatement);
    }
}
