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
 */
package org.knime.bigdata.spark.node.io.database.impala.writer;

import java.sql.SQLException;

import org.knime.bigdata.database.impala.Impala;
import org.knime.bigdata.spark.node.io.database.hive.writer.DBSpark2HiveNodeModel;
import org.knime.bigdata.spark.node.io.hive.writer.FileFormat;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.database.DBType;
import org.knime.database.SQLCommand;
import org.knime.database.agent.ddl.DBStructureManipulator;
import org.knime.database.model.DBTable;
import org.knime.database.port.DBSessionPortObjectSpec;
import org.knime.database.session.DBSession;

/**
 * Node to import data from Impala via Hive in Spark.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class DBSpark2ImpalaNodeModel extends DBSpark2HiveNodeModel {

    @Override
    protected void checkDatabaseIdentifier(final DBSessionPortObjectSpec spec) throws InvalidSettingsException {
        final DBType dbType = spec.getDBSession().getDBType();
        if (Impala.DB_TYPE != dbType) {
            throw new InvalidSettingsException("Input must be a Impala connection");
        }
    }

    @Override
    protected FileFormat getDefaultFormat() {
        return FileFormat.PARQUET;
    }

    /**
     * Data gets imported via Hive. Invalidate meta data makes the table visible to Impala.
     *
     * @throws InvalidSettingsException
     */
    @Override
    protected void postProcessing(final DBSession session, final DBTable table, final ExecutionContext exec)
        throws CanceledExecutionException, InvalidSettingsException{

        try {
            exec.setMessage("Invalidating impala metadata");
            final String tableName = session.getDialect().createFullName(table);
            final SQLCommand[] statements = new SQLCommand[] {
                new SQLCommand("INVALIDATE METADATA " + tableName),
                new SQLCommand("DESCRIBE " + tableName)
            };
            session.getAgent(DBStructureManipulator.class).executeStatements(exec, statements);
        } catch (SQLException e) {
            throw new InvalidSettingsException("During PostProcessing: " + e.getMessage());
        }
    }
}
