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
 *   Created on 03.07.2015 by koetter
 */
package org.knime.bigdata.testing.node.create;

import java.sql.SQLException;
import java.util.Map;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.bigdata.hive.testing.TestingDatabaseConnectionSettingsFactory;
import org.knime.bigdata.spark.core.context.SparkContextIDScheme;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.local.testing.LocalHiveTestingConnectionSettingsFactory;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.inactive.InactiveBranchPortObject;
import org.knime.core.node.port.inactive.InactiveBranchPortObjectSpec;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Node model for the "Create Big Data Test Environment" node.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class CreateTestEnvironmentNodeModel extends AbstractCreateTestEnvironmentNodeModel {

    private DatabaseConnectionSettings m_dbSettings;

    /**
     * Constructor.
     */
    CreateTestEnvironmentNodeModel() {
        super(new PortType[]{}, new PortType[]{DatabaseConnectionPortObject.TYPE, ConnectionInformationPortObject.TYPE,
            SparkContextPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec createHivePortSpec(final SparkContextIDScheme sparkScheme,
        final Map<String, FlowVariable> flowVars) throws InvalidSettingsException {
        switch (sparkScheme) {
            case SPARK_LOCAL:
                m_dbSettings = null;
                if (isLocalSparkWithThriftserver(flowVars)) {
                    // Hive is only available *after* creating the local Spark context
                    return null;
                } else {
                    return InactiveBranchPortObjectSpec.INSTANCE;
                }
            case SPARK_JOBSERVER:
            case SPARK_LIVY:
                m_dbSettings = TestingDatabaseConnectionSettingsFactory.createHiveSettings(flowVars);
                return new DatabaseConnectionPortObjectSpec(m_dbSettings);
            default:
                throw new InvalidSettingsException("Spark context ID scheme not supported: " + sparkScheme);
        }
    }

    @Override
    protected PortObject createHivePort(final ExecutionContext exec, final SparkContextIDScheme sparkScheme,
        final Map<String, FlowVariable> flowVars) throws Exception {

        switch (sparkScheme) {
            case SPARK_LOCAL:
                if (isLocalSparkWithThriftserver(flowVars)) {
                    final DatabaseConnectionPortObject dbPortObject =
                        new DatabaseConnectionPortObject(new DatabaseConnectionPortObjectSpec(
                            LocalHiveTestingConnectionSettingsFactory.createConnectionSettings(flowVars)));
                    openHiveConnection(dbPortObject.getSpec(), exec.createSubProgress(0.1));
                    return dbPortObject;
                } else {
                    return InactiveBranchPortObject.INSTANCE;
                }
            case SPARK_JOBSERVER:
            case SPARK_LIVY:
                return new DatabaseConnectionPortObject(new DatabaseConnectionPortObjectSpec(m_dbSettings));
            default:
                throw new InvalidSettingsException("Spark context ID scheme not supported: " + sparkScheme);
        }
    }

    private void openHiveConnection(DatabaseConnectionPortObjectSpec spec, ExecutionMonitor exec)
        throws InvalidSettingsException, SQLException {
        exec.setProgress(0, "Opening Hive connection");
        try {
            spec.getConnectionSettings(getCredentialsProvider()).execute(getCredentialsProvider(), conn -> {
                return conn != null;
            });
            exec.setProgress(1);
        } catch (SQLException ex) {
            Throwable cause = ExceptionUtils.getRootCause(ex);
            if (cause == null || cause.getMessage() == null) {
                cause = ex;
            }

            throw new SQLException("Could not create connection to database: " + cause.getMessage(), ex);
        }
    }
}
