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
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.bigdata.commons.testing.TestflowVariable;
import org.knime.bigdata.filehandling.local.HDFSLocalConnectionInformation;
import org.knime.bigdata.filehandling.testing.TestingConnectionInformationFactory;
import org.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;
import org.knime.bigdata.hive.testing.TestingDatabaseConnectionSettingsFactory;
import org.knime.bigdata.spark.core.context.SparkContext;
import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.context.testing.TestingSparkContextConfigFactory;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.context.SparkContextConfig;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObject;
import org.knime.bigdata.spark.core.port.context.SparkContextPortObjectSpec;
import org.knime.bigdata.spark.local.testing.LocalHiveTestingConnectionSettingsFactory;
import org.knime.bigdata.testing.FlowVariableReader;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.Node;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.inactive.InactiveBranchPortObject;
import org.knime.core.node.workflow.FlowVariable;

/**
 * Node model for the "Create Big Data Test Environment" node.
 *
 * @author Bjoern Lohrmann, KNIME GmbH
 */
public class CreateTestEnvironmentNodeModel extends SparkNodeModel {

    private DatabaseConnectionSettings m_dbSettings;

    private ConnectionInformation m_fsConnectionInfo;

    private SparkContextConfig m_sparkConfig;

    /**
     * Constructor.
     */
    CreateTestEnvironmentNodeModel() {
        super(new PortType[]{}, new PortType[]{DatabaseConnectionPortObject.TYPE, ConnectionInformationPortObject.TYPE,
            SparkContextPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final Map<String, FlowVariable> flowVars;
        try {
            flowVars = FlowVariableReader.readFromCsv();
            flowVars.values().stream().forEach(v -> Node.invokePushFlowVariable(this, v));
        } catch (Exception e) {
            throw new InvalidSettingsException("Failed to read flowvariables.csv: " + e.getMessage(), e);
        }

        final SparkContextConfig sparkConfig = TestingSparkContextConfigFactory.create(flowVars);
        configureSparkContext(sparkConfig);

        final DatabaseConnectionSettings dbSettings;
        final ConnectionInformation fsConnectionInfo;
        switch (sparkConfig.getSparkContextID().getScheme()) {
            case SPARK_LOCAL:
                dbSettings = null; // Hive is only available *after* creating the local Spark context
                fsConnectionInfo = HDFSLocalConnectionInformation.getInstance();
                break;
            case SPARK_JOBSERVER:
            case SPARK_LIVY:
                dbSettings = TestingDatabaseConnectionSettingsFactory.createHiveSettings(flowVars);
                fsConnectionInfo =
                    TestingConnectionInformationFactory.create(HDFSRemoteFileHandler.HTTPFS_PROTOCOL, flowVars);
                break;
            default:
                throw new InvalidSettingsException(
                    "Spark context ID scheme not supported: " + sparkConfig.getSparkContextID().getScheme());
        }

        // everything seems valid, now we can update the node model state and return port object specs
        m_dbSettings = dbSettings;
        m_fsConnectionInfo = fsConnectionInfo;
        m_sparkConfig = sparkConfig;

        return new PortObjectSpec[]{(m_dbSettings != null) ? new DatabaseConnectionPortObjectSpec(m_dbSettings) : null,
            new ConnectionInformationPortObjectSpec(m_fsConnectionInfo),
            new SparkContextPortObjectSpec(sparkConfig.getSparkContextID())};
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void configureSparkContext(SparkContextConfig config) throws InvalidSettingsException {
        final SparkContext sparkContext = SparkContextManager.getOrCreateSparkContext(config.getSparkContextID());
        final boolean configApplied = sparkContext.ensureConfigured(config, true);
        if (!configApplied) {
            throw new InvalidSettingsException("Could not apply settings for testing Spark context");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final Map<String, FlowVariable> flowVars = getAvailableFlowVariables();

        // test remote fs first because it is quick and does not require any resources
        exec.setProgress(0, "Opening remote file system connection");
        testRemoteFsConnection(m_fsConnectionInfo);
        final ConnectionInformationPortObject fsPortObject =
                new ConnectionInformationPortObject(new ConnectionInformationPortObjectSpec(m_fsConnectionInfo));

        // then try to open the Spark context
        exec.setProgress(0.1, "Configuring Spark context");
        final SparkContextPortObject sparkPortObject = new SparkContextPortObject(m_sparkConfig.getSparkContextID());
        configureSparkContext(m_sparkConfig);
        exec.setProgress(0.2, "Creating Spark context");
        SparkContextManager.getOrCreateSparkContext(m_sparkConfig.getSparkContextID()).ensureOpened(true, exec.createSubProgress(0.7));

        // finally, open the DB connection
        final PortObject dbPortObject;
        switch (m_sparkConfig.getSparkContextID().getScheme()) {
            case SPARK_LOCAL:
                if (isLocalSparkWithThriftserver(flowVars)) {
                    dbPortObject = new DatabaseConnectionPortObject(new DatabaseConnectionPortObjectSpec(
                        LocalHiveTestingConnectionSettingsFactory.create(flowVars)));

                    openHiveConnection(((DatabaseConnectionPortObject)dbPortObject).getSpec(),
                        exec.createSubProgress(0.1));
                } else {
                    dbPortObject = InactiveBranchPortObject.INSTANCE;
                }
                break;
            case SPARK_JOBSERVER:
            case SPARK_LIVY:
                dbPortObject = new DatabaseConnectionPortObject(new DatabaseConnectionPortObjectSpec(m_dbSettings));
                break;
            default:
                throw new InvalidSettingsException(
                    "Spark context ID scheme not supported: " + m_sparkConfig.getSparkContextID().getScheme());
        }

        return new PortObject[]{dbPortObject, fsPortObject, sparkPortObject};
    }

    private static void testRemoteFsConnection(final ConnectionInformation connInfo) throws Exception {
        final ConnectionMonitor<?> monitor = new ConnectionMonitor<>();

        try {
            final RemoteFile<? extends Connection> file =
                RemoteFileFactory.createRemoteFile(connInfo.toURI(), connInfo, monitor);
            if (file != null) {
                //perform a simple operation to check that the connection really exists and is valid
                file.exists();
            }
        } finally {
            monitor.closeAll();
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

    private static boolean isLocalSparkWithThriftserver(final Map<String, FlowVariable> flowVars) {
    return TestflowVariable.stringEquals(TestflowVariable.SPARK_LOCAL_SQLSUPPORT, "HIVEQL_WITH_JDBC", flowVars);
    }
}
