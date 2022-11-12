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
 *   Created on Sep 06, 2016 by Sascha
 */
package org.knime.bigdata.spark.node.io.database.writer;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObject;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.core.node.port.database.DatabaseConnectionSettings;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.connection.DBDriverFactory;
import org.knime.core.node.port.database.reader.DBReader;
import org.knime.core.node.workflow.CredentialsProvider;

/**
 * @author Sascha Wolke, KNIME.com
 */
@Deprecated
public class Spark2DatabaseNodeModel extends SparkNodeModel {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(Spark2DatabaseNodeModel.class);

    /** The unique Spark job id. */
    public static final String JOB_ID = Spark2DatabaseNodeModel.class.getCanonicalName();

    private final Spark2DatabaseSettings m_settings = new Spark2DatabaseSettings();

    /** Constructor. */
    public Spark2DatabaseNodeModel() {
        super(new PortType[] {DatabaseConnectionPortObject.TYPE, SparkDataPortObject.TYPE},
              new PortType[] {DatabasePortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("Connection or input data missing");
        }

        final DatabaseConnectionPortObjectSpec spec = (DatabaseConnectionPortObjectSpec)inSpecs[0];
        checkDatabaseIdentifier(spec);

        // Do not use the table spec here, final SQL table might use different data types!
        return new PortObjectSpec[] { null };
    }

    /**
     * Checks whether the input Database is compatible.
     * @param spec the {@link DatabaseConnectionPortObjectSpec} from the input port
     * @throws InvalidSettingsException If the wrong database is connected
     */
    private void checkDatabaseIdentifier(final DatabaseConnectionPortObjectSpec spec) throws InvalidSettingsException {
        String jdbcUrl = spec.getConnectionSettings(getCredentialsProvider()).getJDBCUrl();
        if (StringUtils.isBlank(jdbcUrl) || !jdbcUrl.startsWith("jdbc:")) {
            throw new InvalidSettingsException("No JDBC URL provided.");
        } else if (jdbcUrl.startsWith("jdbc:hive")) {
            throw new InvalidSettingsException("Unsupported connection, use Spark to Hive/Impala node instead.");
        }
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");
        final DatabaseConnectionPortObject dbPort = (DatabaseConnectionPortObject) inData[0];
        final DatabaseConnectionSettings dbSettings = dbPort.getConnectionSettings(getCredentialsProvider());
        final SparkDataPortObject rdd = (SparkDataPortObject) inData[1];
        final SparkContextID contextID = rdd.getContextID();

        final ArrayList<File> jarFiles = new ArrayList<>();
        final Spark2DatabaseJobInput jobInput = createJobInput(rdd, dbSettings);
        LOGGER.debug("Using JDBC Url: " + jobInput.getUrl());

        if (m_settings.uploadDriver()) {
            DBDriverFactory dbDriverFactory = dbSettings.getUtility().getConnectionFactory().getDriverFactory();
            jarFiles.addAll(dbDriverFactory.getDriverFiles(dbSettings));
            jobInput.setDriver(dbSettings.getDriver());
        }
        try {
            SparkContextUtil.getJobWithFilesRunFactory(contextID, JOB_ID)
                .createRun(jobInput, jarFiles)
                .run(contextID, exec);
        } catch (KNIMESparkException e) {
            final String message = e.getMessage();
            if (message != null && message.contains("Failed to load JDBC data: No suitable driver")) {
                LOGGER.debug("Required JDBC driver not found in cluster. Original error message: " + e.getMessage());
                throw new InvalidSettingsException("Required JDBC driver not found. Enable the 'Upload local JDBC driver' "
                    + "option in the node dialog to upload the required driver files to the cluster.");
            }
            throw e;
        }

        return new PortObject[] { new DatabasePortObject(createResultSpec(dbPort.getSpec())) };
    }

    private Spark2DatabaseJobInput createJobInput(final SparkDataPortObject rdd, final DatabaseConnectionSettings settings) {
        final CredentialsProvider cp = getCredentialsProvider();
        final Properties conProperties = new Properties();
        final String namedInputObject = rdd.getData().getID();
        final IntermediateSpec schema = SparkDataTableUtil.toIntermediateSpec(rdd.getTableSpec());
        final Spark2DatabaseJobInput input = new Spark2DatabaseJobInput(namedInputObject, schema,
            settings.getJDBCUrl(), m_settings.getTable(), m_settings.getSaveMode(), conProperties);

        String user = settings.getUserName(cp);
        String password = settings.getPassword(cp);
        if (!StringUtils.isBlank(user)) {
           conProperties.setProperty("user", user);

           if (!StringUtils.isBlank(password)) {
               conProperties.setProperty("password", password);
           }
        }

        return input;
    }

    /** @return Output spec, based on table spec from database. */
    private DatabasePortObjectSpec createResultSpec(final DatabaseConnectionPortObjectSpec spec)
            throws InvalidSettingsException {

        try {
            String query = "SELECT * FROM " + m_settings.getTable();
            DatabaseConnectionSettings settings = spec.getConnectionSettings(getCredentialsProvider());
            DatabaseQueryConnectionSettings querySettings = new DatabaseQueryConnectionSettings(settings, query);
            DBReader conn = querySettings.getUtility().getReader(querySettings);
            DataTableSpec tableSpec = conn.getDataTableSpec(getCredentialsProvider());
            return new DatabasePortObjectSpec(tableSpec, querySettings);

        } catch (SQLException e) {
            throw new InvalidSettingsException("Unable to fetch result table spec from database: " + e.getMessage());
        }
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadValidatedSettingsFrom(settings);
    }
}