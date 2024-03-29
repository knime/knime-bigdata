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
package org.knime.bigdata.spark.node.io.database.reader;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabasePortObject;
import org.knime.core.node.port.database.DatabasePortObjectSpec;
import org.knime.core.node.port.database.DatabaseQueryConnectionSettings;
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.ExecuteStatement;
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.port.database.aggregation.DBAggregationFunction;
import org.knime.core.node.port.database.connection.DBDriverFactory;
import org.knime.core.node.workflow.CredentialsProvider;

/**
 * @author Sascha Wolke, KNIME.com
 */
@Deprecated
public class Database2SparkNodeModel extends SparkSourceNodeModel {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(Database2SparkNodeModel.class);

    /** The unique Spark job id. */
    public static final String JOB_ID = Database2SparkNodeModel.class.getCanonicalName();

    private final Database2SparkSettings m_settings = new Database2SparkSettings();

    private final boolean m_isDeprecatedNode;

    /**
     * Default constructor.
     *
     * @param isDeprecatedNode Whether this node model instance should emulate the behavior of the deprecated
     *            table2spark node model.
     */
    public Database2SparkNodeModel(final boolean isDeprecatedNode) {
        super(new PortType[] {DatabasePortObject.TYPE}, isDeprecatedNode,
              new PortType[] {SparkDataPortObject.TYPE});

        m_isDeprecatedNode = isDeprecatedNode;
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input query/table found.");
        }

        final DatabasePortObjectSpec spec = (DatabasePortObjectSpec)inSpecs[0];
        checkDatabaseIdentifier(spec);

        // Do not use the database spec here! Use the spark dataframe schema at execution instead.
        return new PortObjectSpec[] { null };
    }

    /**
     * Checks whether the input Database is compatible.
     * @param spec the {@link DatabasePortObjectSpec} from the input port
     * @throws InvalidSettingsException If the wrong database is connected
     */
    private void checkDatabaseIdentifier(final DatabasePortObjectSpec spec) throws InvalidSettingsException {
        String jdbcUrl = spec.getConnectionSettings(getCredentialsProvider()).getJDBCUrl();
        if (StringUtils.isBlank(jdbcUrl) || !jdbcUrl.startsWith("jdbc:")) {
            throw new InvalidSettingsException("No JDBC URL provided.");
        } else if (jdbcUrl.startsWith("jdbc:hive")) {
            throw new InvalidSettingsException("Unsupported connection, use Hive/Impala to Spark node instead.");
        }
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");
        final DatabasePortObject dbPort = (DatabasePortObject) inData[0];
        final DatabaseQueryConnectionSettings dbSettings = dbPort.getConnectionSettings(getCredentialsProvider());
        final SparkContextID contextID = getContextID(inData);

        if (m_isDeprecatedNode) {
            exec.setMessage("Creating a Spark context...");
            ensureContextIsOpen(contextID, exec.createSubProgress(0.1));
        }

        final String namedOutputObject = SparkIDs.createSparkDataObjectID();
        final ArrayList<File> jarFiles = new ArrayList<>();
        final Database2SparkJobInput jobInput = createJobInput(namedOutputObject, dbSettings);
        LOGGER.debug("Using JDBC Url: " + jobInput.getUrl());

        if (m_settings.uploadDriver()) {
            DBDriverFactory dbDriverFactory = dbSettings.getUtility().getConnectionFactory().getDriverFactory();
            jarFiles.addAll(dbDriverFactory.getDriverFiles(dbSettings));
            jobInput.setDriver(dbSettings.getDriver());
        }

        try {
            final JobOutput jobOutput = SparkContextUtil.getJobWithFilesRunFactory(contextID, JOB_ID)
                .createRun(jobInput, jarFiles)
                .run(contextID, exec);
            final DataTableSpec outputSpec = KNIMEToIntermediateConverterRegistry
                .convertSpec(jobOutput.getSpec(namedOutputObject));

            return new PortObject[]{new SparkDataPortObject(
                new SparkDataTable(contextID, namedOutputObject, outputSpec))};
        } catch (KNIMESparkException e) {
            final String message = e.getMessage();
            if (message != null && message.contains("Failed to load JDBC data: No suitable driver")) {
                LOGGER.debug("Required JDBC driver not found in cluster. Original error message: " + e.getMessage());
                throw new InvalidSettingsException("Required JDBC driver not found. Enable the 'Upload local JDBC driver' "
                    + "option in the node dialog to upload the required driver files to the cluster.");
            }
            throw e;
        }
    }

    private Database2SparkJobInput createJobInput(final String namedOutputObject, final DatabaseQueryConnectionSettings settings) throws InvalidSettingsException {
        final CredentialsProvider cp = getCredentialsProvider();
        final String url = settings.getJDBCUrl();
        final String query =  String.format("(%s) %s", settings.getQuery(), getTempTableName(url));
        final Properties conProperties = new Properties();
        final Database2SparkJobInput input = new Database2SparkJobInput(namedOutputObject, url, query, conProperties);

        String user = settings.getUserName(cp);
        String password = settings.getPassword(cp);
        if (!StringUtils.isBlank(user)) {
           conProperties.setProperty("user", user);

           if (!StringUtils.isBlank(password)) {
               conProperties.setProperty("password", password);
           }
        }

        if (!m_settings.useDefaultFetchSize()) {
            conProperties.setProperty("fetchSize", Integer.toString(m_settings.getFetchSize()));
        }

        if (m_settings.usePartitioning()) {
            if (m_settings.useAutoBounds()) {
                fetchBounds(input, settings);
            } else {
                input.setPartitioning(m_settings.getPartitionColumn(),
                                      m_settings.getLowerBound(),
                                      m_settings.getUpperBound(),
                                      m_settings.getNumPartitions());
            }
        }

        return input;
    }

    /**
     * Fetch lower+upper partition bounds from server and setup job input partitioning settings.
     */
    private void fetchBounds(final Database2SparkJobInput jobInput, final DatabaseQueryConnectionSettings settings) throws InvalidSettingsException {
        DatabaseUtility utility = settings.getUtility();
        StatementManipulator statementManipulator = settings.getUtility().getStatementManipulator();
        String partCol = utility.getStatementManipulator().quoteIdentifier(m_settings.getPartitionColumn());
        DBAggregationFunction minFunction = utility.getAggregationFunction("MIN");
        DBAggregationFunction maxFunction = utility.getAggregationFunction("MAX");
        String table = "(" + settings.getQuery() + ") " + getTempTableName(settings.getJDBCUrl());
        final String newQuery = "SELECT "
                + minFunction.getSQLFragment4SubQuery(statementManipulator, table, partCol)
                + ", "
                + maxFunction.getSQLFragment4SubQuery(statementManipulator, table, partCol)
                + " FROM " + table;
        try {
            settings.execute(getCredentialsProvider(), new ExecuteStatement<String>() {
                @Override
                public String apply(final Connection connection) throws Exception {
                try(final ResultSet result = connection.createStatement().executeQuery(newQuery)) {
                    result.next();
                    long min = result.getLong(1);
                    long max = result.getLong(2);
                    jobInput.setPartitioning(m_settings.getPartitionColumn(), min, max, m_settings.getNumPartitions());
                    LOGGER.info("Using " + min + " as lower and " + max + " as upper bound.");
                    return null;
                }
            }});
            } catch(Exception e) {
                throw new InvalidSettingsException("Unable to fetch lower and upper partition bounds.", e);
            }
    }

    /**
     * Returns a random name for a temporary table.
     *
     * @return a random table name
     */
    private final String getTempTableName(final String jdbcUrl) {
        if (jdbcUrl.toLowerCase().startsWith("jdbc:oracle")) {
            // oracle supports only 30 characters in table names...
            return ("t" + UUID.randomUUID().toString().replace("-", "")).substring(0, 29);
        } else {
            return "tempTable_" + UUID.randomUUID().toString().replace('-', '_');
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