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
 *   Created on Sep 06, 2016 by Sascha
 */
package com.knime.bigdata.spark.node.io.database.reader;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
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
import org.knime.core.node.port.database.StatementManipulator;
import org.knime.core.node.port.database.aggregation.DBAggregationFunction;
import org.knime.core.node.port.database.connection.DBDriverFactory;
import org.knime.core.node.workflow.CredentialsProvider;

import com.knime.bigdata.spark.core.context.SparkContextID;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.job.JobOutput;
import com.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataTable;
import com.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import com.knime.bigdata.spark.core.util.SparkIDs;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Database2SparkNodeModel extends SparkSourceNodeModel {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(Database2SparkNodeModel.class);

    /** The unique Spark job id. */
    public static final String JOB_ID = Database2SparkNodeModel.class.getCanonicalName();

    private final Database2SparkSettings m_settings = new Database2SparkSettings();

    /** Constructor. */
    public Database2SparkNodeModel() {
        super(new PortType[] {DatabasePortObject.TYPE},
              new PortType[] {SparkDataPortObject.TYPE});
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
        ensureContextIsOpen(contextID);

        final String namedOutputObject = SparkIDs.createRDDID();
        final ArrayList<File> jarFiles = new ArrayList<>();
        final Database2SparkJobInput jobInput = createJobInput(namedOutputObject, dbSettings);
        LOGGER.debug("Using JDBC Url: " + jobInput.getUrl());

        if (m_settings.uploadDriver()) {
            DBDriverFactory dbDriverFactory = dbSettings.getUtility().getConnectionFactory().getDriverFactory();
            jarFiles.addAll(dbDriverFactory.getDriverFiles(dbSettings));
            jobInput.setDriver(dbSettings.getDriver());
        }

        final JobOutput jobOutput = SparkContextUtil.getJobWithFilesRunFactory(contextID, JOB_ID)
            .createRun(jobInput, jarFiles)
            .run(contextID, exec);

        final DataTableSpec outputSpec = KNIMEToIntermediateConverterRegistry.convertSpec(jobOutput.getSpec(namedOutputObject));
        final SparkDataTable resultTable = new SparkDataTable(contextID, namedOutputObject, outputSpec);
        final SparkDataPortObject sparkObject = new SparkDataPortObject(resultTable);

        return new PortObject[] { sparkObject };
    }

    private Database2SparkJobInput createJobInput(final String namedOutputObject, final DatabaseQueryConnectionSettings settings) throws InvalidSettingsException {
        final CredentialsProvider cp = getCredentialsProvider();
        final String url = settings.getJDBCUrl();
        final String query =  String.format("(%s)", settings.getQuery());
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
        String table = "(" + settings.getQuery() + ")";
        String newQuery = "SELECT "
                + minFunction.getSQLFragment4SubQuery(statementManipulator, table, partCol)
                + ", "
                + maxFunction.getSQLFragment4SubQuery(statementManipulator, table, partCol)
                + " FROM " + table;

        try(final Connection connection = settings.createConnection(getCredentialsProvider())) {
            synchronized (settings.syncConnection(connection)) {
                try(final ResultSet result = connection.createStatement().executeQuery(newQuery)) {
                    result.next();
                    long min = result.getLong(1);
                    long max = result.getLong(2);
                    jobInput.setPartitioning(m_settings.getPartitionColumn(), min, max, m_settings.getNumPartitions());
                    LOGGER.info("Using " + min + " as lower and " + max + " as upper bound.");
                }
            }
        } catch(Exception e) {
            throw new InvalidSettingsException("Unable to fetch lower and upper partition bounds.", e);
        }
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadValidatedSettingsFrom(settings);
    }
}