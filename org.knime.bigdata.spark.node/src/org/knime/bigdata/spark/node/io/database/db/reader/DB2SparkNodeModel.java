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
 *   Created on Mai 26, 2019 by Mareike
 */
package org.knime.bigdata.spark.node.io.database.db.reader;

import java.io.File;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobOutput;
import org.knime.bigdata.spark.core.node.SparkSourceNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.types.converter.knime.KNIMEToIntermediateConverterRegistry;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.node.io.database.db.SparkDBNodeUtils;
import org.knime.bigdata.spark.node.io.database.reader.Database2SparkJobInput;
import org.knime.bigdata.spark.node.io.database.reader.Database2SparkNodeModel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.database.SQLQuery;
import org.knime.database.connection.UrlDBConnectionController;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.function.aggregation.DBAggregationFunction;
import org.knime.database.function.aggregation.DBAggregationFunctionSet;
import org.knime.database.port.DBDataPortObject;
import org.knime.database.port.DBDataPortObjectSpec;
import org.knime.database.session.DBSession;


/**
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class DB2SparkNodeModel extends SparkSourceNodeModel {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(DB2SparkNodeModel.class);

    /** The unique Spark job id. */
    public static final String JOB_ID = Database2SparkNodeModel.class.getCanonicalName();

    private final DB2SparkSettings m_settings = new DB2SparkSettings();


    /**
     * Default constructor.
     *
     */
    public DB2SparkNodeModel() {
        super(new PortType[] {DBDataPortObject.TYPE}, false,
              new PortType[] {SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input query/table found.");
        }

        final DBDataPortObjectSpec spec = (DBDataPortObjectSpec)inSpecs[0];
        SparkDBNodeUtils.checkDBIdentifier(spec.getDBSession(), true);
        SparkDBNodeUtils.checkJdbcUrlSupport(spec.getSessionInformation());

        // Do not use the database spec here! Use the spark dataframe schema at execution instead.
        return new PortObjectSpec[] { null };
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");
        final DBDataPortObject dbPort = (DBDataPortObject) inData[0];
        final DBSession dbSession = dbPort.getDBSession();
        final SparkContextID contextID = getContextID(inData);
        final String namedOutputObject = SparkIDs.createSparkDataObjectID();
        final List<File> jarFiles;
        final Database2SparkJobInput jobInput = createJobInput(namedOutputObject, dbPort);
        LOGGER.debug("Using JDBC Url: " + jobInput.getUrl());

        if (m_settings.uploadDriver()) {
            jarFiles = SparkDBNodeUtils.getDriverFiles(dbSession);
            jobInput.setDriver(SparkDBNodeUtils.getDriverClass(dbSession));
        } else {
            jarFiles = new ArrayList<>();
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
            SparkDBNodeUtils.detectMissingDriverException(LOGGER, e);
            throw new InvalidSettingsException(e.getMessage(), e);
        }
    }

    private Database2SparkJobInput createJobInput(final String namedOutputObject, final DBDataPortObject dbPort) throws InvalidSettingsException {
        final UrlDBConnectionController controller = (UrlDBConnectionController) dbPort.getSessionInformation().getConnectionController();
        final String url = controller.getConnectionJdbcUrl();
        final Properties conProperties  = controller.getConnectionJdbcProperties();
        final SQLQuery sqlQuery = dbPort.getData().getQuery();
        final String query =  String.format("(%s) %s", sqlQuery.getQuery(), getTempTableName(url));
        final Database2SparkJobInput input = new Database2SparkJobInput(namedOutputObject, url, query, conProperties);

        if (!m_settings.useDefaultFetchSize()) {
            conProperties.setProperty("fetchSize", Integer.toString(m_settings.getFetchSize()));
        }

        if (m_settings.usePartitioning()) {
            if (m_settings.useAutoBounds()) {
                fetchBounds(input, dbPort, url);
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
    private void fetchBounds(final Database2SparkJobInput jobInput, final DBDataPortObject dbPort, final String url) throws InvalidSettingsException {

        DBSession session = dbPort.getDBSession();
        DBSQLDialect dialect = session.getDialect();

        String partCol = dialect.delimit(m_settings.getPartitionColumn());
        DBAggregationFunctionSet functions = dbPort.getDBSession().getAggregationFunctions();
        DBAggregationFunction minFunction = functions.getFunction("MIN");
        DBAggregationFunction maxFunction = functions.getFunction("MAX");

        SQLQuery sqlQuery = dbPort.getData().getQuery();

        String table = "(" + sqlQuery.getQuery() + ") " + getTempTableName(url);
        final String newQuery = "SELECT "
                + minFunction.getSQLFragment4SubQuery(table, partCol, dialect)
                + ", "
                + maxFunction.getSQLFragment4SubQuery(table, partCol, dialect)
                + " FROM " + table;

        try (java.sql.Connection connection = session.getConnectionProvider().getConnection(new ExecutionMonitor())) {
            try (Statement statement = connection.createStatement()) {
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

    /**
     * Returns a random name for a temporary table.
     *
     * @return a random table name
     */
    private final static String getTempTableName(final String jdbcUrl) {
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