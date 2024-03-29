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
 *   Created on 27.05.2015 by koetter
 */
package org.knime.bigdata.spark.node.io.hive.writer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;

import org.knime.bigdata.hive.utility.HiveUtility;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.job.JobInput;
import org.knime.bigdata.spark.core.job.SimpleJobRunFactory;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.time.localtime.LocalTimeValue;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
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
import org.knime.core.node.port.database.DatabaseUtility;
import org.knime.core.node.port.database.ExecuteStatement;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
@Deprecated
public class Spark2HiveNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = Spark2HiveNodeModel.class.getCanonicalName();

    private Spark2HiveSettings m_settings = new Spark2HiveSettings(getDefaultFormat());

    /**
     * Constructor.
     */
    public Spark2HiveNodeModel() {
        super(new PortType[]{DatabaseConnectionPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{DatabasePortObject.TYPE});

    }

    /**
     * @return the default file format for the Hive table creation
     *
     */
    protected FileFormat getDefaultFormat() {
        return FileFormat.ORC;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("No input data found");
        }

        final DatabaseConnectionPortObjectSpec spec = (DatabaseConnectionPortObjectSpec)inSpecs[0];
        checkDatabaseIdentifier(spec);
        final DataTableSpec tableSpec = ((SparkDataPortObjectSpec)inSpecs[1]).getTableSpec();
        final DatabasePortObjectSpec resultSpec = createResultSpec(spec, tableSpec);
        FileFormat fileFormat = FileFormat.fromDialogString(m_settings.getFileFormat());
        // Check for limitations in column names.
        checkColumnNames(tableSpec, fileFormat);
        if (fileFormat == FileFormat.AVRO) {
            for (int i = 0; i < tableSpec.getNumColumns(); i++) {
                DataType c = tableSpec.getColumnSpec(i).getType();
                if (c.isCompatible(LocalTimeValue.class)) {
                    throw new InvalidSettingsException("Avro does not support timestamp.");
                }
            }
        }
        return new PortObjectSpec[]{resultSpec};
    }

    private static void checkColumnNames(final DataTableSpec tableSpec, final FileFormat fileFormat)
        throws InvalidSettingsException {
        String[] columnNames = tableSpec.getColumnNames();
        for (String name : columnNames) {
            if (name.contains(",")) {
                throw new InvalidSettingsException(String.format("No comma allowed in column name: %s.", name));
            }
            if (fileFormat == FileFormat.AVRO && !Pattern.matches("[A-Za-z_][A-Za-z0-9_]*", name)) {
                throw new InvalidSettingsException(String.format(
                    "Invalid column name %s. Column names in Avro must match the regular expression [A-Za-z_][A-Za-z0-9_]*.",
                    name));
            }
        }
    }

    /**
     * Checks whether the input Database is compatible.
     *
     * @param spec the {@link DatabaseConnectionPortObjectSpec} from the input port
     * @throws InvalidSettingsException If the wrong database is connected
     */
    protected void checkDatabaseIdentifier(final DatabaseConnectionPortObjectSpec spec)
        throws InvalidSettingsException {
        if (!spec.getDatabaseIdentifier().contains(HiveUtility.DATABASE_IDENTIFIER)) {
            throw new InvalidSettingsException("Input must be a Hive connection");
        }
    }

    private DatabasePortObjectSpec createResultSpec(final DatabaseConnectionPortObjectSpec spec,
        final DataTableSpec tableSpec) throws InvalidSettingsException {
        //TK_TODO: take into account that hive does not support upper case columns
        final String query = "SELECT * FROM " + m_settings.getTableName();
        return new DatabasePortObjectSpec(tableSpec,
            new DatabaseQueryConnectionSettings(spec.getConnectionSettings(getCredentialsProvider()), query));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");
        final SparkDataPortObject rdd = (SparkDataPortObject)inData[1];
        final SimpleJobRunFactory<JobInput> runFactory =
            SparkContextUtil.getSimpleRunFactory(rdd.getContextID(), JOB_ID);
        final DatabaseConnectionPortObject con = (DatabaseConnectionPortObject)inData[0];
        final DatabaseConnectionSettings settings = con.getConnectionSettings(getCredentialsProvider());
        final DatabaseUtility utility = settings.getUtility();
        final String tableName = m_settings.getTableName();
        settings.execute(getCredentialsProvider(), new ExecuteStatement<String>() {
            @Override
            public String apply(final Connection connection) throws Exception {
                if (utility.tableExists(connection, tableName)) {
                    if (m_settings.getDropExisting()) {
                        final String dropTableStmt = utility.getStatementManipulator().dropTable(tableName, false);
                        settings.execute(dropTableStmt, getCredentialsProvider());
                    } else {
                        throw new KNIMESparkException("Table " + tableName + " already exists");
                    }
                }
                return null;
            }
        });
        final IntermediateSpec schema = SparkDataTableUtil.toIntermediateSpec(rdd.getTableSpec());
        FileFormat fileFormat = FileFormat.fromDialogString(m_settings.getFileFormat());

        final Spark2HiveJobInput jobInput = new Spark2HiveJobInput(rdd.getData().getID(), tableName, schema,
            fileFormat.name(), m_settings.getCompression());

        runFactory.createRun(jobInput).run(rdd.getContextID());
        settings.execute(getCredentialsProvider(), new ExecuteStatement<String>() {
            @Override
            public String apply(final Connection connection) throws Exception {
                try {
                    postProcessing(connection, tableName, exec);
                } catch (SQLException e) {
                    throw new InvalidSettingsException("During PostProcessing: " + e.getMessage());
                }
                return null;
            }
        });
        final DatabasePortObjectSpec resultSpec = createResultSpec(con.getSpec(), rdd.getTableSpec());
        return new PortObject[]{new DatabasePortObject(resultSpec)};
    }

    /**
     * Do whatever post processing is necessary.
     *
     * @param connection the database connection settings
     * @param tableName the created table's name
     * @param exec the execution environment
     * @throws SQLException
     */
    protected void postProcessing(final Connection connection, final String tableName, final ExecutionContext exec)
        throws SQLException {
        // do nothing
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveAdditionalSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateAdditionalSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadAdditionalValidatedSettingsFrom(settings);
    }

}
