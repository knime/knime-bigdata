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
 *   Created on 29.04.2019 by Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
package org.knime.bigdata.spark.node.io.database.hive.writer;

import java.sql.SQLException;
import java.sql.SQLType;
import java.util.regex.Pattern;

import org.knime.bigdata.database.hive.Hive;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.node.io.database.hive.writer.DBSpark2HiveSettings.TableExistsAction;
import org.knime.bigdata.spark.node.io.hive.writer.FileFormat;
import org.knime.bigdata.spark.node.io.hive.writer.Spark2HiveJobInput;
import org.knime.bigdata.spark.node.io.hive.writer.Spark2HiveNodeModel;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.time.localtime.LocalTimeValue;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.database.DatabaseConnectionPortObjectSpec;
import org.knime.database.DBDataObject;
import org.knime.database.DBType;
import org.knime.database.agent.ddl.DBStructureManipulator;
import org.knime.database.agent.ddl.DropTableParameters;
import org.knime.database.agent.metadata.DBMetadataReader;
import org.knime.database.datatype.mapping.DBTypeMappingRegistry;
import org.knime.database.datatype.mapping.DBTypeMappingService;
import org.knime.database.model.DBTable;
import org.knime.database.model.impl.DefaultDBTable;
import org.knime.database.port.DBDataPortObject;
import org.knime.database.port.DBSessionPortObject;
import org.knime.database.port.DBSessionPortObjectSpec;
import org.knime.database.session.DBSession;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;

/**
 *
 * @author Mareike Hoeger, KNIME GmbH, Konstanz, Germany
 */
public class DBSpark2HiveNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = Spark2HiveNodeModel.class.getCanonicalName();

    private final DBSpark2HiveSettings m_settings = new DBSpark2HiveSettings(getDefaultFormat());

    /**
     * Constructor.
     */
    public DBSpark2HiveNodeModel() {
        super(new PortType[]{DBSessionPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{DBDataPortObject.TYPE});

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

        final DBSessionPortObjectSpec spec = (DBSessionPortObjectSpec)inSpecs[0];
        checkDatabaseIdentifier(spec);
        final DataTableSpec tableSpec = ((SparkDataPortObjectSpec)inSpecs[1]).getTableSpec();

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
        return new PortObjectSpec[]{null};
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
    protected void checkDatabaseIdentifier(final DBSessionPortObjectSpec spec) throws InvalidSettingsException {
        final DBType dbType = spec.getDBSession().getDBType();
        //I use the string here for databricks to avoid a dependency on the whole Databricks plugin
        if (Hive.DB_TYPE != dbType && !dbType.getId().equals("databricks")) {
            throw new InvalidSettingsException("Input must be a Hive connection");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");
        final SparkDataPortObject rdd = (SparkDataPortObject)inData[1];
        final DBSessionPortObject sessionObj = (DBSessionPortObject)inData[0];
        final DBSession session = sessionObj.getDBSession();
        final DefaultDBTable table = new DefaultDBTable(m_settings.getTableName(), m_settings.getSchema());

        checkTableExistsAndDropIfAllowed(exec, session, table);

        final Spark2HiveJobInput jobInput = createJobInput(rdd, session, table);

        SparkContextUtil.getSimpleRunFactory(rdd.getContextID(), JOB_ID)
            .createRun(jobInput)
            .run(rdd.getContextID());

        postProcessing(session, table, exec);

        final DataTypeMappingConfiguration<SQLType> typeMappingConfig = getTypeMappingConfig(sessionObj);
        final DBDataObject resultObject = session.getAgent(DBMetadataReader.class).getDBDataObject(exec,
            table.getSchemaName(), table.getName(), typeMappingConfig);

        return new PortObject[]{new DBDataPortObject(sessionObj, resultObject)};
    }

    private void checkTableExistsAndDropIfAllowed(final ExecutionContext exec, final DBSession session, final DefaultDBTable table)
        throws CanceledExecutionException, SQLException, KNIMESparkException {
        final boolean tableExist = session.getAgent(DBMetadataReader.class).isExistingTable(exec, table);
        if (tableExist) {
            if (m_settings.onExistingTableAction() == TableExistsAction.DROP) {
                final DBStructureManipulator manipulator = session.getAgent(DBStructureManipulator.class);
                manipulator.dropTable(exec, new DropTableParameters(table, false));
            } else {
                throw new KNIMESparkException("Table " + table.getName() + " already exists");
            }
        }
    }

    private Spark2HiveJobInput createJobInput(final SparkDataPortObject rdd, final DBSession session,
        final DefaultDBTable table) {
        final IntermediateSpec schema = SparkDataTableUtil.toIntermediateSpec(rdd.getTableSpec());
        FileFormat fileFormat = FileFormat.fromDialogString(m_settings.getFileFormat());
        String fullname = session.getDialect().createFullName(table);
        final Spark2HiveJobInput jobInput = new Spark2HiveJobInput(rdd.getData().getID(), fullname, schema,
            fileFormat.name(), m_settings.getCompression());
        return jobInput;
    }

    private static DataTypeMappingConfiguration<SQLType> getTypeMappingConfig(final DBSessionPortObject sessionObj)
        throws InvalidSettingsException {

        final DBTypeMappingService<?, ?> mappingService =
            DBTypeMappingRegistry.getInstance().getDBTypeMappingService(sessionObj.getDBSession().getDBType());

        final DataTypeMappingConfiguration<SQLType> typeMappingConfig = sessionObj.getExternalToKnimeTypeMapping()
            .resolve(mappingService, DataTypeMappingDirection.EXTERNAL_TO_KNIME);

        return typeMappingConfig;
    }

    /**
     * Do whatever post processing is necessary.
     *
     * @param session active database session
     * @param tableName the name of the created table
     * @param exec the execution environment
     * @throws CanceledExecutionException
     * @throws InvalidSettingsException
     */
    protected void postProcessing(final DBSession session, final DBTable tableName, final ExecutionContext exec)
        throws CanceledExecutionException, InvalidSettingsException {
        // nothing to do in Hive
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
