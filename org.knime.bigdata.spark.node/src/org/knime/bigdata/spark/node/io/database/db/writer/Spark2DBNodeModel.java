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
package org.knime.bigdata.spark.node.io.database.db.writer;

import java.io.File;
import java.sql.SQLType;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.exception.KNIMESparkException;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import org.knime.bigdata.spark.node.io.database.db.SparkDBNodeUtils;
import org.knime.bigdata.spark.node.io.database.writer.Spark2DatabaseJobInput;
import org.knime.bigdata.spark.node.io.database.writer.Spark2DatabaseNodeModel;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.database.DBDataObject;
import org.knime.database.agent.metadata.DBMetadataReader;
import org.knime.database.connection.UrlDBConnectionController;
import org.knime.database.datatype.mapping.DBTypeMappingRegistry;
import org.knime.database.datatype.mapping.DBTypeMappingService;
import org.knime.database.port.DBDataPortObject;
import org.knime.database.port.DBSessionPortObject;
import org.knime.database.port.DBSessionPortObjectSpec;
import org.knime.database.session.DBSession;
import org.knime.datatype.mapping.DataTypeMappingConfiguration;
import org.knime.datatype.mapping.DataTypeMappingDirection;

/**
 * Spark to DB node model.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2DBNodeModel extends SparkNodeModel {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(Spark2DBNodeModel.class);

    /** The unique Spark job id. */
    public static final String JOB_ID = Spark2DatabaseNodeModel.class.getCanonicalName();

    private final Spark2DBSettings m_settings = new Spark2DBSettings();

    /** Constructor. */
    public Spark2DBNodeModel() {
        super(new PortType[] {DBSessionPortObject.TYPE, SparkDataPortObject.TYPE},
              new PortType[] {DBDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("Connection or input data missing");
        }

        final DBSessionPortObjectSpec spec = (DBSessionPortObjectSpec)inSpecs[0];
        SparkDBNodeUtils.checkDBIdentifier(spec.getDBSession(), false);
        SparkDBNodeUtils.checkJdbcUrlSupport(spec.getSessionInformation());

        // Do not use the table spec here, final SQL table might use different data types!
        return new PortObjectSpec[] { null };
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");
        final DBSessionPortObject dbPort = (DBSessionPortObject) inData[0];
        final DBSession dbSession = dbPort.getDBSession();
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[1];
        final SparkContextID contextID = sparkPort.getContextID();
        final List<File> jarFiles;
        final Spark2DatabaseJobInput jobInput = createJobInput(dbPort, sparkPort);
        LOGGER.debug("Using JDBC Url: " + jobInput.getUrl());

        if (m_settings.uploadDriver()) {
            jarFiles = SparkDBNodeUtils.getDriverFiles(dbSession);
            jobInput.setDriver(SparkDBNodeUtils.getDriverClass(dbSession));
        } else {
            jarFiles = new ArrayList<>();
        }

        try {
            SparkContextUtil.getJobWithFilesRunFactory(contextID, JOB_ID)
                .createRun(jobInput, jarFiles)
                .run(contextID, exec);
        } catch (KNIMESparkException e) {
            SparkDBNodeUtils.detectMissingDriverException(LOGGER, e);
            throw new InvalidSettingsException(e.getMessage(), e);
        }

        final DataTypeMappingConfiguration<SQLType> typeMappingConfig = getTypeMappingConfig(dbPort.getSpec());
        final DBDataObject resultObject = dbSession.getAgent(DBMetadataReader.class).getDBDataObject(exec,
            m_settings.getSchema(), m_settings.getTable(), typeMappingConfig);

        return new PortObject[]{new DBDataPortObject(dbPort, resultObject)};
    }

    private Spark2DatabaseJobInput createJobInput(final DBSessionPortObject dbPort, final SparkDataPortObject sparkPort) {
        final String namedInputObject = sparkPort.getData().getID();
        final IntermediateSpec schema = SparkDataTableUtil.toIntermediateSpec(sparkPort.getTableSpec());

        final UrlDBConnectionController controller = (UrlDBConnectionController) dbPort.getSessionInformation().getConnectionController();
        final String url = controller.getConnectionJdbcUrl();
        final Properties conProperties  = controller.getConnectionJdbcProperties();
        final String table = dbPort.getDBSession().getDialect().createFullName(m_settings.getSchema(), m_settings.getTable());
        return new Spark2DatabaseJobInput(namedInputObject, schema, url, table, m_settings.getSaveMode(), conProperties);
    }

    static DataTypeMappingConfiguration<SQLType> getTypeMappingConfig(final DBSessionPortObjectSpec spec)
            throws InvalidSettingsException {

        final DBTypeMappingService<?, ?> mappingService =
            DBTypeMappingRegistry.getInstance().getDBTypeMappingService(spec.getDBSession().getDBType());

        return spec.getExternalToKnimeTypeMapping().resolve(mappingService, DataTypeMappingDirection.EXTERNAL_TO_KNIME);
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