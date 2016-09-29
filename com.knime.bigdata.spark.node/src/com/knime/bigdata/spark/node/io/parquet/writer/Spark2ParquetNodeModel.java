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
 *   Created on Aug 10, 2016 by sascha
 */
package com.knime.bigdata.spark.node.io.parquet.writer;

import java.util.ArrayList;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.hdfs.filehandler.HDFSRemoteFileHandler;
import com.knime.bigdata.spark.core.context.SparkContextUtil;
import com.knime.bigdata.spark.core.job.JobInput;
import com.knime.bigdata.spark.core.job.SimpleJobRunFactory;
import com.knime.bigdata.spark.core.node.SparkNodeModel;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataTableUtil;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateField;
import com.knime.bigdata.spark.core.types.intermediate.IntermediateSpec;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2ParquetNodeModel extends SparkNodeModel {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(Spark2ParquetNodeModel.class);

    /**The unique Spark job id.*/
    public static final String JOB_ID = Spark2ParquetNodeModel.class.getCanonicalName();

    private final Spark2ParquetSettings m_settings = new Spark2ParquetSettings();

    /** Constructor. */
    public Spark2ParquetNodeModel() {
        super(new PortType[] {ConnectionInformationPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[0]);
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("Connection or input data missing");
        }

        final ConnectionInformationPortObjectSpec object = (ConnectionInformationPortObjectSpec) inSpecs[0];
        final ConnectionInformation connInfo = object.getConnectionInformation();

        if (connInfo == null) {
            throw new InvalidSettingsException("No connection information available");
        }

        if (!HDFSRemoteFileHandler.isSupportedConnection(connInfo)) {
            throw new InvalidSettingsException("HDFS connection required");
        }

        m_settings.validateSettings();

        return new PortObjectSpec[0];
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        exec.setMessage("Starting spark job");

        final String outputPath = m_settings.getDirectory() + "/" + m_settings.getTableName();
        final String saveMode = m_settings.getSaveMode();

        final SparkDataPortObject rdd = (SparkDataPortObject) inData[1];
        final IntermediateSpec schema = SparkDataTableUtil.toIntermediateSpec(rdd.getTableSpec());

        if (SparkContextUtil.getSparkVersion(rdd.getContextID()).equals(SparkVersion.V_1_2)) {
            ArrayList<String> warnings = new ArrayList<>();

            if (!saveMode.equalsIgnoreCase("ErrorIfExists")) {
                warnings.add("Spark 1.2 does not support save mode on parquet files. Save mode settings ignored.");
            }

            if (containsType(schema, IntermediateDataTypes.TIMESTAMP)) {
                warnings.add("Spark 1.2 does not support timestamp on parquert files, use Spark 1.3 or higher if required.");
            }

            if (warnings.size() > 1) {
                setWarningMessage(warnings.get(0) + "\n" + warnings.get(1));
            } else if(warnings.size() > 0) {
                setWarningMessage(warnings.get(0));
            }
        }

        LOGGER.info("Writing " + rdd.getData().getID() + " rdd into " + outputPath);
        final SimpleJobRunFactory<JobInput> runFactory = SparkContextUtil.getSimpleRunFactory(rdd.getContextID(), JOB_ID);
        final Spark2ParquetJobInput jobInput = new Spark2ParquetJobInput(rdd.getData().getID(), outputPath, schema, saveMode);
        runFactory.createRun(jobInput).run(rdd.getContextID(), exec);

        return new PortObject[0];
    }

    private boolean containsType(final IntermediateSpec spec, final IntermediateDataType type) {
        for (IntermediateField field : spec.getFields()) {
            if (field.getType().equals(type)) {
                return true;
            }
        }
        return false;
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
