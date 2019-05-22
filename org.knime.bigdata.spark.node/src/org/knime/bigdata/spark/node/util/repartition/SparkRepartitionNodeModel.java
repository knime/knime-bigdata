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
 *   Created on May 6, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.util.repartition;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;


/**
 * Spark node to repartition spark data frames.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkRepartitionNodeModel extends SparkNodeModel {

    /**
     *  The unique Spark job id.
     */
    public static final String JOB_ID = SparkRepartitionNodeModel.class.getCanonicalName();

    private final SparkRepartitionNodeSettings m_settings = new SparkRepartitionNodeSettings();

    /**
     * Default constructor.
     */
    public SparkRepartitionNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Spark data table found.");
        }

        final SparkDataPortObjectSpec sparkPortSpec = ((SparkDataPortObjectSpec)inSpecs[0]);
        final SparkVersion version = SparkContextUtil.getSparkVersion(sparkPortSpec.getContextID());
        if (SparkVersion.V_2_0.compareTo(version) > 0) {
            throw new InvalidSettingsException("Unsupported Spark Version! This node requires at least Spark 2.0.");
        }

        return inSpecs;
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject inputPort = (SparkDataPortObject)inData[0];
        final SparkContextID contextID = inputPort.getContextID();
        final String namedInputObject = inputPort.getTableName();
        final String namedOutputObject = SparkIDs.createSparkDataObjectID();

        final RepartitionJobInput jobInput = getJobInput(namedInputObject, namedOutputObject);
        final RepartitionJobOutput jobOutput = SparkContextUtil
            .<RepartitionJobInput, RepartitionJobOutput>getJobRunFactory(contextID, JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);

        if (jobOutput.isDynamicAllocation()) {
            setWarningMessage(
                String.format("Dynamic resource allocation detected. Found %d available executor cores.",
                    jobOutput.dynAllocationExecutors()));
        }

        return new PortObject[] { createSparkPortObject(inputPort, namedOutputObject) };
    }

    private RepartitionJobInput getJobInput(final String input, final String output) throws InvalidSettingsException {
        final boolean coalesce = m_settings.useCoalesce();

        switch (m_settings.getCalculationMode()) {
            case FIXED_VALUE:
                return RepartitionJobInput.fixedValue(input, output, coalesce, m_settings.getFixedValue());

            case MULTIPLY_PART_COUNT:
                return RepartitionJobInput.multiplyPartCount(input, output, coalesce,
                    m_settings.getMultiplyPartitionFactor());

            case DIVIDE_PART_COUNT:
                return RepartitionJobInput.dividePartCount(input, output, coalesce,
                    m_settings.getDividePartitionFactor());

            case MULTIPLY_EXECUTOR_CORES:
                return RepartitionJobInput.multiplyExecutorCoresCount(input, output, coalesce,
                    m_settings.getMultiplyCoresFactor());

            default:
                throw new InvalidSettingsException("Unknown calculation mode: " + m_settings.getCalculationMode());
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
        m_settings.loadSettingsFrom(settings);
    }
}
