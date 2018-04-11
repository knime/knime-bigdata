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
 *   Created on Jan 29, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.mllib.freqitemset;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.model.SparkModel;
import org.knime.bigdata.spark.core.port.remotemodel.RemoteSparkModelPortObject;
import org.knime.bigdata.spark.core.port.remotemodel.RemoteSparkModelPortObjectSpec;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.LongCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Find frequent item sets using FP-growth.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkFrequentItemSetNodeModel extends SparkNodeModel {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkFrequentItemSetNodeModel.class);

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkFrequentItemSetNodeModel.class.getCanonicalName();

    /** The unique model name. */
    public static final String MODEL_NAME = "Frequent Items";

    private final SparkFrequentItemSetSettings m_settings = new SparkFrequentItemSetSettings();

    SparkFrequentItemSetNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, RemoteSparkModelPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("Spark input data required.");
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec)inSpecs[0];
        final SparkContextID contextId = spec.getContextID();
        m_settings.loadDefaults(new DataTableSpec[] { spec.getTableSpec() });
        final DataType itemsetType = getItemsetType(spec.getTableSpec());
        final FrequentItemSetModelMetaData modelMeta = new FrequentItemSetModelMetaData(itemsetType);
        return new PortObjectSpec[]{
            new SparkDataPortObjectSpec(contextId, createFreqItemsTableSpec(itemsetType)),
            new RemoteSparkModelPortObjectSpec(contextId, getSparkVersion(spec), MODEL_NAME, modelMeta)
        };
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject sparkPort = (SparkDataPortObject) inData[0];
        final SparkContextID contextID = sparkPort.getContextID();
        final String inputObject = sparkPort.getData().getID();
        final DataTableSpec inputSpec = sparkPort.getTableSpec();
        final DataType itemsetType = getItemsetType(inputSpec);
        final FrequentItemSetModelMetaData modelMeta = new FrequentItemSetModelMetaData(itemsetType);
        final String freqItemsOutputObject = SparkIDs.createSparkDataObjectID();
        final FrequentItemSetJobInput jobInput = new FrequentItemSetJobInput(
            inputObject, freqItemsOutputObject,
            m_settings.getItemsColumn(), m_settings.getMinSupport());
        if (m_settings.overwriteNumPartitions()) {
            jobInput.setNumPartitions(m_settings.getNumPartitions());
        }

        LOGGER.info("Running frequent items Spark job...");
        final FrequentItemSetJobOutput jobOutput =
            SparkContextUtil.<FrequentItemSetJobInput, FrequentItemSetJobOutput>getJobRunFactory(contextID, JOB_ID)
                .createRun(jobInput)
                .run(contextID, exec);
        LOGGER.info("Frequent items Spark job done.");

        final SparkModel sparkModel = new SparkModel(
            getSparkVersion(sparkPort), MODEL_NAME, jobOutput.getModel(), inputSpec, null, modelMeta);

        return new PortObject[]{
            createSparkPortObject(sparkPort, createFreqItemsTableSpec(itemsetType), freqItemsOutputObject),
            new RemoteSparkModelPortObject(contextID, sparkModel)
        };
    }

    /**
     * Return data type of items column or throw exception if column is missing.
     *
     * @param spec input data table spec
     * @return data type of items column, defined in settings
     * @throws InvalidSettingsException if input column is missing or not a collection
     */
    private DataType getItemsetType(final DataTableSpec spec) throws InvalidSettingsException {
        DataColumnSpec col = spec.getColumnSpec(m_settings.getItemsColumn());
        if (col == null) {
            throw new InvalidSettingsException("Input column " + m_settings.getItemsColumn() + " not found.");
        } else if (!col.getType().isCollectionType()) {
            throw new InvalidSettingsException("Input column " + m_settings.getItemsColumn() + " is not a collection.");
        } else {
            return col.getType();
        }
    }

    private DataTableSpec createFreqItemsTableSpec(final DataType itemsetType) {
        DataColumnSpecCreator items = new DataColumnSpecCreator("items", itemsetType);
        DataColumnSpecCreator freq = new DataColumnSpecCreator("freq", LongCell.TYPE);
        return new DataTableSpec(items.createSpec(), freq.createSpec());
    }

    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveAdditionalSettingsTo(settings);
    }

    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateAdditionalSettings(settings);
    }

    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadAdditionalValidatedSettingsFrom(settings);
    }
}
