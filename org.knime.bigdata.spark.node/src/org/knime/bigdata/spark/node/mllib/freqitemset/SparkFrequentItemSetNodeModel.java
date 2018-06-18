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

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.def.IntCell;
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

    private final SparkFrequentItemSetSettings m_settings = new SparkFrequentItemSetSettings();

    SparkFrequentItemSetNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("Spark input data required.");
        }

        final SparkDataPortObjectSpec itemsPortSpec = (SparkDataPortObjectSpec)inSpecs[0];
        final DataTableSpec itemsSpec = itemsPortSpec.getTableSpec();
        final SparkContextID contextId = itemsPortSpec.getContextID();
        m_settings.loadDefaults(itemsSpec);
        final DataType itemsetType = getColumnSpec(itemsSpec, "item sets", true, m_settings.getItemsColumn()).getType();
        return new PortObjectSpec[]{
            new SparkDataPortObjectSpec(contextId, createFreqItemsTableSpec(itemsetType))
        };
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject inputPort = (SparkDataPortObject) inData[0];
        final String outputObject = SparkIDs.createSparkDataObjectID();
        return new PortObject[]{runFreqItemsJob(exec, inputPort, outputObject, m_settings)};
    }

    /**
     * Run the frequent item sets spark job.
     *
     * @param exec context
     * @param inputPort input port with item sets
     * @param outputObject ID of output object
     * @param settings settings to use
     * @return output port with frequent item sets
     * @throws Exception on any failures
     */
    public static PortObject runFreqItemsJob(final ExecutionContext exec, final SparkDataPortObject inputPort,
        final String outputObject, final SparkFrequentItemSetSettings settings) throws Exception {

        final SparkContextID contextID = inputPort.getContextID();
        final String inputObject = inputPort.getData().getID();
        final DataTableSpec inputSpec = inputPort.getTableSpec();
        final DataType itemsetType = getColumnSpec(inputSpec, "item sets", true, settings.getItemsColumn()).getType();
        final FrequentItemSetJobInput jobInput = new FrequentItemSetJobInput(
            inputObject, outputObject, settings.getItemsColumn(), settings.getMinSupport());

        if (settings.overwriteNumPartitions()) {
            jobInput.setNumPartitions(settings.getNumPartitions());
        }

        LOGGER.info("Running frequent item sets Spark job");
        SparkContextUtil.getSimpleRunFactory(contextID, JOB_ID).createRun(jobInput).run(contextID, exec);
        LOGGER.info("Frequent item sets Spark job done.");

        return createSparkPortObject(inputPort, createFreqItemsTableSpec(itemsetType), outputObject);
    }

    /**
     * Returns spec of a column or throw exception if column is missing.
     *
     * @param spec input data table spec
     * @param colDesc description of column e.g. item sets, antecedent...
     * @param collection <code>true</code> if column should be a collection
     * @param colName name of column
     * @return spec of column
     * @throws InvalidSettingsException if column is missing or (not) contains collections
     */
    public static DataColumnSpec getColumnSpec(final DataTableSpec spec, final String colDesc, final boolean collection, final String colName) throws InvalidSettingsException {
        if (StringUtils.isBlank(colName)) {
            throw new InvalidSettingsException("No " + colDesc + " column selected.");
        }

        DataColumnSpec col = spec.getColumnSpec(colName);
        if (col == null) {
            throw new InvalidSettingsException("Input " + colDesc + " column '" + colName + "' not found.");
        } else if (collection && !col.getType().isCollectionType()) {
            throw new InvalidSettingsException("Input " + colDesc + " column '" + colName + "' is not a collection column.");
        } else if (!collection && col.getType().isCollectionType()) {
            throw new InvalidSettingsException("Selected " + colDesc + " column ''" + colName + "' contains a collection instead of single values.");
        } else {
            return col;
        }
    }

    /**
     * @param itemsetType data type of items
     * @return a frequent item sets data table spec
     */
    public static DataTableSpec createFreqItemsTableSpec(final DataType itemsetType) {
        DataColumnSpecCreator itemSet = new DataColumnSpecCreator("ItemSet", itemsetType);
        DataColumnSpecCreator itemSetSize = new DataColumnSpecCreator("ItemSetSize", IntCell.TYPE);
        DataColumnSpecCreator freq = new DataColumnSpecCreator("ItemSetSupport", LongCell.TYPE);
        return new DataTableSpec(itemSet.createSpec(), itemSetSize.createSpec(), freq.createSpec());
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
