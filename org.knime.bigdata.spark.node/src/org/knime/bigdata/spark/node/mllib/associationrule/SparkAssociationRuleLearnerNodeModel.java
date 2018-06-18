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
package org.knime.bigdata.spark.node.mllib.associationrule;

import static org.knime.bigdata.spark.node.mllib.freqitemset.SparkFrequentItemSetNodeModel.createFreqItemsTableSpec;
import static org.knime.bigdata.spark.node.mllib.freqitemset.SparkFrequentItemSetNodeModel.getColumnSpec;
import static org.knime.bigdata.spark.node.mllib.freqitemset.SparkFrequentItemSetNodeModel.runFreqItemsJob;

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
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Frequent item sets and associations rules learning node model.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkAssociationRuleLearnerNodeModel extends SparkNodeModel {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkAssociationRuleLearnerNodeModel.class);

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkAssociationRuleLearnerNodeModel.class.getCanonicalName();

    private final SparkAssociationRuleLearnerSettings m_settings = new SparkAssociationRuleLearnerSettings();

    SparkAssociationRuleLearnerNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE});
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
        final DataColumnSpec itemsColumn = getColumnSpec(itemsSpec, "item sets", true, m_settings.getItemsColumn());

        return new PortObjectSpec[]{
            new SparkDataPortObjectSpec(contextId, createAssociationRulesSpec(itemsColumn.getType())),
            new SparkDataPortObjectSpec(contextId, createFreqItemsTableSpec(itemsColumn.getType()))
        };
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject itemsPort = (SparkDataPortObject) inData[0];
        final DataTableSpec itemsSpec = itemsPort.getTableSpec();
        final SparkContextID contextID = itemsPort.getContextID();

        final String freqItemSetsOutputObject = SparkIDs.createSparkDataObjectID();
        final DataColumnSpec itemsColumn = getColumnSpec(itemsSpec, "item sets", true, m_settings.getItemsColumn());
        final PortObject freqItemSetsPort = runFreqItemsJob(exec, itemsPort, freqItemSetsOutputObject, m_settings);

        final String rulesOutputObject = SparkIDs.createSparkDataObjectID();
        final DataTableSpec rulesOutputSpec = createAssociationRulesSpec(itemsColumn.getType());
        final AssociationRuleLearnerJobInput jobInput = new AssociationRuleLearnerJobInput(
            freqItemSetsOutputObject, rulesOutputObject, m_settings.getMinConfidence());

        LOGGER.info("Running association rules learner Spark job");
        SparkContextUtil.getSimpleRunFactory(contextID, JOB_ID).createRun(jobInput).run(contextID, exec);
        LOGGER.info("Association rules learner Spark job done.");

        return new PortObject[]{
            createSparkPortObject(itemsPort, rulesOutputSpec, rulesOutputObject),
            freqItemSetsPort
        };
    }

    private static DataTableSpec createAssociationRulesSpec(final DataType itemsCollectionType) {
        DataColumnSpecCreator consequent = new DataColumnSpecCreator("Consequent", itemsCollectionType.getCollectionElementType());
        DataColumnSpecCreator antecedent = new DataColumnSpecCreator("Antecedent", itemsCollectionType);
        DataColumnSpecCreator confidence = new DataColumnSpecCreator("RuleConfidence", DoubleCell.TYPE);
        DataColumnSpecCreator confidencePerc = new DataColumnSpecCreator("RuleConfidence%", DoubleCell.TYPE);
        return new DataTableSpec(consequent.createSpec(), antecedent.createSpec(), confidence.createSpec(), confidencePerc.createSpec());
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
