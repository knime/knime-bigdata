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

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.port.data.SparkDataTable;
import org.knime.bigdata.spark.core.port.model.SparkModel;
import org.knime.bigdata.spark.core.port.remotemodel.RemoteSparkModelPortObject;
import org.knime.bigdata.spark.core.port.remotemodel.RemoteSparkModelPortObjectSpec;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.bigdata.spark.node.mllib.freqitemset.FrequentItemSetModelMetaData;
import org.knime.bigdata.spark.node.mllib.freqitemset.FrequentItemSetNodeModel;
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
 * Associations rules learning using frequent pattern mining.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class AssociationRuleLearnerNodeModel extends SparkNodeModel {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(AssociationRuleLearnerNodeModel.class);

    /** The unique Spark job id. */
    public static final String JOB_ID = AssociationRuleLearnerNodeModel.class.getCanonicalName();

    /** The unique model name. */
    public static final String MODEL_NAME = "Association Rules";

    private final AssociationRuleLearnerSettings m_settings = new AssociationRuleLearnerSettings();

    AssociationRuleLearnerNodeModel() {
        super(new PortType[]{RemoteSparkModelPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, RemoteSparkModelPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("Remtoe spark input model required.");
        }

        final RemoteSparkModelPortObjectSpec spec = (RemoteSparkModelPortObjectSpec)inSpecs[0];

        if (!spec.getModelName().equals(FrequentItemSetNodeModel.MODEL_NAME)) {
            throw new InvalidSettingsException("Invalid input model, frequent item model requiered.");
        }

        final SparkContextID contextId = spec.getContextID();
        final DataType itemsetType = ((FrequentItemSetModelMetaData) spec.getModelMetaData()).getItemsetType();
        final AssociationRuleModelMetaData modelMeta = new AssociationRuleModelMetaData(itemsetType);
        return new PortObjectSpec[]{
            new SparkDataPortObjectSpec(contextId, createAssociationRulesSpec(itemsetType)),
            new RemoteSparkModelPortObjectSpec(contextId, getSparkVersion(spec), MODEL_NAME, modelMeta)
        };
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final RemoteSparkModelPortObject modelPort = (RemoteSparkModelPortObject) inData[0];
        final SparkContextID contextID = modelPort.getContextID();
        final SparkModel freqItemsModel = modelPort.getModel();
        final DataType itemsetType = ((FrequentItemSetModelMetaData) freqItemsModel.getMetaData()).getItemsetType();
        final AssociationRuleModelMetaData modelMetaData = new AssociationRuleModelMetaData(itemsetType);

        final String associationRulesOutputObject = SparkIDs.createSparkDataObjectID();
        final AssociationRuleLearnerJobInput jobInput = new AssociationRuleLearnerJobInput(
            freqItemsModel.getModel(), associationRulesOutputObject, m_settings.getMinConfidence());

        LOGGER.info("Running association rules learner Spark job...");
        final AssociationRuleLearnerJobOutput jobOutput =
            SparkContextUtil.<AssociationRuleLearnerJobInput, AssociationRuleLearnerJobOutput>getJobRunFactory(contextID, JOB_ID)
                .createRun(jobInput)
                .run(contextID, exec);
        LOGGER.info("Association rules learner Spark job done.");

        final SparkModel ruleModel = new SparkModel(
            getSparkVersion(modelPort), MODEL_NAME, jobOutput.getModel(), freqItemsModel.getTableSpec(), null, modelMetaData);

        return new PortObject[]{
            new SparkDataPortObject(new SparkDataTable(contextID, associationRulesOutputObject, createAssociationRulesSpec(itemsetType))),
            new RemoteSparkModelPortObject(contextID, ruleModel)
        };
    }

    private DataTableSpec createAssociationRulesSpec(final DataType itemsCollectionType) {
        DataColumnSpecCreator antecedent = new DataColumnSpecCreator("antecedent", itemsCollectionType);
        DataColumnSpecCreator consequent = new DataColumnSpecCreator("consequent", itemsCollectionType);
        DataColumnSpecCreator confidence = new DataColumnSpecCreator("confidence", DoubleCell.TYPE);
        return new DataTableSpec(antecedent.createSpec(), consequent.createSpec(), confidence.createSpec());
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
