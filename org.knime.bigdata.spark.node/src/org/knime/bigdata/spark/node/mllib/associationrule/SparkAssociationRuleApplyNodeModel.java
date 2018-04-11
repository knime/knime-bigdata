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
import org.knime.bigdata.spark.core.port.remotemodel.RemoteSparkModelPortObject;
import org.knime.bigdata.spark.core.port.remotemodel.RemoteSparkModelPortObjectSpec;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

/**
 * Associations rules apply node.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkAssociationRuleApplyNodeModel extends SparkNodeModel {
    private static final NodeLogger LOGGER = NodeLogger.getLogger(SparkAssociationRuleApplyNodeModel.class);

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkAssociationRuleApplyNodeModel.class.getCanonicalName();

    private final SparkAssociationRuleApplySettings m_settings = new SparkAssociationRuleApplySettings();

    SparkAssociationRuleApplyNodeModel() {
        super(new PortType[]{RemoteSparkModelPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("Spark items input model and data required.");
        }
        final RemoteSparkModelPortObjectSpec rulesPortSpec = (RemoteSparkModelPortObjectSpec) inSpecs[0];
        final SparkDataPortObjectSpec itemsPortSpec = (SparkDataPortObjectSpec) inSpecs[1];
        final DataTableSpec itemsTableSpec = itemsPortSpec.getTableSpec();
        final SparkContextID contextID = itemsPortSpec.getContextID();
        final AssociationRuleModelMetaData rulesMeta = (AssociationRuleModelMetaData) rulesPortSpec.getModelMetaData();

        m_settings.loadDefaults(itemsTableSpec);

        if (!rulesPortSpec.getModelName().equals(SparkAssociationRuleLearnerNodeModel.MODEL_NAME)) {
            throw new InvalidSettingsException("Invalid input model, conect a " + SparkAssociationRuleLearnerNodeModel.MODEL_NAME + " instead.");
        }

        if (m_settings.getItemColumn() == null) {
            throw new InvalidSettingsException("No items column selected.");
        }

        final DataType itemsColType = getItemsetColumn(itemsTableSpec, m_settings.getItemColumn()).getType();
        if (!itemsColType.equals(rulesMeta.getItemsetType())) {
            throw new InvalidSettingsException("Input column type and rules column type are not compatible.");
        }

        return new PortObjectSpec[]{
            new SparkDataPortObjectSpec(contextID, createOutputSpec(itemsTableSpec))
        };
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final RemoteSparkModelPortObject rulesPort = (RemoteSparkModelPortObject) inData[0];
        final SparkDataPortObject itemsPort = (SparkDataPortObject) inData[1];
        final SparkContextID contextID = itemsPort.getContextID();
        final String outputObject = SparkIDs.createSparkDataObjectID();
        final AssociationRuleApplyJobInput jobInput = new AssociationRuleApplyJobInput(
            rulesPort.getModel().getModel(), itemsPort.getData().getID(), outputObject,
            m_settings.getItemColumn(), m_settings.getOutputColumn());
        if (m_settings.hasRuleLimit()) {
            jobInput.setRuleLimit(m_settings.getRuleLimit());
        }

        LOGGER.info("Running association rules apply Spark job...");
        final AssociationRuleApplyJobOutput jobOutput = SparkContextUtil
            .<AssociationRuleApplyJobInput, AssociationRuleApplyJobOutput>getJobRunFactory(contextID, JOB_ID)
            .createRun(jobInput)
            .run(contextID, exec);
        LOGGER.info("Association rules learner Spark job done.");

        if (m_settings.hasRuleLimit() && m_settings.getRuleLimit() == jobOutput.getRuleCount()) {
            setWarningMessage("Using only " + jobOutput.getRuleCount() + " rules (limit reached).");
        }

        return new PortObject[]{
            createSparkPortObject(itemsPort, createOutputSpec(itemsPort.getTableSpec()), outputObject)
        };
    }

    /**
     * Return column spec of collection column with given name or throw exception if column is missing.
     *
     * @param spec input data table spec
     * @param name column name
     * @return column spec of column
     * @throws InvalidSettingsException if input column is missing or not a collection type
     */
    private DataColumnSpec getItemsetColumn(final DataTableSpec spec, final String name) throws InvalidSettingsException {
        DataColumnSpec col = spec.getColumnSpec(name);
        if (col == null) {
            throw new InvalidSettingsException("Input column " + m_settings.getItemColumn() + " not found.");
        } else if (!col.getType().isCollectionType()) {
            throw new InvalidSettingsException("Input column " + m_settings.getItemColumn() + " is not a collection.");
        } else {
            return col;
        }
    }

    private DataTableSpec createOutputSpec(final DataTableSpec inputSpec) throws InvalidSettingsException {
        final String outputName = DataTableSpec.getUniqueColumnName(inputSpec, m_settings.getOutputColumn());
        final DataColumnSpec itemsColumn = getItemsetColumn(inputSpec, m_settings.getItemColumn());
        final DataColumnSpecCreator outputColumn = new DataColumnSpecCreator(outputName, itemsColumn.getType());
        return new DataTableSpec(inputSpec, new DataTableSpec(outputColumn.createSpec()));
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
