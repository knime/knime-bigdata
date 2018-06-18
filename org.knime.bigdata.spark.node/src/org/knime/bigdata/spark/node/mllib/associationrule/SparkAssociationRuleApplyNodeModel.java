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

import static org.knime.bigdata.spark.node.mllib.freqitemset.SparkFrequentItemSetNodeModel.getColumnSpec;

import org.knime.bigdata.spark.core.context.SparkContextID;
import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.core.util.SparkIDs;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
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
        super(new PortType[]{SparkDataPortObject.TYPE, SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE});
    }

    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 2 || inSpecs[0] == null || inSpecs[1] == null) {
            throw new InvalidSettingsException("Association rules and input data required.");
        }
        final SparkDataPortObjectSpec rulesPortSpec = (SparkDataPortObjectSpec) inSpecs[0];
        final DataTableSpec rulesSpec = rulesPortSpec.getTableSpec();
        final SparkDataPortObjectSpec itemsPortSpec = (SparkDataPortObjectSpec) inSpecs[1];
        final DataTableSpec itemsSpec = itemsPortSpec.getTableSpec();
        final SparkContextID contextID = itemsPortSpec.getContextID();

        m_settings.loadDefaults(rulesSpec, itemsSpec);

        final DataColumnSpec antColumn = getColumnSpec(rulesSpec, "antecedent", true, m_settings.getAntecedentColumn());
        final DataColumnSpec conseqColumn = getColumnSpec(rulesSpec, "consequent", false, m_settings.getConsequentColumn());
        final DataColumnSpec itemsColumn = getColumnSpec(itemsSpec, "item sets", true, m_settings.getItemColumn());
        final String outputColumnName = getOutputColumnName(itemsSpec);

        if (!itemsColumn.getType().equals(antColumn.getType())) {
            throw new InvalidSettingsException("Data types of item sets and antecedent column are incompatible.");
        } else if (!itemsColumn.getType().getCollectionElementType().equals(conseqColumn.getType())) {
            throw new InvalidSettingsException("Data types of item sets and consequents column are incompatible.");
        }

        final DataColumnSpecCreator outputColumn = new DataColumnSpecCreator(outputColumnName, itemsColumn.getType());
        final DataTableSpec outputSpec = new DataTableSpec(itemsSpec, new DataTableSpec(outputColumn.createSpec()));
        return new PortObjectSpec[]{new SparkDataPortObjectSpec(contextID, outputSpec)};
    }

    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rulesPort = (SparkDataPortObject) inData[0];
        final SparkDataPortObject itemsPort = (SparkDataPortObject) inData[1];
        final SparkContextID contextID = rulesPort.getContextID();
        final DataTableSpec itemsSpec = itemsPort.getTableSpec();
        final DataColumnSpec itemsColumn = getColumnSpec(itemsSpec, "item sets", true, m_settings.getItemColumn());
        final String outputObject = SparkIDs.createSparkDataObjectID();
        final String outputColumnName = getOutputColumnName(itemsSpec);
        final AssociationRuleApplyJobInput jobInput = new AssociationRuleApplyJobInput(
            rulesPort.getData().getID(), m_settings.getAntecedentColumn(), m_settings.getConsequentColumn(),
            itemsPort.getData().getID(), m_settings.getItemColumn(),
            outputObject, outputColumnName);

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

        final DataColumnSpecCreator outputColumn = new DataColumnSpecCreator(outputColumnName, itemsColumn.getType());
        final DataTableSpec outputSpec = new DataTableSpec(itemsSpec, new DataTableSpec(outputColumn.createSpec()));
        return new PortObject[]{createSparkPortObject(itemsPort, outputSpec, outputObject)};
    }

    /** @return unique output column name */
    private String getOutputColumnName(final DataTableSpec inputSpec) {
        return DataTableSpec.getUniqueColumnName(inputSpec, m_settings.getOutputColumn());
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
