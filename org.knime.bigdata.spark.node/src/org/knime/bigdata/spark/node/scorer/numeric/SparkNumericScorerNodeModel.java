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
 *   Created on 30.09.2015 by Bjoern Lohrmann
 */
package org.knime.bigdata.spark.node.scorer.numeric;

import java.util.Map;

import org.knime.base.node.mine.scorer.numeric.NumericScorerSettings;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.workflow.FlowVariable;

import org.knime.bigdata.spark.core.context.SparkContextUtil;
import org.knime.bigdata.spark.core.job.JobRunFactory;
import org.knime.bigdata.spark.core.node.SparkNodeModel;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;
import org.knime.bigdata.spark.node.scorer.accuracy.ScorerJobInput;

/**
 * Node model for Spark Numeric Scorer node. Provides the same settings as the regular {@link org.knime.base.node.mine.scorer.numeric.NumericScorerNodeModel}.
 *
 * TODO: Remove code duplicates with {@link org.knime.base.node.mine.scorer.numeric.NumericScorerNodeModel} as much as possible
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkNumericScorerNodeModel extends SparkNodeModel {

    /** The unique Spark job id. */
    public static final String JOB_ID = SparkNumericScorerNodeModel.class.getCanonicalName();

    /** The node logger for this class. */
    protected static final NodeLogger LOGGER = NodeLogger.getLogger(SparkNumericScorerNodeModel.class);

    private final NumericScorerSettings m_numericScorerSettings = new NumericScorerSettings();

    private SparkNumericScorerViewData m_viewData;

    /** Constructor. */
    public SparkNumericScorerNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{BufferedDataTable.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final DataTableSpec inSpec = ((SparkDataPortObjectSpec)inSpecs[0]).getTableSpec();

        final DataColumnSpec reference = inSpec.getColumnSpec(m_numericScorerSettings.getReferenceColumnName());
        if (reference == null) {
            if (m_numericScorerSettings.getReferenceColumnName().equals(NumericScorerSettings.DEFAULT_REFERENCE)) {
                throw new InvalidSettingsException("No columns selected for reference");
            }
            throw new InvalidSettingsException("No such column in input table: " + m_numericScorerSettings.getReferenceColumnName());
        }
        if (!reference.getType().isCompatible(DoubleValue.class)) {
            throw new InvalidSettingsException("The reference column (" + m_numericScorerSettings.getReferenceColumnName()
                + ") is not double valued: " + reference.getType());
        }
        final DataColumnSpec predicted = inSpec.getColumnSpec(m_numericScorerSettings.getPredictionColumnName());
        if (predicted == null) {
            if (m_numericScorerSettings.getPredictionColumnName().equals(NumericScorerSettings.DEFAULT_PREDICTED)) {
                throw new InvalidSettingsException("No columns selected for prediction");
            }
            throw new InvalidSettingsException("No such column in input table: " + m_numericScorerSettings.getPredictionColumnName());
        }
        if (!predicted.getType().isCompatible(DoubleValue.class)) {
            throw new InvalidSettingsException("The prediction column (" + m_numericScorerSettings.getPredictionColumnName()
                + ") is not double valued: " + predicted.getType());
        }
        pushFlowVars(true);
        return new DataTableSpec[]{createOutputSpec(inSpec)};
    }

    /**
     * @param spec Input table spec.
     * @return Output table spec.
     */
    private DataTableSpec createOutputSpec(final DataTableSpec spec) {
        String o = m_numericScorerSettings.getOutputColumnName();
        final String output = m_numericScorerSettings.doOverride() ? o : m_numericScorerSettings.getPredictionColumnName();
        return new DataTableSpec("Scores", new DataColumnSpecCreator(output, DoubleCell.TYPE).createSpec());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void resetInternal() {
        m_viewData = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject inPort = (SparkDataPortObject)inData[0];
        final DataTableSpec tableSpec = inPort.getTableSpec();

        final JobRunFactory<ScorerJobInput, NumericScorerJobOutput> runFactory = SparkContextUtil.getJobRunFactory(inPort.getContextID(), JOB_ID);
        final ScorerJobInput jobInput = new ScorerJobInput(inPort.getTableName(), tableSpec.findColumnIndex(m_numericScorerSettings.getReferenceColumnName()),
            tableSpec.findColumnIndex(m_numericScorerSettings.getPredictionColumnName()));
        final NumericScorerJobOutput jobOutput = runFactory.createRun(jobInput).run(inPort.getContextID(), exec);

        m_viewData = new SparkNumericScorerViewData(jobOutput.getR2(), jobOutput.getAbsError(),
            jobOutput.getSquaredError(), jobOutput.getRootSquaredError(), jobOutput.getSignedDiff());

        pushFlowVars(false);
        return new BufferedDataTable[]{createOutputTable(exec, tableSpec)};
    }

    private BufferedDataTable createOutputTable(final ExecutionContext exec, final DataTableSpec tableSpec) {
        BufferedDataContainer container = exec.createDataContainer(createOutputSpec(tableSpec));

        container.addRowToTable(new DefaultRow("R^2", m_viewData.getRSquare()));
        container.addRowToTable(new DefaultRow("mean absolute error", m_viewData.getMeanAbsError()));
        container.addRowToTable(new DefaultRow("mean squared error", m_viewData.getMeanSquaredError()));
        container.addRowToTable(new DefaultRow("root mean squared deviation", m_viewData.getRootMeanSquaredDeviation()));
        container.addRowToTable(new DefaultRow("mean signed difference", m_viewData.getMeanSignedDifference()));
        container.close();
        return container.getTable();
    }

    /**
     * Pushes the results to flow variables.
     *
     * @param isConfigureOnly true enable overwriting check
     */
    private void pushFlowVars(final boolean isConfigureOnly) {

        if (m_numericScorerSettings.doFlowVariables()) {
            Map<String, FlowVariable> vars = getAvailableFlowVariables();

            String prefix = m_numericScorerSettings.getFlowVariablePrefix();
            String rsquareName = prefix + "R^2";
            String meanAbsName = prefix + "mean absolute error";
            String meanSquareName = prefix + "mean squared error";
            String rootmeanName = prefix + "root mean squared deviation";
            String meanSignedName = prefix + "mean signed difference";
            if (isConfigureOnly
                && (vars.containsKey(rsquareName) || vars.containsKey(meanAbsName) || vars.containsKey(meanSquareName)
                    || vars.containsKey(rootmeanName) || vars.containsKey(meanSignedName))) {
                addWarning("A flow variable was replaced!");
            }

            double rsquare = isConfigureOnly ? 0.0 : m_viewData.getRSquare();
            double meanAbs = isConfigureOnly ? 0.0 : m_viewData.getMeanAbsError();
            double meanSquare = isConfigureOnly ? 0 : m_viewData.getMeanSquaredError();
            double rootmean = isConfigureOnly ? 0 : m_viewData.getRootMeanSquaredDeviation();
            double meanSigned = isConfigureOnly ? 0 : m_viewData.getMeanSignedDifference();
            pushFlowVariableDouble(rsquareName, rsquare);
            pushFlowVariableDouble(meanAbsName, meanAbs);
            pushFlowVariableDouble(meanSquareName, meanSquare);
            pushFlowVariableDouble(rootmeanName, rootmean);
            pushFlowVariableDouble(meanSignedName, meanSigned);
        }
    }

    /**
     * @param string
     */
    private void addWarning(final String string) {
        String warningMessage = getWarningMessage();
        if (warningMessage == null || warningMessage.isEmpty()) {
            setWarningMessage(string);
        } else {
            setWarningMessage(warningMessage + "\n" + string);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_numericScorerSettings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
       m_numericScorerSettings.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
       m_numericScorerSettings.validateSettings(settings);
    }

    /**
     * @return the view data
     */
    public SparkNumericScorerViewData getViewData() {
        return m_viewData;
    }


}
