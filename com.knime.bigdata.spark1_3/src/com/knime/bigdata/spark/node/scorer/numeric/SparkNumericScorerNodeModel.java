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
 *   Created on 30.09.2015 by Bjoern Lohrmann
 */
package com.knime.bigdata.spark.node.scorer.numeric;

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
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelColumnName;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.jobserver.server.NumericScorerData;
import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.node.scorer.accuracy.ScorerTask;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 * Node model for Spark Numeric Scorer node. Provides the same settings as the regular {@link org.knime.base.node.mine.scorer.numeric.NumericScorerNodeModel}.
 *
 * TODO: Remove code duplicates with {@link org.knime.base.node.mine.scorer.numeric.NumericScorerNodeModel} as much as possible
 *
 * @author Bjoern Lohrmann, KNIME.com
 */
public class SparkNumericScorerNodeModel extends SparkNodeModel {

    /** The node logger for this class. */
    protected static final NodeLogger LOGGER = NodeLogger.getLogger(SparkNumericScorerNodeModel.class);

    static final String CFGKEY_REFERENCE = "reference";

    static final String DEFAULT_REFERENCE = "";

    static final String CFGKEY_PREDICTED = "predicted";

    static final String DEFAULT_PREDICTED = "";

    static final String CFGKEY_OUTPUT = "output column";

    static final String DEFAULT_OUTPUT = "";

    private static final String CFGKEY_OVERRIDE_OUTPUT = "override default output name";

    static final boolean DEFAULT_OVERRIDE_OUTPUT = false;

    static final SettingsModelColumnName createReference() {
        return new SettingsModelColumnName(CFGKEY_REFERENCE, DEFAULT_REFERENCE);
    }

    static final SettingsModelColumnName createPredicted() {
        return new SettingsModelColumnName(CFGKEY_PREDICTED, DEFAULT_PREDICTED);
    }

    /**
     * @return The {@link SettingsModelBoolean} for the overriding output.
     */
    static SettingsModelBoolean createOverrideOutput() {
        return new SettingsModelBoolean(CFGKEY_OVERRIDE_OUTPUT, DEFAULT_OVERRIDE_OUTPUT);
    }

    /**
     * @return A new {@link SettingsModelString} for the output column name.
     */
    static SettingsModelString createOutput() {
        return new SettingsModelString(CFGKEY_OUTPUT, DEFAULT_OUTPUT);
    }

    private final SettingsModelColumnName m_reference = createReference();

    private final SettingsModelColumnName m_predicted = createPredicted();

    private final SettingsModelBoolean m_overrideOutput = createOverrideOutput();

    private final SettingsModelString m_outputColumnName = createOutput();

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

        final DataColumnSpec reference = inSpec.getColumnSpec(m_reference.getColumnName());
        if (reference == null) {
            if (m_reference.getColumnName().equals(DEFAULT_REFERENCE)) {
                throw new InvalidSettingsException("No columns selected for reference");
            }
            throw new InvalidSettingsException("No such column in input table: " + m_reference.getColumnName());
        }
        if (!reference.getType().isCompatible(DoubleValue.class)) {
            throw new InvalidSettingsException("The reference column (" + m_reference.getColumnName()
                + ") is not double valued: " + reference.getType());
        }
        final DataColumnSpec predicted = inSpec.getColumnSpec(m_predicted.getColumnName());
        if (predicted == null) {
            if (m_predicted.getColumnName().equals(DEFAULT_PREDICTED)) {
                throw new InvalidSettingsException("No columns selected for prediction");
            }
            throw new InvalidSettingsException("No such column in input table: " + m_predicted.getColumnName());
        }
        if (!predicted.getType().isCompatible(DoubleValue.class)) {
            throw new InvalidSettingsException("The prediction column (" + m_predicted.getColumnName()
                + ") is not double valued: " + predicted.getType());
        }
        return new DataTableSpec[]{createOutputSpec(inSpec)};
    }

    /**
     * @param spec Input table spec.
     * @return Output table spec.
     */
    private DataTableSpec createOutputSpec(final DataTableSpec spec) {
        String o = m_outputColumnName.getStringValue();
        final String output = m_overrideOutput.getBooleanValue() ? o : m_predicted.getColumnName();
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

        final NumericScorerData result =
            (NumericScorerData)new ScorerTask(inPort.getData(), tableSpec.findColumnIndex(m_reference.getColumnName()),
                tableSpec.findColumnIndex(m_predicted.getColumnName()), false).execute(exec);

        m_viewData = new SparkNumericScorerViewData(result.getRSquare(), result.getMeanAbsError(),
            result.getMeanSquaredError(), result.getRmsd(), result.getMeanSignedDifference());

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
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_reference.saveSettingsTo(settings);
        m_predicted.saveSettingsTo(settings);
        m_overrideOutput.saveSettingsTo(settings);
        m_outputColumnName.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_reference.loadSettingsFrom(settings);
        m_predicted.loadSettingsFrom(settings);
        m_overrideOutput.loadSettingsFrom(settings);
        m_outputColumnName.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_reference.validateSettings(settings);
        m_predicted.validateSettings(settings);
        m_overrideOutput.validateSettings(settings);
        m_outputColumnName.validateSettings(settings);
    }

    /**
     * @return the view data
     */
    public SparkNumericScorerViewData getViewData() {
        return m_viewData;
    }
}
