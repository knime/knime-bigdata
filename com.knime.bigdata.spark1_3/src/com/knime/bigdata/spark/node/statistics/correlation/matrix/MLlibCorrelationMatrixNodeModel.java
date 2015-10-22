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
 *   Created on 13.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.statistics.correlation.matrix;

import java.util.List;

import org.knime.base.node.preproc.correlation.pmcc.PMCCPortObjectAndSpec;
import org.knime.base.util.HalfDoubleMatrix;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTableHolder;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.node.mllib.MLlibNodeSettings;
import com.knime.bigdata.spark.node.mllib.MLlibSettings;
import com.knime.bigdata.spark.node.statistics.correlation.CorrelationTask;
import com.knime.bigdata.spark.node.statistics.correlation.MLlibCorrelationMethod;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.util.SparkIDs;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibCorrelationMatrixNodeModel extends SparkNodeModel
implements BufferedDataTableHolder {

    private final MLlibNodeSettings m_settings = new MLlibNodeSettings(false);

    private final SettingsModelString m_method = createMethodModel();

    private BufferedDataTable m_correlationTable;

    /**
     * Constructor.
     */
    MLlibCorrelationMatrixNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{SparkDataPortObject.TYPE, BufferedDataTable.TYPE,
            PMCCPortObjectAndSpec.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelString createMethodModel() {
        return new SettingsModelString("correlationMethod", MLlibCorrelationMethod.getDefault().getActionCommand());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length < 1 || inSpecs[0] ==  null) {
            return null;
        }
        final SparkDataPortObjectSpec spec = (SparkDataPortObjectSpec) inSpecs[0];
        m_settings.check(spec.getTableSpec());
        final MLlibSettings settings = m_settings.getSettings(spec.getTableSpec());
        final String[] includes = settings.getFatureColNames().toArray(new String[0]);
        final DataTableSpec resultSpec = createSpec(settings);
        return new PortObjectSpec[] {new SparkDataPortObjectSpec(spec.getContext(), resultSpec),
            PMCCPortObjectAndSpec.createOutSpec(includes), new PMCCPortObjectAndSpec(includes)};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inData[0];
        final MLlibSettings settings = m_settings.getSettings(data.getTableSpec());
        final String resultRDD = SparkIDs.createRDDID();
        final DataTableSpec resultSpec = createSpec(settings);
        final MLlibCorrelationMethod method = MLlibCorrelationMethod.get(m_method.getStringValue());
        final CorrelationTask task = new CorrelationTask(data.getData(), settings.getFeatueColIdxs(),
            method.getMethod(), resultRDD, true);
        final HalfDoubleMatrix correlationMatrix = task.execute(exec);
        final String[] includeNames = settings.getFatureColNames().toArray(new String[0]);
        final PMCCPortObjectAndSpec pmccModel =
                new PMCCPortObjectAndSpec(includeNames, correlationMatrix);
        final BufferedDataTable out = pmccModel.createCorrelationMatrix(exec);
        m_correlationTable = out;
        return new PortObject[] {createSparkPortObject(data, resultSpec, resultRDD), out, pmccModel};
    }

    /** {@inheritDoc} */
    @Override
    public BufferedDataTable[] getInternalTables() {
        return new BufferedDataTable[]{m_correlationTable};
    }

    /** {@inheritDoc} */
    @Override
    public void setInternalTables(final BufferedDataTable[] tables) {
        m_correlationTable = tables[0];
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void resetInternal() {
        m_correlationTable = null;
    }

    /**
     * @param settings
     * @return
     */
    private DataTableSpec createSpec(final MLlibSettings settings) {
        final List<String> featureColNames = settings.getFatureColNames();
        final DataColumnSpecCreator creator = new DataColumnSpecCreator("Dummy", DoubleCell.TYPE);
        final DataColumnSpec specs[] = new DataColumnSpec[featureColNames.size()];
        int idx = 0;
        for (final String colName : featureColNames) {
            creator.setName(colName);
            specs[idx++] = creator.createSpec();
        }
        return new DataTableSpec(specs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
        m_method.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
        m_method.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
        m_method.loadSettingsFrom(settings);
    }
}
