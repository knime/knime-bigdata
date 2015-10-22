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
package com.knime.bigdata.spark.node.statistics.correlation.column;

import org.knime.base.util.HalfDoubleMatrix;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.RowKey;
import org.knime.core.data.def.DefaultRow;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.BufferedDataContainer;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.node.statistics.correlation.CorrelationTask;
import com.knime.bigdata.spark.node.statistics.correlation.MLlibCorrelationMethod;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class MLlibCorrelationColumnNodeModel extends SparkNodeModel {

    private final SettingsModelString m_method = createMethodModel();

    private final SettingsModelString m_col1 = createCol1Model();

    private final SettingsModelString m_col2 = createCol2Model();

    /**
     * Constructor.
     */
    MLlibCorrelationColumnNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE},
            new PortType[]{BufferedDataTable.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelString createCol1Model() {
        return new SettingsModelString("column1", null);
    }

    /**
     * @return
     */
    static SettingsModelString createCol2Model() {
        return new SettingsModelString("column2", null);
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
        DataTableSpec tableSpec = spec.getTableSpec();
        getColIdx(tableSpec, m_col1);
        getColIdx(tableSpec, m_col2);
        return new PortObjectSpec[] {createResultSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject data = (SparkDataPortObject)inData[0];
        DataTableSpec tableSpec = data.getTableSpec();
        final int col1Idx = getColIdx(tableSpec, m_col1);
        final int col2Idx = getColIdx(tableSpec, m_col2);
        final MLlibCorrelationMethod method = MLlibCorrelationMethod.get(m_method.getStringValue());
        final CorrelationTask task = new CorrelationTask(data.getData(), new Integer[] {col1Idx, col2Idx},
            method.getMethod(), null, false);
        final HalfDoubleMatrix corr = task.execute(exec);
        final BufferedDataContainer container = exec.createDataContainer(createResultSpec());
        container.addRowToTable(new DefaultRow(RowKey.createRowKey(0), new DoubleCell(corr.get(0, 0))));
        container.close();
        return new PortObject[] {container.getTable()};
    }

    /**
     * @return
     */
    private DataTableSpec createResultSpec() {
        return new DataTableSpec((new DataColumnSpecCreator("Correlation coefficient", DoubleCell.TYPE)).createSpec());
    }

    private int getColIdx(final DataTableSpec tableSpec, final SettingsModelString col) throws InvalidSettingsException {
        final String colName = col.getStringValue();
        if (colName == null) {
            throw new InvalidSettingsException("Please select the column name");
        }
        int i = tableSpec.findColumnIndex(colName);
        if (i < 0) {
            throw new InvalidSettingsException("Column " + colName + " not found in input data");
        }
        return i;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_col1.saveSettingsTo(settings);
        m_col2.saveSettingsTo(settings);
        m_method.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_col1.validateSettings(settings);
        m_col2.validateSettings(settings);
        m_method.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_col1.loadSettingsFrom(settings);
        m_col2.loadSettingsFrom(settings);
        m_method.loadSettingsFrom(settings);
    }
}
