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
 *   Created on 26.06.2015 by koetter
 */
package com.knime.bigdata.spark.node.io.table.writer;

import org.knime.core.data.DataTable;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;

import com.knime.bigdata.spark.node.AbstractSparkNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.util.SparkDataTableCreator;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class Spark2TableNodeModel extends AbstractSparkNodeModel {

    private final SettingsModelIntegerBounded m_fetchSize = createFetchSizeModel();

    private final SettingsModelBoolean m_fetchAll = createFetchAllModel();
    /**Constructor.*/
    Spark2TableNodeModel() {
        super(new PortType[] {SparkDataPortObject.TYPE}, new PortType[] {BufferedDataTable.TYPE});
    }

    /**
     * @return
     */
    static SettingsModelBoolean createFetchAllModel() {
        return new SettingsModelBoolean("fetchAll", true);
    }

    /**
     * @return fetch size model
     */
    static SettingsModelIntegerBounded createFetchSizeModel() {
        SettingsModelIntegerBounded model = new SettingsModelIntegerBounded("fetchSize", 1000, 1, Integer.MAX_VALUE);
        model.setEnabled(false);
        return model;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length != 1 || inSpecs[0] == null) {
            throw new InvalidSettingsException("Please connect the input port");
        }
        final SparkDataPortObject spec = (SparkDataPortObject)inSpecs[0];
        return new PortObjectSpec[] {spec.getTableSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        if (inData == null || inData.length != 1 || inData[0] == null) {
            throw new InvalidSettingsException("Please connect the input port");
        }
        exec.setMessage("Retrieving data from Spark...");
        final SparkDataPortObject dataObject = (SparkDataPortObject)inData[0];
        final DataTable dataTable;
        if (m_fetchAll.getBooleanValue()) {
            dataTable = SparkDataTableCreator.getDataTable(dataObject.getData());
        } else {
            dataTable = SparkDataTableCreator.getDataTable(dataObject.getData(), m_fetchSize.getIntValue());
        }
        exec.setMessage("Create KNIME data table...");
        final BufferedDataTable result = exec.createBufferedDataTable(dataTable, exec);
        return new PortObject[] {result};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        m_fetchSize.saveSettingsTo(settings);
        m_fetchAll.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fetchSize.validateSettings(settings);
        m_fetchAll.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_fetchSize.loadSettingsFrom(settings);
        m_fetchAll.loadSettingsFrom(settings);
    }

}
