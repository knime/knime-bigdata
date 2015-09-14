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
package com.knime.bigdata.spark.node.preproc.filter.column;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;

import com.knime.bigdata.spark.node.SparkNodeModel;
import com.knime.bigdata.spark.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;
import com.knime.bigdata.spark.port.data.SparkDataTable;
import com.knime.bigdata.spark.util.SparkIDs;
import com.knime.bigdata.spark.util.SparkUtil;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkColumnFilterNodeModel extends SparkNodeModel {

    private DataColumnSpecFilterConfiguration m_conf;

    /**Constructor.*/
    protected SparkColumnFilterNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{SparkDataPortObject.TYPE});
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configureInternal(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        if (inSpecs == null || inSpecs.length <= 0 || inSpecs[0] == null) {
            throw new InvalidSettingsException("No input Spark RDD available");
        }
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec)inSpecs[0];
        ColumnRearranger c = createColumnRearranger(sparkSpec.getTableSpec());
        return new PortObjectSpec[] {new SparkDataPortObjectSpec(sparkSpec.getContext(), c.createSpec())};
    }

    /**
     * Creates the output data table spec according to the current settings.
     * Throws an InvalidSettingsException if columns are specified that don't
     * exist in the input table spec.
     */
    private ColumnRearranger createColumnRearranger(final DataTableSpec spec) {
        if (m_conf == null) {
            m_conf = createDCSFilterConfiguration();
            // auto-configure
            m_conf.loadDefaults(spec, true);
        }
        final FilterResult filter = getFilterResult(spec);
        final String[] incls = filter.getIncludes();
        final ColumnRearranger c = new ColumnRearranger(spec);
        c.keepOnly(incls);
        return c;
    }

    /** A new configuration to store the settings. Also enables the type filter.
     * @return ...
     */
    public static final  DataColumnSpecFilterConfiguration createDCSFilterConfiguration() {
        return new DataColumnSpecFilterConfiguration("column-filter");
    }

    /** Returns the object holding the include and exclude columns.
     * @param spec the spec to be applied to the current configuration.
     * @return filter result
     */
    private FilterResult getFilterResult(final DataTableSpec spec) {
        if (m_conf == null) {
            return null;
        }
        return m_conf.applyTo(spec);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] executeInternal(final PortObject[] inData, final ExecutionContext exec) throws Exception {
        final SparkDataPortObject rdd = (SparkDataPortObject) inData[0];
        final DataTableSpec tableSpec = rdd.getTableSpec();
        final Integer[] columnIndices = SparkUtil.getColumnIndices(tableSpec, m_conf.applyTo(tableSpec).getIncludes());
        final String resultTable = SparkIDs.createRDDID();
        final ColumnFilterTask task = new ColumnFilterTask(rdd.getData(), columnIndices, resultTable);
        task.execute(exec);
        final ColumnRearranger c = createColumnRearranger(tableSpec);
        DataTableSpec resultSpec = c.createSpec();
        return new PortObject[] {
            new SparkDataPortObject(new SparkDataTable(rdd.getContext(), resultTable, resultSpec))};
    }

    /**
     * Writes number of filtered columns, and the names as
     * {@link org.knime.core.data.DataCell} to the given settings.
     *
     * @param settings the object to save the settings into
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        if (m_conf != null) {
            m_conf.saveConfiguration(settings);
        }
    }

    /**
     * Reads the filtered columns.
     *
     * @param settings to read from
     * @throws InvalidSettingsException if the settings does not contain the
     *             size or a particular column key
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        DataColumnSpecFilterConfiguration conf = createDCSFilterConfiguration();
        conf.loadConfigurationInModel(settings);
        m_conf = conf;
    }

    /** {@inheritDoc} */
    @Override
    protected void validateSettings(final NodeSettingsRO settings)
            throws InvalidSettingsException {
        DataColumnSpecFilterConfiguration conf = createDCSFilterConfiguration();
        conf.loadConfigurationInModel(settings);
    }
}
