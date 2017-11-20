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
 *   Created on 13.09.2015 by koetter
 */
package com.knime.bigdata.spark.node.preproc.filter.column;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.knime.core.node.util.filter.NameFilterConfiguration.FilterResult;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;

import com.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import com.knime.bigdata.spark.core.port.data.SparkDataPortObjectSpec;

/**
 *
 * @author Tobias Koetter, KNIME.com
 */
public class SparkColumnFilterNodeModel extends AbstractSparkColumnFilterNodeModel {


    private DataColumnSpecFilterConfiguration m_conf;

    /** A new configuration to store the settings. Also enables the type filter.
     * @return ...
     */
    public static final DataColumnSpecFilterConfiguration createDCSFilterConfiguration() {
        return new DataColumnSpecFilterConfiguration("column-filter");
    }

    /**Constructor.*/
    protected SparkColumnFilterNodeModel() {
        super(new PortType[]{SparkDataPortObject.TYPE}, new PortType[]{SparkDataPortObject.TYPE}, 0);
    }
    @Override
    protected ColumnRearranger createColumnRearranger(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        final SparkDataPortObjectSpec sparkSpec = (SparkDataPortObjectSpec)inSpecs[getSparkInputIdx()];
        return createColumnRearranger(sparkSpec.getTableSpec());
    }

    @Override
    protected ColumnRearranger createColumnRearranger(final PortObject[] inData) throws InvalidSettingsException {
        return createColumnRearranger(((SparkDataPortObject)inData[getSparkInputIdx()]).getTableSpec());
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

    /** Returns the object holding the include and exclude columns.
     * @param spec the spec to be applied to the current configuration.
     * @return filter result
     */
    protected FilterResult getFilterResult(final DataTableSpec spec) {
        if (m_conf == null) {
            return null;
        }
        return m_conf.applyTo(spec);
    }

    /**
     * Writes number of filtered columns, and the names as
     * {@link org.knime.core.data.DataCell} to the given settings.
     *
     * @param settings the object to save the settings into
     */
    @Override
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
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
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        DataColumnSpecFilterConfiguration conf = createDCSFilterConfiguration();
        conf.loadConfigurationInModel(settings);
        m_conf = conf;
    }

    /** {@inheritDoc} */
    @Override
    protected void validateAdditionalSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        DataColumnSpecFilterConfiguration conf = createDCSFilterConfiguration();
        conf.loadConfigurationInModel(settings);
    }
}
