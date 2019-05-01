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
 *   Created on 23.09.2014 by koetter
 */
package org.knime.bigdata.database.hive.functions;

import java.awt.Component;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataType;
import org.knime.core.data.DoubleValue;
import org.knime.core.data.def.DoubleCell;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.database.dialect.DBSQLDialect;
import org.knime.database.function.aggregation.DBAggregationFunction;
import org.knime.database.function.aggregation.DBAggregationFunctionFactory;

/**
 *
 * @author Tobias Koetter, KNIME AG, Zurich, Switzerland
 */
public class PercentileApproxDBAggregationFunction implements DBAggregationFunction {

    private static final String ID = "PERCENTILE_APPROX";
    /**Factory for {@link PercentileApproxDBAggregationFunction}.*/
    public static final class Factory implements DBAggregationFunctionFactory {
        /**
         * {@inheritDoc}
         */
        @Override
        public String getId() {
            return ID;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public DBAggregationFunction createInstance() {
            return new PercentileApproxDBAggregationFunction();
        }
    }

    private PercentileApproxFuntionSettingsPanel m_settingsPanel;
    private final PercentileApproxFuntionSettings m_settings = new PercentileApproxFuntionSettings(0.1, 10000);

    /**
     * {@inheritDoc}
     */
    @Override
    public String getId() {
        return ID;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getLabel() {
        return getId();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isCompatible(final DataType type) {
        return type.isCompatible(DoubleValue.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return "Returns an approximate percentile of a numeric column (including floating point types) in the "
                + "group. The aproximation parameter controls approximation accuracy at the cost of memory. "
                + "Higher values yield better approximations, and the default is 10,000. When the number of distinct "
                + "values in the selected column is smaller than the approximation value, this gives an exact "
                + "percentile value.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasOptionalSettings() {
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Component getSettingsPanel() {
        if (m_settingsPanel == null) {
            m_settingsPanel = new PercentileApproxFuntionSettingsPanel(m_settings);
        }
        return m_settingsPanel;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadValidatedSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec spec)
            throws NotConfigurableException {
        try {
            m_settings.loadSettingsFrom(settings);
        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_settings.saveSettingsTo(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_settings.validateSettings(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void validate() throws InvalidSettingsException {
        //nothing to validate
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void configure(final DataTableSpec spec) throws InvalidSettingsException {
        //nothing to check or do
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DataType getType(final DataType originalType) {
        return DoubleCell.TYPE;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getColumnName() {
        return getLabel() + "_" + m_settings.getPercentile();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSQLFragment(final String tableName, final String columnName, final DBSQLDialect dialect) {

        return getLabel() + "("
                + tableName + "." + columnName
                + ", " + m_settings.getPercentile() + ", " + m_settings.getApprox() + ")";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSQLFragment4SubQuery(final String tableName, final String subQuery, final DBSQLDialect dialect) {
        return getLabel() + "((" + subQuery + ")," +  m_settings.getPercentile() + ", "+ m_settings.getApprox() + ")";
    }

}
