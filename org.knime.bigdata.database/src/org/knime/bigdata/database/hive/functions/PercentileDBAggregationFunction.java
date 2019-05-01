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
import org.knime.core.data.LongValue;
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
public class PercentileDBAggregationFunction implements DBAggregationFunction {

    private static final String ID = "PERCENTILE";
    /**Factory for {@link PercentileDBAggregationFunction}.*/
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
            return new PercentileDBAggregationFunction();
        }
    }

    private PercentileFuntionSettingsPanel m_settingsPanel;
    private final PercentileFuntionSettings m_settings = new PercentileFuntionSettings(0.1);

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
        return type.isCompatible(LongValue.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return "Returns the exact percentile of the selected column in the group "
                + "(does not work with floating point types). The percentile parameter must be between 0 and 1. "
                + "NOTE: A true percentile can only be computed for integer values. "
                + "Use PERCENTILE_APPROX if your input is non-integral.";
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
            m_settingsPanel = new PercentileFuntionSettingsPanel(m_settings);
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
                + ", " + m_settings.getPercentile() + ")";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getSQLFragment4SubQuery(final String tableName, final String subQuery, final DBSQLDialect dialect) {
        return getLabel() + "((" + subQuery + ")," +  m_settings.getPercentile() + ")";
    }

}
