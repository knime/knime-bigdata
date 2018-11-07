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
 *   Created on Nov 5, 2018 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.preproc.filter.row;

import org.knime.bigdata.spark.node.preproc.filter.row.operator.SparkOperatorRegistry;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.rowfilter.component.RowFilterComponent;
import org.knime.core.node.rowfilter.component.RowFilterConfig;

/**
 * Spark row filter node settings.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkRowFilterSettings {

    private static final String CFG_CONDITIONS = "conditions";

    private final RowFilterConfig m_rowFilterConfig =
        new RowFilterConfig(CFG_CONDITIONS, SparkRowFilterNodeModel.GROUP_TYPES);

    /** @return row filter configuration */
    public RowFilterConfig getRowFilterConfig() {
        return m_rowFilterConfig;
    }

    /**
     * Saves current configuration to the Node settings.
     *
     * @param settings the {@link NodeSettingsWO}
     */
    protected void saveAdditionalSettingsTo(final NodeSettingsWO settings) {
        m_rowFilterConfig.saveSettingsTo(settings);
    }

    /**
     * Validates configuration before loading from the Node settings.
     *
     * @param settings the {@link NodeSettingsRO}
     * @param tableSpec the {@linkplain DataTableSpec input table specification} to check
     * @throws InvalidSettingsException if settings are invalid or can't be retrieved
     */
    protected void validateAdditionalSettings(final NodeSettingsRO settings, final DataTableSpec tableSpec) throws InvalidSettingsException {
        m_rowFilterConfig.validateSettings(settings, tableSpec, SparkOperatorRegistry.getInstance());
    }

    /**
     * Validates the current state of {@link RowFilterComponent}.
     *
     * @param tableSpec the {@linkplain DataTableSpec input table specification} to check
     * @throws InvalidSettingsException if any issue found during the validation process
     */
    public void validate(final DataTableSpec tableSpec) throws InvalidSettingsException {
        m_rowFilterConfig.validate(tableSpec, SparkOperatorRegistry.getInstance());
    }

    /**
     * Loads configuration from the Node settings.
     *
     * @param settings the {@link NodeSettingsRO}
     * @throws InvalidSettingsException if settings are invalid or can't be retrieved
     */
    protected void loadAdditionalValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_rowFilterConfig.loadValidatedSettingsFrom(settings);
    }
}
