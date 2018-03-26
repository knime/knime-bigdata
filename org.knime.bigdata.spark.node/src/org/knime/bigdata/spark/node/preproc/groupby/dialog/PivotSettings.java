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
 */
package org.knime.bigdata.spark.node.preproc.groupby.dialog;

import org.knime.bigdata.spark.node.preproc.groupby.SparkGroupByJobInput;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelIntegerBounded;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.defaultnodesettings.SettingsModelStringArray;

/**
 * Pivot settings.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class PivotSettings {

    private static final String CFG_COLUMN = "pivot.column";
    private final SettingsModelString m_column = new SettingsModelString(CFG_COLUMN, "");

    /** automatic detect values mode */
    protected static final String MODE_AUTO_VALUES = "auto";

    /** manual define values mode */
    protected static final String MODE_MANUAL_VALUES = "manual";

    private static final String CFG_MODE = "pivot.mode";
    private final SettingsModelString m_mode = new SettingsModelString(CFG_MODE, MODE_AUTO_VALUES);

    private static final String CFG_AUTO_LIMIT = "pivot.autoLimit";
    private final SettingsModelIntegerBounded m_computeValuesLimit = new SettingsModelIntegerBounded(CFG_AUTO_LIMIT, 500, 1, 10000);

    private static final String CFG_VALUES = "pivot.values";
    private final SettingsModelStringArray m_values = new SettingsModelStringArray(CFG_VALUES, new String[0]);

    private static final String CFG_USE_PIVOT_AS_COL_NAME = "pivot.usePivotAsColName";
    private final SettingsModelBoolean m_ignoreMissingValues = new SettingsModelBoolean(CFG_USE_PIVOT_AS_COL_NAME, true);

    /** @return column name model */
    public SettingsModelString getColumnModel() { return m_column; }

    /** @return column name */
    public String getColumn() { return m_column.getStringValue(); }

    /** @return pivot mode model */
    public SettingsModelString getModeModel() { return m_mode; }

    /** @return <code>true</code> if auto values mode is selected */
    public boolean isAutoValuesMode() { return m_mode.getStringValue().equals(MODE_AUTO_VALUES); }

    /** @return <code>true</code> if manual values mode is selected */
    public boolean isManualValuesMode() { return m_mode.getStringValue().equals(MODE_MANUAL_VALUES); }

    /** @return auto limit model */
    public SettingsModelIntegerBounded getComputeValuesLimitModel() { return m_computeValuesLimit; }

    /** @return values count limit in auto compute values mode */
    public int getComputeValuesLimit() { return m_computeValuesLimit.getIntValue(); }

    /** @return values model */
    public SettingsModelStringArray getValuesModel() { return m_values; }

    /** @return values, used in manual values mode */
    public String[] getValues() { return m_values.getStringArrayValue(); }

    /** @return use pivot as column model */
    public SettingsModelBoolean getIgnoreMissingValuesModel() { return m_ignoreMissingValues; }

    /** @return <code>true</code> if missing values in the pivot column should be ignored */
    public boolean ignoreMissingValues() { return m_ignoreMissingValues.getBooleanValue(); }

    /**
     * Write values from this into given configuration object.
     *
     * @param settings The {@link org.knime.core.node.NodeSettings} to write trough.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_column.saveSettingsTo(settings);
        m_mode.saveSettingsTo(settings);
        m_computeValuesLimit.saveSettingsTo(settings);
        m_values.saveSettingsTo(settings);
        m_ignoreMissingValues.saveSettingsTo(settings);
    }

    /**
     * Read value(s) of this component model from configuration object.
     *
     * @param settings The {@link org.knime.core.node.NodeSettings} to read
     *            from.
     * @throws InvalidSettingsException if loading fails
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_column.loadSettingsFrom(settings);
        m_mode.loadSettingsFrom(settings);
        m_computeValuesLimit.loadSettingsFrom(settings);
        m_values.loadSettingsFrom(settings);
        m_ignoreMissingValues.loadSettingsFrom(settings);

        m_computeValuesLimit.setEnabled(isAutoValuesMode());
        m_values.setEnabled(isManualValuesMode());
    }

    /**
     * Validates that the window function settings can be loaded.
     *
     * @param settings The {@link org.knime.core.node.NodeSettings} to read from.
     * @throws InvalidSettingsException if validation fails
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_column.validateSettings(settings);
        m_mode.validateSettings(settings);
        m_computeValuesLimit.validateSettings(settings);
        m_values.validateSettings(settings);
        m_ignoreMissingValues.validateSettings(settings);
    }

    /**
     * Add pivot configuration to given job input.
     * @param jobInput destination job input
     * @param aggCount used aggregations count
     */
    public void addJobConfig(final SparkGroupByJobInput jobInput, final int aggCount) {
        jobInput.setPivotColumn(getColumn());
        if (isAutoValuesMode()) {
            jobInput.setComputePivotValues(true);
            jobInput.setComputePivotValuesLimit(getComputeValuesLimit());
            jobInput.setIgnoreMissingValuesInPivotColumn(ignoreMissingValues());
        } else {
            jobInput.setComputePivotValues(false);
            jobInput.setPivotValues(getValues());
        }
    }
}
