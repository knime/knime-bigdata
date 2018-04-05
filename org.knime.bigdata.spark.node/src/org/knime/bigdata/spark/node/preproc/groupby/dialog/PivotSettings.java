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

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.node.preproc.groupby.SparkGroupByJobInput;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataValue;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.BufferedDataTable;
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
    protected static final String MODE_ALL_VALUES = "all";

    /** load values from input table */
    protected static final String MODE_INPUT_TABLE = "inputTable";

    /** manual define values mode */
    protected static final String MODE_MANUAL_VALUES = "manual";

    private static final String CFG_MODE = "pivot.mode";
    private final SettingsModelString m_mode = new SettingsModelString(CFG_MODE, MODE_ALL_VALUES);

    private static final String CFG_VALUES_LIMIT = "pivot.valuesLimit";
    private final SettingsModelIntegerBounded m_valuesLimit = new SettingsModelIntegerBounded(CFG_VALUES_LIMIT, 500, 1, 10000);

    private static final String CFG_VALUES = "pivot.values";
    private final SettingsModelStringArray m_values = new SettingsModelStringArray(CFG_VALUES, new String[0]);

    private static final String CFG_IGNORE_MISSING_VALUES = "pivot.ignoreMissingValues";
    private final SettingsModelBoolean m_ignoreMissingValues = new SettingsModelBoolean(CFG_IGNORE_MISSING_VALUES, true);

    private static final String CFG_INPUT_VALUES_COLUMN = "pivot.inputValuesTableColumn";
    private final SettingsModelString m_inputValuesColumn = new SettingsModelString(CFG_INPUT_VALUES_COLUMN, "");

    private static final String CFG_VALIDATE_MANUAL_VALUES = "pivot.validateManualValues";
    private final SettingsModelBoolean m_validateManualValues = new SettingsModelBoolean(CFG_VALIDATE_MANUAL_VALUES, false);

    /** @return column name model */
    public SettingsModelString getColumnModel() { return m_column; }

    /** @return column name */
    public String getColumn() { return m_column.getStringValue(); }

    /** @return pivot mode model */
    public SettingsModelString getModeModel() { return m_mode; }

    /** @return <code>true</code> if auto values mode is selected */
    public boolean isAutoValuesMode() { return m_mode.getStringValue().equals(MODE_ALL_VALUES); }

    /** @return <code>true</code> if manual values mode is selected */
    public boolean isManualValuesMode() { return m_mode.getStringValue().equals(MODE_MANUAL_VALUES); }

    /** @return values limit model */
    public SettingsModelIntegerBounded getValuesLimitModel() { return m_valuesLimit; }

    /** @return values limit in auto compute values or values from input table mode */
    public int getValuesLimit() { return m_valuesLimit.getIntValue(); }

    /** @return values model */
    public SettingsModelStringArray getValuesModel() { return m_values; }

    /** @return values, used in manual values mode */
    public String[] getValues() { return m_values.getStringArrayValue(); }

    /** @return use pivot as column model */
    public SettingsModelBoolean getIgnoreMissingValuesModel() { return m_ignoreMissingValues; }

    /** @return <code>true</code> if missing values in the pivot column should be ignored */
    public boolean ignoreMissingValues() { return m_ignoreMissingValues.getBooleanValue(); }

    /** @return pivot values input column name model */
    public SettingsModelString getInputValuesColumnModel() { return m_inputValuesColumn; }

    /** @return column name of pivot values input table to use */
    public String getInputValuesColumn() { return m_inputValuesColumn.getStringValue(); }

    /** @return validate manual values model */
    public SettingsModelBoolean getValidateManualValuesModel() { return m_validateManualValues; }

    /** @return <code>true</code> if manual defined values list should be verified against pivot values in spark data frame */
    public boolean validateManualValues() { return m_validateManualValues.getBooleanValue(); }

    /**
     * Validates if given settings contains a non empty input values column configuration.
     * @param settings The {@link org.knime.core.node.NodeSettings} to read from.
     * @return <code>true</code> if settings contains a non empty input values column configuration
     */
    public boolean containsInputValuesColumnConfig(final NodeSettingsRO settings) {
        return !StringUtils.isBlank(settings.getString(CFG_INPUT_VALUES_COLUMN, ""));
    }

    /**
     * Write values from this into given configuration object.
     *
     * @param settings The {@link org.knime.core.node.NodeSettings} to write trough.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_column.saveSettingsTo(settings);
        m_mode.saveSettingsTo(settings);
        m_valuesLimit.saveSettingsTo(settings);
        m_ignoreMissingValues.saveSettingsTo(settings);
        m_inputValuesColumn.saveSettingsTo(settings);
        m_values.saveSettingsTo(settings);
        m_validateManualValues.saveSettingsTo(settings);
    }

    /**
     * Read value(s) of this component model from configuration object.
     *
     * @param settings The {@link org.knime.core.node.NodeSettings} to read from.
     * @throws InvalidSettingsException if loading fails
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_column.loadSettingsFrom(settings);
        m_mode.loadSettingsFrom(settings);
        m_valuesLimit.loadSettingsFrom(settings);
        m_ignoreMissingValues.loadSettingsFrom(settings);
        m_inputValuesColumn.loadSettingsFrom(settings);
        m_values.loadSettingsFrom(settings);
        m_validateManualValues.loadSettingsFrom(settings);
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
        m_valuesLimit.validateSettings(settings);
        m_ignoreMissingValues.validateSettings(settings);
        m_inputValuesColumn.validateSettings(settings);
        m_values.validateSettings(settings);
        m_validateManualValues.validateSettings(settings);
    }

    /**
     * Add pivot configuration to given job input.
     * @param jobInput destination job input
     */
    public void addJobConfig(final SparkGroupByJobInput jobInput) {
        jobInput.setPivotColumn(getColumn());
        if (isAutoValuesMode()) {
            jobInput.setComputePivotValues(true);
            jobInput.setComputePivotValuesLimit(getValuesLimit());
            jobInput.setIgnoreMissingValuesInPivotColumn(ignoreMissingValues());
        } else {
            jobInput.setComputePivotValues(false);
            jobInput.setPivotValues(getValues(), validateManualValues());
        }
    }

    /**
     * Add pivot configuration to given job input using a {@link BufferedDataTable} as pivot values.
     *
     * @param jobInput destination job input
     * @param valuesTable table with pivot values column
     * @return <code>true</code> if all values from input tables are used, <code>false</code> if limit has been reached
     *         and additional values are ignored
     */
    public boolean addJobConfig(final SparkGroupByJobInput jobInput, final BufferedDataTable valuesTable) {
        final String[] values = getValues(valuesTable);
        final int limit = getValuesLimit();

        jobInput.setPivotColumn(getColumn());
        jobInput.setComputePivotValues(false);

        if (values.length >= limit) {
            jobInput.setPivotValues(Arrays.copyOf(values, limit), validateManualValues());
            return false;
        } else {
            jobInput.setPivotValues(values, validateManualValues());
            return true;
        }
    }

    /**
     * Create values list from input table.
     * @param valuesTable table with pivot values
     * @return list with unique pivot values without a limit
     */
    public String[] getValues(final BufferedDataTable valuesTable) {
        final int columnIdx = valuesTable.getDataTableSpec().findColumnIndex(getInputValuesColumn());
        final CloseableRowIterator it = valuesTable.iterator();
        final HashSet<Serializable> values = new HashSet<>();
        while (it.hasNext()) {
            final DataRow row = it.next();
            if (row.getNumCells() >= columnIdx) {
                if (!row.getCell(columnIdx).isMissing()) {
                    values.add(row.getCell(columnIdx).toString());
                } else if (!ignoreMissingValues()) {
                    values.add(null);
                }
            }
        }

        return values.toArray(new String[0]);
    }

    /**
     * Try to guess the input values column name.
     * @param inSpecs specs with spark and data table spec
     */
    public void guessInputValuesColumn(final DataTableSpec[] inSpecs) {
        final DataTableSpec sparkTableSpec = inSpecs[0];
        final DataTableSpec pivotTableSpec = inSpecs[1];
        final DataColumnSpec sparkColumn = sparkTableSpec.getColumnSpec(getColumn());

        // pivot table contains compatible column with same name as pivot column
        if (pivotTableSpec.containsName(getColumn()) && isCompatile(sparkColumn, pivotTableSpec.getColumnSpec(getColumn()))) {
            m_inputValuesColumn.setStringValue(getColumn());

        // find any compatible column
        } else {
            for (int i = 0; i < pivotTableSpec.getNumColumns(); i++) {
                final DataColumnSpec col = pivotTableSpec.getColumnSpec(i);
                if (isCompatile(sparkColumn, col)) {
                    m_inputValuesColumn.setStringValue(col.getName());
                    break;
                }
            }
        }
    }

    /** @return <code>true</code> if <em>a</em> is compatible to any data value class of <em>b</em> */
    private boolean isCompatile(final DataColumnSpec a, final DataColumnSpec b) {
        for (Class<? extends DataValue> dv : b.getType().getValueClasses()) {
            if (a.getType().isCompatible(dv)) {
                return true;
            }
        }

        return false;
    }
}
