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
 *   Created on Nov 16, 2017 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.preproc.groupby.dialog;

import java.util.ArrayList;

import org.apache.commons.lang3.StringUtils;
import org.knime.base.node.preproc.groupby.ColumnNamePolicy;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

import org.knime.bigdata.spark.core.sql_function.SparkSQLFunctionJobInput;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataType;
import org.knime.bigdata.spark.core.types.intermediate.IntermediateDataTypes;

/**
 * Window Function Settings.
 * @author Sascha Wolke, KNIME GmbH
 */
public class WindowFunctionSettings {

    private static final String CFG_WINDOW_ENABLED = "window.enabled";
    private final SettingsModelBoolean m_enabled = new SettingsModelBoolean(CFG_WINDOW_ENABLED, false);

    private static final String CFG_WINDOW_COL = "window.column";
    private final SettingsModelString m_column = new SettingsModelString(CFG_WINDOW_COL, "");

    private static final String CFG_WINDOW_DURATION = "window.duration";
    private final SettingsModelString m_windowDuration = new SettingsModelString(CFG_WINDOW_DURATION, "");

    private static final String CFG_WINDOW_SLIDE_DURATION = "window.slideDuration";
    private final SettingsModelString m_slideDuration = new SettingsModelString(CFG_WINDOW_SLIDE_DURATION, "");

    private static final String CFG_WINDOW_START_TIME = "window.startTime";
    private final SettingsModelString m_startTime = new SettingsModelString(CFG_WINDOW_START_TIME, "");

    /** @return true if window function is enabled */
    public boolean isEnabled() { return m_enabled.getBooleanValue(); }

    /** @return enable windows function model */
    public SettingsModelBoolean getEnabledModel() { return m_enabled; }

    /** @return column name model */
    public SettingsModelString getColumnModel() { return m_column; }

    /** @return window duration model */
    public SettingsModelString getWindowDurationModel() { return m_windowDuration; }

    /** @return slide duration model */
    public SettingsModelString getSlideDurationModel() { return m_slideDuration; }

    /** @return start time model */
    public SettingsModelString getStartTimeModel() { return m_startTime; }

    /**
     * Write values from this into given configuration object.
     *
     * @param settings The {@link org.knime.core.node.NodeSettings} to write trough.
     */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        m_enabled.saveSettingsTo(settings);
        m_column.saveSettingsTo(settings);
        m_windowDuration.saveSettingsTo(settings);
        m_slideDuration.saveSettingsTo(settings);
        m_startTime.saveSettingsTo(settings);
    }

    /**
     * Read value(s) of this component model from configuration object.
     *
     * @param settings The {@link org.knime.core.node.NodeSettings} to read
     *            from.
     * @throws InvalidSettingsException if loading fails
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_enabled.loadSettingsFrom(settings);
        m_column.loadSettingsFrom(settings);
        m_windowDuration.loadSettingsFrom(settings);
        m_slideDuration.loadSettingsFrom(settings);
        m_startTime.loadSettingsFrom(settings);
    }


    /**
     * Validates that the window function settings can be loaded.
     *
     * @param settings The {@link org.knime.core.node.NodeSettings} to read from.
     * @throws InvalidSettingsException if validation fails
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_enabled.validateSettings(settings);
        m_column.validateSettings(settings);
        m_windowDuration.validateSettings(settings);
        m_slideDuration.validateSettings(settings);
        m_startTime.validateSettings(settings);
    }

    private String[] getArgs() {
        final ArrayList<String> args = new ArrayList<>();
        args.add(m_column.getStringValue());
        args.add(m_windowDuration.getStringValue());
        if (!StringUtils.isBlank(m_slideDuration.getStringValue())) {
            args.add(m_slideDuration.getStringValue());

            if (!StringUtils.isBlank(m_startTime.getStringValue())) {
                args.add(m_startTime.getStringValue());
            }
        }
        return args.toArray(new String[0]);
    }

    /**
     * @param policy {@link ColumnNamePolicy} to use
     * @return Generated column name
     */
    public String getColumnName(final ColumnNamePolicy policy) {
        switch (policy) {
            case KEEP_ORIGINAL_NAME:
                return m_column.getStringValue();
            case AGGREGATION_METHOD_COLUMN_NAME:
                return "window(" + StringUtils.join(getArgs(), ",") + ")";
            case COLUMN_NAME_AGGREGATION_METHOD:
                return m_column.getStringValue() + "(window)";
            default:
                return m_column.getStringValue();
        }
    }

    /**
     * @param factory spark side factory name
     * @param outColName output column name
     * @return Spark job input
     */
    public SparkSQLFunctionJobInput getSparkJobInput(final String factory, final String outColName) {
        final String args[] = getArgs();
        final IntermediateDataType argTypes[] = new IntermediateDataType[args.length];

        for (int i = 0; i < args.length; i++) {
            argTypes[i] = IntermediateDataTypes.STRING;
        }

        return new SparkSQLFunctionJobInput("window", factory, outColName, args, argTypes);
    }
}
