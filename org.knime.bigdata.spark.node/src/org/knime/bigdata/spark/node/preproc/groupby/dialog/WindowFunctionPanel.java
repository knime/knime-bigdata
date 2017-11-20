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

import java.awt.Color;

import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingConstants;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.date.DateAndTimeValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentString;

import org.knime.bigdata.spark.node.sql_function.SparkSQLFunctionCombinationProvider;

/**
 * Panel representing time based window settings.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class WindowFunctionPanel implements ChangeListener {

    /** The default title of the panel to display in a dialog. */
    public static final String DEFAULT_TITLE = "Time Based Window";

    private final WindowFunctionSettings m_settings;

    private final JPanel m_panel;

    private final JLabel m_unsupportedVersionLabel;

    private final DialogComponentBoolean m_enabledComponent;

    private final DialogComponentColumnNameSelection m_columnComponent;

    private final DialogComponentString m_windowDurationComponent;

    private final DialogComponentString m_slideDurationComponent;

    private final DialogComponentString m_startTime;

    private boolean m_hasWindowFunction = false;

    /**
     * Panel representing time based window settings.
     * @param settings window settings to use
     */
    @SuppressWarnings("unchecked")
    public WindowFunctionPanel(final WindowFunctionSettings settings) {
        m_panel = new JPanel();
        m_panel.setLayout(new BoxLayout(m_panel, BoxLayout.PAGE_AXIS));
        m_settings = settings;
        m_unsupportedVersionLabel = new JLabel("Unsupported Spark Version");
        m_unsupportedVersionLabel.setHorizontalAlignment(SwingConstants.CENTER);
        m_unsupportedVersionLabel.setForeground(Color.RED);
        m_panel.add(m_unsupportedVersionLabel);

        m_enabledComponent = new DialogComponentBoolean(m_settings.getEnabledModel(), "Enabled");
        m_panel.add(m_enabledComponent.getComponentPanel());

        m_columnComponent = new DialogComponentColumnNameSelection(m_settings.getColumnModel(), "Column", 0, false, DateAndTimeValue.class);
        m_panel.add(m_columnComponent.getComponentPanel());

        m_windowDurationComponent = new DialogComponentString(m_settings.getWindowDurationModel(), "Window duration");
        m_panel.add(m_windowDurationComponent.getComponentPanel());

        m_slideDurationComponent = new DialogComponentString(m_settings.getSlideDurationModel(), "Slide duration (optional)");
        m_panel.add(m_slideDurationComponent.getComponentPanel());

        m_startTime = new DialogComponentString(m_settings.getStartTimeModel(), "Start time (optional)");
        m_panel.add(m_startTime.getComponentPanel());
    }

    /**
     * @return the panel in which all sub-components of this component are
     *         arranged. This panel can be added to the dialog pane.
     */
    public JPanel getComponentPanel() {
        return m_panel;
    }

    /**
     * @param settings {@link NodeSettingsRO}
     * @param functionProvider the {@link SparkSQLFunctionCombinationProvider}
     * @param spec the input {@link DataTableSpec}
     * @throws NotConfigurableException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings,
        final SparkSQLFunctionCombinationProvider functionProvider, final DataTableSpec spec)
        throws NotConfigurableException {

        m_enabledComponent.loadSettingsFrom(settings, new DataTableSpec[] { spec });
        m_columnComponent.loadSettingsFrom(settings, new DataTableSpec[] { spec });
        m_windowDurationComponent.loadSettingsFrom(settings, new DataTableSpec[] { spec });
        m_slideDurationComponent.loadSettingsFrom(settings, new DataTableSpec[] { spec });
        m_startTime.loadSettingsFrom(settings, new DataTableSpec[] { spec });

        m_unsupportedVersionLabel.setVisible(!functionProvider.hasWindowFunction());
        m_hasWindowFunction = functionProvider.hasWindowFunction();
        updateInputEnabledState();

        m_settings.getEnabledModel().addChangeListener(this);
        m_settings.getColumnModel().addChangeListener(this);
        m_settings.getWindowDurationModel().addChangeListener(this);
        m_settings.getSlideDurationModel().addChangeListener(this);
    }

    /**
     * @param settings the settings object to write to
     * @throws InvalidSettingsException on invalid settings
     */
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.getEnabledModel().removeChangeListener(this);
        m_settings.getColumnModel().removeChangeListener(this);
        m_settings.getWindowDurationModel().removeChangeListener(this);
        m_settings.getSlideDurationModel().removeChangeListener(this);

        m_enabledComponent.saveSettingsTo(settings);
        m_columnComponent.saveSettingsTo(settings);
        m_windowDurationComponent.saveSettingsTo(settings);
        m_slideDurationComponent.saveSettingsTo(settings);
        m_startTime.saveSettingsTo(settings);
    }

    /**
     * Validates the internal state of the window functions.
     * @throws InvalidSettingsException if the internal state of the function is invalid
     */
    public void validate() throws InvalidSettingsException {
        if (m_settings.getEnabledModel().getBooleanValue()) {
            if (StringUtils.isBlank(m_settings.getColumnModel().getStringValue())) {
                throw new InvalidSettingsException("Window column name required.");
            }

            if (StringUtils.isBlank(m_settings.getWindowDurationModel().getStringValue())) {
                throw new InvalidSettingsException("Window duration required.");
            }
        }
    }

    /** Enable or disable fields on changes */
    private void updateInputEnabledState() {
        boolean enabled = m_hasWindowFunction && m_settings.getEnabledModel().getBooleanValue();
        m_settings.getEnabledModel().setEnabled(m_hasWindowFunction);
        m_settings.getColumnModel().setEnabled(enabled);
        m_settings.getWindowDurationModel().setEnabled(enabled);
        m_settings.getSlideDurationModel().setEnabled(
            enabled && !StringUtils.isBlank(m_settings.getWindowDurationModel().getStringValue()));
        m_settings.getStartTimeModel().setEnabled(
            enabled && !StringUtils.isBlank(m_settings.getSlideDurationModel().getStringValue()));
    }

    @Override
    public void stateChanged(final ChangeEvent e) {
        updateInputEnabledState();
    }
}
