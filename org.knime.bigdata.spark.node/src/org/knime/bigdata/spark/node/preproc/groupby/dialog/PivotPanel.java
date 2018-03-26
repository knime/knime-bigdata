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

import static org.knime.bigdata.spark.node.preproc.groupby.dialog.PivotSettings.MODE_AUTO_VALUES;
import static org.knime.bigdata.spark.node.preproc.groupby.dialog.PivotSettings.MODE_MANUAL_VALUES;

import java.awt.BorderLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.base.filehandling.NodeUtils;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DataValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentColumnNameSelection;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;

/**
 * Panel representing pivoting settings.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class PivotPanel implements ChangeListener {

    /** The default title of the panel to display in a dialog. */
    public static final String DEFAULT_TITLE = "Pivot";

    private final PivotSettings m_settings = new PivotSettings();

    private final JPanel m_panel;

    private final JLabel m_columnLabel;
    private final DialogComponentColumnNameSelection m_column;
    private final JLabel m_modeLabel;
    private final DialogComponentButtonGroup m_mode;
    private final DialogComponentNumber m_limit;
    private final PivotValuesPanel m_valuesPanel;
    private final DialogComponentBoolean m_ignoreMissingValues;

    /**
     * Panel representing pivoting settings.
     */
    @SuppressWarnings("unchecked")
    public PivotPanel() {
        GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.fill = GridBagConstraints.VERTICAL;
        gbc.insets = new Insets(0, 0, 0, 0); // the dialog components already have some space around

        final JPanel panel = new JPanel(new GridBagLayout());

        gbc.gridwidth = 2;
        m_columnLabel = new JLabel("Column:");
        panel.add(m_columnLabel, gbc);
        gbc.gridx++;
        m_column = new DialogComponentColumnNameSelection(m_settings.getColumnModel(), "", 0, true, DataValue.class);
        panel.add(m_column.getComponentPanel(), gbc);
        gbc.gridy++;

        gbc.gridwidth = 1;
        gbc.gridx = 0;
        m_modeLabel = new JLabel("Values:");
        final Box labelBox = Box.createVerticalBox();
        labelBox.add(m_modeLabel);
        labelBox.add(Box.createVerticalGlue());
        panel.add(labelBox, gbc);
        gbc.gridx++;

        // automatic values
        final JPanel autoPanel = new JPanel();
        autoPanel.setLayout(new BoxLayout(autoPanel, BoxLayout.Y_AXIS));
        m_mode = new DialogComponentButtonGroup(m_settings.getModeModel(), null, true,
            new String[] { "Automatic", "Manual" },
            new String[] { MODE_AUTO_VALUES, MODE_MANUAL_VALUES });
        autoPanel.add(leftAlign(m_mode.getButton(MODE_AUTO_VALUES)));

        final Box autoOptions = Box.createHorizontalBox();
        autoOptions.add(Box.createHorizontalStrut(10));
        m_limit = new DialogComponentNumber(m_settings.getComputeValuesLimitModel(), "Limit", 10);
        autoOptions.add(m_limit.getComponentPanel());
        m_ignoreMissingValues = new DialogComponentBoolean(m_settings.getIgnoreMissingValuesModel(),
                "Ignore missing values");
        autoOptions.add(m_ignoreMissingValues.getComponentPanel());
        autoPanel.add(autoOptions);
        panel.add(autoPanel, gbc);
        gbc.gridy++;

        // manual values
        final JPanel manualPanel = new JPanel();
        manualPanel.setLayout(new BoxLayout(manualPanel, BoxLayout.Y_AXIS));
        manualPanel.add(leftAlign(m_mode.getButton(MODE_MANUAL_VALUES)));
        m_valuesPanel = new PivotValuesPanel(m_settings.getValuesModel());
        manualPanel.add(leftAlign(m_valuesPanel.getComponentPanel(), 10));
        panel.add(manualPanel, gbc);
        gbc.gridy++;

        m_panel = new JPanel(new BorderLayout());
        m_panel.add(panel, BorderLayout.PAGE_START);
    }

    private JComponent leftAlign(final JComponent component) {
        final Box box = Box.createHorizontalBox();
        box.add(component);
        box.add(Box.createHorizontalGlue());
        return box;
    }

    private JComponent leftAlign(final JComponent component, final int leftInset) {
        final Box box = Box.createHorizontalBox();
        box.add(Box.createHorizontalStrut(leftInset));
        box.add(component);
        box.add(Box.createHorizontalGlue());
        return box;
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
     * @param spec the input {@link DataTableSpec}
     * @throws NotConfigurableException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec spec)
        throws NotConfigurableException {

        m_column.loadSettingsFrom(settings, new DataTableSpec[] { spec });
        m_mode.loadSettingsFrom(settings, new DataTableSpec[] { spec });
        m_limit.loadSettingsFrom(settings, new DataTableSpec[] { spec });
        m_valuesPanel.loadSettingsFrom(settings);
        m_ignoreMissingValues.loadSettingsFrom(settings, new DataTableSpec[] { spec });

        updateInputEnabledState();

        m_settings.getModeModel().addChangeListener(this);
    }

    /**
     * @param settings the settings object to write to
     * @throws InvalidSettingsException on invalid settings
     */
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.getModeModel().removeChangeListener(this);

        m_column.saveSettingsTo(settings);
        m_mode.saveSettingsTo(settings);
        m_limit.saveSettingsTo(settings);
        m_valuesPanel.saveSettingsTo(settings);
        m_ignoreMissingValues.saveSettingsTo(settings);
    }

    /**
     * Validates the internal state pivot configuration dialog.
     * @throws InvalidSettingsException if the internal state is invalid
     */
    public void validate() throws InvalidSettingsException {
        m_valuesPanel.validate();
    }

    /** Enable or disable fields on changes */
    private void updateInputEnabledState() {
        m_limit.getModel().setEnabled(m_settings.isAutoValuesMode());
        m_ignoreMissingValues.getModel().setEnabled(m_settings.isAutoValuesMode());
        m_settings.getValuesModel().setEnabled(m_settings.isManualValuesMode());
    }

    @Override
    public void stateChanged(final ChangeEvent e) {
        // handle mode changes
        updateInputEnabledState();
    }
}
