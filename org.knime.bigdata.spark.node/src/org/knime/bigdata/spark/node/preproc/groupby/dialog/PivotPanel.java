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

import static org.knime.bigdata.spark.node.preproc.groupby.dialog.PivotSettings.MODE_ALL_VALUES;
import static org.knime.bigdata.spark.node.preproc.groupby.dialog.PivotSettings.MODE_INPUT_TABLE;
import static org.knime.bigdata.spark.node.preproc.groupby.dialog.PivotSettings.MODE_MANUAL_VALUES;

import java.awt.BorderLayout;
import java.awt.CardLayout;
import java.awt.Component;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;

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

    private final DialogComponentColumnNameSelection m_column;
    private final DialogComponentButtonGroup m_mode;
    private final JPanel m_valuesPanel;

    private final JPanel m_valuesAutoPanel;
    private final DialogComponentNumber m_limitAutoValues;
    private final DialogComponentBoolean m_ignoreMVAutoValues;

    private final JPanel m_valuesInputPanel;
    private final DialogComponentColumnNameSelection m_inputValuesColumn;
    private final DialogComponentNumber m_limitInputValues;
    private final DialogComponentBoolean m_ignoreMVInputValues;

    private final PivotValuesPanel m_valuesManualPanel;

    private boolean m_hasValuesInputTable = false;

    /**
     * Panel representing pivoting settings.
     */
    @SuppressWarnings("unchecked")
    public PivotPanel() {
        GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);
        gbc.fill = GridBagConstraints.VERTICAL;
        gbc.insets = new Insets(0, 0, 0, 0);

        final JPanel panel = new JPanel(new GridBagLayout());

        // pivot column
        gbc.anchor = GridBagConstraints.EAST;
        panel.add(new JLabel("Pivot Column:"), gbc);
        gbc.gridx++;
        gbc.anchor = GridBagConstraints.WEST;
        m_column = new DialogComponentColumnNameSelection(m_settings.getColumnModel(), "", 0, true, DataValue.class);
        panel.add(m_column.getComponentPanel(), gbc);
        gbc.gridy++;

        // values mode
        gbc.gridx = 0;
        gbc.anchor = GridBagConstraints.EAST;
        panel.add(new JLabel("Pivot Values:"), gbc);
        gbc.gridx++;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(0, 5, 0, 0); // column panel (above) has an empty label in front
        m_mode = new DialogComponentButtonGroup(m_settings.getModeModel(), null, false,
            new String[] { "Use all values", "Use values from data table", "Manually specify values" },
            new String[] { MODE_ALL_VALUES, MODE_INPUT_TABLE, MODE_MANUAL_VALUES });
        m_mode.getButton(MODE_ALL_VALUES).setToolTipText("Pivot values will be determined based on the values in the chosen pivot column.");
        m_mode.getButton(MODE_INPUT_TABLE).setToolTipText("Pivot values are taken from a column in the connected data table.");
        m_mode.getButton(MODE_MANUAL_VALUES).setToolTipText("Pivot values are taken from a manually specified list.");
        panel.add(m_mode.getComponentPanel(), gbc);
        gbc.gridy++;

        // value options area
        m_valuesPanel = new JPanel();
        m_valuesPanel.setLayout(new CardLayout());
        panel.add(m_valuesPanel, gbc);

        // automatic values
        m_limitAutoValues = createLimitValuesComponent();
        m_ignoreMVAutoValues = createIgnoreMissingValuePanel();
        m_valuesAutoPanel =
            createValueOptionsPanel(m_limitAutoValues.getComponentPanel(), m_ignoreMVAutoValues.getComponentPanel());
        m_valuesPanel.add(m_valuesAutoPanel, MODE_ALL_VALUES);

        // values from input table
        m_inputValuesColumn = new DialogComponentColumnNameSelection(m_settings.getInputValuesColumnModel(), "Column with pivot values:", 1, false, DataValue.class);
        m_limitInputValues = createLimitValuesComponent();
        m_ignoreMVInputValues = createIgnoreMissingValuePanel();
        m_valuesInputPanel = createValueOptionsPanel(m_inputValuesColumn.getComponentPanel(),
            m_limitInputValues.getComponentPanel(), m_ignoreMVInputValues.getComponentPanel());
        m_valuesPanel.add(m_valuesInputPanel, MODE_INPUT_TABLE);

        // manual values
        m_valuesManualPanel = new PivotValuesPanel(m_settings.getValuesModel());
        m_valuesPanel.add(createValueOptionsPanel(m_valuesManualPanel.getComponentPanel()), MODE_MANUAL_VALUES);

        m_panel = new JPanel(new BorderLayout());
        m_panel.add(panel, BorderLayout.PAGE_START);
    }

    private DialogComponentNumber createLimitValuesComponent() {
        return new DialogComponentNumber(m_settings.getValuesLimitModel(), "Limit number of values:", 10);
    }

    private DialogComponentBoolean createIgnoreMissingValuePanel() {
        return new DialogComponentBoolean(m_settings.getIgnoreMissingValuesModel(), "Ignore missing values");
    }

    private JPanel createValueOptionsPanel(final Component ...components) {
        final JPanel container = new JPanel(new FlowLayout(FlowLayout.LEFT, 5, 0));
        final JPanel panel = new JPanel(new GridBagLayout());
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.fill = GridBagConstraints.VERTICAL;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.insets = new Insets(0, 0, 0, 0);
        gbc.gridx = 0;
        gbc.gridy = 0;

        for(Component comp : components) {
            panel.add(comp, gbc);
            gbc.gridy++;
        }

        container.add(panel);
        return container;
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
     * @param specs the input {@link DataTableSpec}
     * @throws NotConfigurableException if the settings are invalid
     */
    public void loadSettingsFrom(final NodeSettingsRO settings, final DataTableSpec[] specs)
        throws NotConfigurableException {

        m_column.loadSettingsFrom(settings, specs);
        m_mode.loadSettingsFrom(settings, specs);

        m_limitAutoValues.loadSettingsFrom(settings, specs);
        m_ignoreMVAutoValues.loadSettingsFrom(settings, specs);
        m_valuesManualPanel.loadSettingsFrom(settings);

        m_hasValuesInputTable = specs.length == 2 && specs[1] != null;
        if (m_hasValuesInputTable) {
            boolean containsInValColConf = m_settings.containsInputValuesColumnConfig(settings);
            m_inputValuesColumn.loadSettingsFrom(settings, specs);
            if (!containsInValColConf) {
                m_settings.guessInputValuesColumn(specs);
            }
        }
        updateAvailableModes();

        m_settings.getModeModel().addChangeListener(this);
    }

    /**
     * Enable/disable non available values mode and select an available mode.
     */
    private void updateAvailableModes() {
        final String valuesMode = m_settings.getModeModel().getStringValue();

        if (m_hasValuesInputTable) {
            if (!valuesMode.equals(MODE_INPUT_TABLE)) {
                m_settings.getModeModel().setStringValue(MODE_INPUT_TABLE); // do this before set enable!
            }

            m_mode.getButton(MODE_ALL_VALUES).setEnabled(false);
            m_mode.getButton(MODE_INPUT_TABLE).setEnabled(true);
            m_mode.getButton(MODE_MANUAL_VALUES).setEnabled(false);

        } else {
            if (valuesMode.equals(MODE_INPUT_TABLE)) {
                m_settings.getModeModel().setStringValue(MODE_ALL_VALUES); // do this before set enable!
            }

            m_mode.getButton(MODE_ALL_VALUES).setEnabled(true);
            m_mode.getButton(MODE_INPUT_TABLE).setEnabled(false);
            m_mode.getButton(MODE_MANUAL_VALUES).setEnabled(true);
        }

        final CardLayout cl = (CardLayout)m_valuesPanel.getLayout();
        cl.show(m_valuesPanel, m_settings.getModeModel().getStringValue());
    }

    /**
     * @param settings the settings object to write to
     * @throws InvalidSettingsException on invalid settings
     */
    public void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.getModeModel().removeChangeListener(this);

        m_column.saveSettingsTo(settings);
        m_mode.saveSettingsTo(settings);
        m_limitAutoValues.saveSettingsTo(settings);
        m_ignoreMVAutoValues.saveSettingsTo(settings);
        m_inputValuesColumn.saveSettingsTo(settings);
        m_valuesManualPanel.saveSettingsTo(settings);

        m_settings.getModeModel().removeChangeListener(this);
    }

    /**
     * Validates the internal state pivot configuration dialog.
     * @throws InvalidSettingsException if the internal state is invalid
     */
    public void validate() throws InvalidSettingsException {
        m_valuesPanel.validate();
    }



    @Override
    public void stateChanged(final ChangeEvent e) {
        updateAvailableModes();
    }
}
