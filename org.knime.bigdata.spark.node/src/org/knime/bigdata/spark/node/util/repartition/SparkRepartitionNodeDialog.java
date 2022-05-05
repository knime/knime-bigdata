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
 *   Created on May 6, 2019 by Sascha Wolke, KNIME GmbH
 */
package org.knime.bigdata.spark.node.util.repartition;

import java.awt.CardLayout;
import java.awt.Font;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.GridLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.List;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.ButtonGroup;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.knime.bigdata.spark.core.context.SparkContextManager;
import org.knime.bigdata.spark.core.port.data.SparkDataPortObject;
import org.knime.bigdata.spark.node.util.repartition.RepartitionJobInput.CalculationMode;
import org.knime.core.node.DataAwareNodeDialogPane;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.defaultnodesettings.DialogComponent;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelNumber;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;


/**
 * Spark repartition node dialog.
 *
 * @author Sascha Wolke, KNIME GmbH
 */
public class SparkRepartitionNodeDialog extends DataAwareNodeDialogPane implements ActionListener, ChangeListener {

    private final SparkRepartitionNodeSettings m_settings = new SparkRepartitionNodeSettings();

    private final ButtonGroup m_calcModeButtonGroup;
    private final JRadioButton m_fixedValueButton;
    private final JRadioButton m_multiplyByPartButton;
    private final JRadioButton m_divideByPartButton;
    private final JRadioButton m_multiplyByCoresButton;

    private final CardLayout m_prevCardLayout;
    private final JPanel m_prevCardPanel;
    private SparkDataPortObject m_inputPort;
    private boolean m_adaptiveQueryExecution;
    private final JLabel m_prevCurrentPartitions;
    private final JLabel m_prevNewPartitions;
    private final JPanel m_prev;
    private final JPanel m_prevAdaptiveExecution;
    private final JPanel m_prevNotAvailable;
    private final JPanel m_prevPredNotExecuted;
    private final JPanel m_prevExecutorCalculation;

    private final List<DialogComponent> m_components = new ArrayList<>();

    SparkRepartitionNodeDialog() {

        ////////////////// Calculation mode panel //////////////////
        final JPanel calcModePanel = new JPanel(new GridBagLayout());
        calcModePanel.setBorder(BorderFactory.createTitledBorder(" Set number of partitions to "));
        final GridBagConstraints gbc = new GridBagConstraints();
        gbc.gridx = gbc.gridy = 0;
        gbc.anchor = GridBagConstraints.WEST;
        gbc.fill = GridBagConstraints.NONE;
        gbc.weightx = 1.0;

        m_calcModeButtonGroup = new ButtonGroup();
        m_fixedValueButton = createCalcModeLine(calcModePanel, gbc, m_settings.getFixedValueModel(), "fixed value", 1);
        m_multiplyByPartButton = createCalcModeLine(calcModePanel, gbc, m_settings.getMultiplyPartitionFactorModel(),
            "multiply current partitions by", 1);
        m_divideByPartButton = createCalcModeLine(calcModePanel, gbc, m_settings.getDividePartitionFactorModel(),
            "divide current partitions by", 1);
        m_multiplyByCoresButton = createCalcModeLine(calcModePanel, gbc, m_settings.getMultiplyCoresFactorModel(),
            "multiply available executor cores by", 1);

        ////////////////// Preview panel //////////////////
        m_prevCardLayout = new CardLayout();
        m_prevCardPanel = new JPanel(m_prevCardLayout);
        m_prevCardPanel.setBorder(BorderFactory.createTitledBorder(" Preview "));
        m_prevCurrentPartitions = new JLabel("");
        m_prevCurrentPartitions.setFont(m_prevCurrentPartitions.getFont().deriveFont(Font.PLAIN));
        m_prevNewPartitions = new JLabel("");
        m_prevNewPartitions.setFont(m_prevNewPartitions.getFont().deriveFont(Font.PLAIN));
        m_prev = createPreviewPanel(m_prevCurrentPartitions, m_prevNewPartitions);
        m_prevCardPanel.add(m_prev, "prev");
        m_prevAdaptiveExecution =
            createPreviewErrorPanel("Partition count not available (Spark Context uses adaptive query execution)");
        m_prevCardPanel.add(m_prevAdaptiveExecution, "prevAdaptiveExecution");
        m_prevNotAvailable =
            createPreviewErrorPanel("Not available (Spark Context does not exist anymore or DataFrame is missing)");
        m_prevCardPanel.add(m_prevNotAvailable, "prevNotAvailable");
        m_prevPredNotExecuted = createPreviewErrorPanel("Not available (please execute the predecessor node first)");
        m_prevCardPanel.add(m_prevPredNotExecuted, "prevPredNotExecuted");
        m_prevExecutorCalculation =
            createPreviewErrorPanel("Not available (number of executor cores is only known during node execution)");
        m_prevCardPanel.add(m_prevExecutorCalculation, "prevExecutorCalculation");

        final Box settingsPanel = Box.createVerticalBox();
        settingsPanel.add(calcModePanel);
        settingsPanel.add(m_prevCardPanel);
        addTab("Settings", settingsPanel);

        ////////////////// Advanced panel //////////////////
        final JPanel advancedPanel = new JPanel();
        final DialogComponentBoolean useCoalesceComponent = new DialogComponentBoolean(m_settings.getUseCoalesceModel(), "avoid shuffling when decreasing partition count (use coalesce)");
        m_components.add(useCoalesceComponent);
        advancedPanel.add(useCoalesceComponent.getComponentPanel());
        addTab("Advanced", advancedPanel);
    }

    private JRadioButton createCalcModeLine(final JPanel panel, final GridBagConstraints gbc,
            final SettingsModelNumber model, final String label, final int stepSize) {

        final JRadioButton button = new JRadioButton(label);
        m_calcModeButtonGroup.add(button);
        button.addActionListener(this);

        final DialogComponentNumber component = new DialogComponentNumber(model, null, stepSize, 8);
        model.addChangeListener(this);
        m_components.add(component);

        final Box linePanel = Box.createHorizontalBox();
        linePanel.add(button);
        linePanel.add(component.getComponentPanel());
        panel.add(linePanel, gbc);
        gbc.gridy++;

        return button;
    }

    private static JPanel createPreviewPanel(final JLabel currentCount, final JLabel newCount) {
        final Box currentBox = Box.createHorizontalBox();
        currentBox.add(Box.createHorizontalStrut(5));
        currentBox.add(new JLabel("Current number of partitions: "));
        currentBox.add(currentCount);
        currentBox.add(Box.createHorizontalGlue());

        final Box newBox = Box.createHorizontalBox();
        newBox.add(Box.createHorizontalStrut(5));
        newBox.add(new JLabel("New number of partitions: "));
        newBox.add(newCount);
        newBox.add(Box.createHorizontalGlue());

        final JPanel panel = new JPanel(new GridLayout(2, 1));
        panel.add(currentBox);
        panel.add(newBox);

        return panel;
    }

    private static JPanel createPreviewErrorPanel(final String msg) {
        final JPanel panel = new JPanel();
        final JLabel label = new JLabel("<html><body><i>"+ msg + "</i></body></html>");
        label.setFont(label.getFont().deriveFont(Font.PLAIN));
        panel.add(label);
        return panel;
    }

    /**
     * Invoked when calculation mode changed.
     */
    @Override
    public void actionPerformed(final ActionEvent e) {
        final SettingsModelString model = m_settings.getCalculationModeModel();

        if (m_fixedValueButton.isSelected()) {
            model.setStringValue(CalculationMode.FIXED_VALUE.toString());
        } else if (m_multiplyByPartButton.isSelected()) {
            model.setStringValue(CalculationMode.MULTIPLY_PART_COUNT.toString());
        } else if (m_divideByPartButton.isSelected()) {
            model.setStringValue(CalculationMode.DIVIDE_PART_COUNT.toString());
        } else if (m_multiplyByCoresButton.isSelected()) {
            model.setStringValue(CalculationMode.MULTIPLY_EXECUTOR_CORES.toString());
        }

        updateInputEnabled();
        updatePreview();
    }

    /**
     * Invoked when fixed value or one of the factors changed.
     */
    @Override
    public void stateChanged(final ChangeEvent e) {
        updatePreview();
    }

    private void updateInputEnabled() {
        switch (m_settings.getCalculationMode()) {
            case FIXED_VALUE:
                m_settings.getFixedValueModel().setEnabled(true);
                m_settings.getMultiplyPartitionFactorModel().setEnabled(false);
                m_settings.getDividePartitionFactorModel().setEnabled(false);
                m_settings.getMultiplyCoresFactorModel().setEnabled(false);
                break;
            case MULTIPLY_PART_COUNT:
                m_settings.getFixedValueModel().setEnabled(false);
                m_settings.getMultiplyPartitionFactorModel().setEnabled(true);
                m_settings.getDividePartitionFactorModel().setEnabled(false);
                m_settings.getMultiplyCoresFactorModel().setEnabled(false);
                break;
            case DIVIDE_PART_COUNT:
                m_settings.getFixedValueModel().setEnabled(false);
                m_settings.getMultiplyPartitionFactorModel().setEnabled(false);
                m_settings.getDividePartitionFactorModel().setEnabled(true);
                m_settings.getMultiplyCoresFactorModel().setEnabled(false);
                break;
            case MULTIPLY_EXECUTOR_CORES:
                m_settings.getFixedValueModel().setEnabled(false);
                m_settings.getMultiplyPartitionFactorModel().setEnabled(false);
                m_settings.getDividePartitionFactorModel().setEnabled(false);
                m_settings.getMultiplyCoresFactorModel().setEnabled(true);
        }

        if (m_adaptiveQueryExecution) {
            // disable multiply and divide by partition count buttons and inputs
            m_multiplyByPartButton.setEnabled(false);
            m_settings.getMultiplyPartitionFactorModel().setEnabled(false);
            m_divideByPartButton.setEnabled(false);
            m_settings.getDividePartitionFactorModel().setEnabled(false);
        } else {
            m_multiplyByPartButton.setEnabled(true);
            m_divideByPartButton.setEnabled(true);
        }
    }

    private void updatePreview() {
        final CalculationMode mode = m_settings.getCalculationMode();

        if (m_adaptiveQueryExecution) {
            m_prevCardLayout.show(m_prevCardPanel, "prevAdaptiveExecution");
        } else if (mode == CalculationMode.MULTIPLY_EXECUTOR_CORES) {
            m_prevCardLayout.show(m_prevCardPanel, "prevExecutorCalculation");
        } else if (m_inputPort == null) {
            m_prevCardLayout.show(m_prevCardPanel, "prevPredNotExecuted");
        } else if (m_inputPort.getData().getStatistics() ==  null) {
            m_prevCardLayout.show(m_prevCardPanel, "prevNotAvailable");
        } else {
            int currParts = m_inputPort.getData().getStatistics().getNumPartitions();
            int newParts = -1;
            if (mode == CalculationMode.FIXED_VALUE) {
                newParts = m_settings.getFixedValue();
            } else if (mode == CalculationMode.MULTIPLY_PART_COUNT) {
                newParts = Math.max(1, (int)(currParts * m_settings.getMultiplyPartitionFactor()));
            } else if (mode == CalculationMode.DIVIDE_PART_COUNT) {
                newParts = Math.max(1, (int)(currParts / m_settings.getDividePartitionFactor()));
            }
            m_prevCurrentPartitions.setText(Integer.toString(currParts));
            m_prevNewPartitions.setText(Integer.toString(newParts));
            m_prevCardLayout.show(m_prevCardPanel, "prev");
        }
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObject[] input)
            throws NotConfigurableException {

        if (input != null && input.length > 0) {
            m_inputPort = (SparkDataPortObject)input[0];
            m_adaptiveQueryExecution =
                SparkContextManager.getOrCreateSparkContext(m_inputPort.getContextID()).adaptiveExecutionEnabled();
        } else {
            m_inputPort = null;
            m_adaptiveQueryExecution = false;
        }

        loadSettingsFrom(settings);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
        throws NotConfigurableException {

        m_inputPort = null;
        loadSettingsFrom(settings);
    }

    private void loadSettingsFrom(final NodeSettingsRO settings) throws NotConfigurableException {
        try {
            m_settings.getCalculationModeModel().loadSettingsFrom(settings);
            switch (m_settings.getCalculationMode()) {
                case FIXED_VALUE:
                    m_fixedValueButton.setSelected(true);
                    break;
                case MULTIPLY_PART_COUNT:
                    m_multiplyByPartButton.setSelected(true);
                    break;
                case DIVIDE_PART_COUNT:
                    m_divideByPartButton.setSelected(true);
                    break;
                default:
                    m_multiplyByCoresButton.setSelected(true);
                    break;
            }

            for (final DialogComponent c : m_components) {
                c.loadSettingsFrom(settings, null);
            }

            updateInputEnabled();
            updatePreview();
        } catch (InvalidSettingsException e) {
            throw new NotConfigurableException(e.getMessage());
        }
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.validateSettings();
        m_settings.saveSettingsTo(settings);
    }

    @Override
    public void onOpen() {
        updateInputEnabled();
        updatePreview();
    }
}
