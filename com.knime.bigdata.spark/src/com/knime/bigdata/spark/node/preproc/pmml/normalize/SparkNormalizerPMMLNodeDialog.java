/* ------------------------------------------------------------------
 * This source code, its documentation and all appendant files
 * are protected by copyright law. All rights reserved.
 *
 * Copyright by KNIME.com, Zurich, Switzerland
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
 *   Created on 03.08.2015 by dwk
 */
package com.knime.bigdata.spark.node.preproc.pmml.normalize;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.GridLayout;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.util.HashMap;
import java.util.Map;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JTextField;

import org.knime.core.data.DataTableSpec;
import org.knime.core.data.DoubleValue;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeDialogPane;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.CheckUtils;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterPanel;

import com.knime.bigdata.spark.node.preproc.pmml.normalize.SparkNormalizerPMMLConfig.NormalizerMode;
import com.knime.bigdata.spark.port.data.SparkDataPortObjectSpec;

/**
 *
 * @author dwk
 * @note this is an identical copy of org.knime.base.node.preproc.normalize3.SparkNormalizerPMMLNodeDialog (hopefully), we had to
 *       copy it as it is package protected TODO - make Normalizer3Config public and check what to do with this class

 */
public class SparkNormalizerPMMLNodeDialog  extends NodeDialogPane {

        /*
         * The tab's name.
         */
        private static final String TAB = "Methods";

        private JTextField m_minTextField;

        private JTextField m_maxTextField;

        private DataColumnSpecFilterPanel m_filterPanel;

        private Map<NormalizerMode, JRadioButton> m_buttonMap = new HashMap<NormalizerMode, JRadioButton>();

        /**
         * Creates a new dialog for the Normalize Node.
         */
        @SuppressWarnings("unchecked")
        public SparkNormalizerPMMLNodeDialog() {
            super();
            m_filterPanel = new DataColumnSpecFilterPanel(DoubleValue.class);
            JPanel panel = generateContent();

            JPanel all = new JPanel(new BorderLayout());
            all.add(m_filterPanel, BorderLayout.NORTH);
            all.add(panel, BorderLayout.SOUTH);
            super.addTab(TAB, all);
        }

        /*
         * Generates the radio buttons and text fields
         */
        private JPanel generateContent() {
            JPanel panel = new JPanel();

            // min-max
            JPanel panel1 = new JPanel();
            GridLayout gl = new GridLayout(2, 4);
            panel1.setLayout(gl);

            final JRadioButton minmaxButton = new JRadioButton("Min-Max Normalization");
            minmaxButton.setSelected(true);
            JLabel nmin = new JLabel("Min: ");
            JPanel spanel1 = new JPanel();
            spanel1.setLayout(new BorderLayout());
            spanel1.add(nmin, BorderLayout.EAST);
            spanel1.setMaximumSize(new Dimension(30, 10));

            m_minTextField = new JTextField(5);

            JLabel nmax = new JLabel("Max: ");
            JPanel spanel2 = new JPanel();
            spanel2.setLayout(new BorderLayout());
            spanel2.add(nmax, BorderLayout.EAST);
            spanel2.setMaximumSize(new Dimension(30, 10));

            m_maxTextField = new JTextField(5);

            panel1.add(minmaxButton);
            panel1.add(spanel1);
            panel1.add(m_minTextField);
            panel1.add(Box.createHorizontalGlue());
            panel1.add(new JPanel());
            panel1.add(spanel2);
            panel1.add(m_maxTextField);
            panel1.add(Box.createHorizontalGlue());

            // z-score
            JPanel panel2 = new JPanel();
            panel2.setLayout(new BorderLayout());
            final JRadioButton zScoreButton = new JRadioButton("Z-Score Normalization (Gaussian)");
            panel2.add(zScoreButton, BorderLayout.WEST);

            // decimal scaling
            JPanel panel3 = new JPanel();
            panel3.setLayout(new BorderLayout());
            final JRadioButton decButton = new JRadioButton("Normalization by Decimal Scaling");
            panel3.add(decButton, BorderLayout.WEST);

            // Group the radio buttons.
            ButtonGroup group = new ButtonGroup();
            group.add(minmaxButton);
            group.add(zScoreButton);
            group.add(decButton);
            m_buttonMap.put(NormalizerMode.MINMAX, minmaxButton);
            m_buttonMap.put(NormalizerMode.DECIMALSCALING, decButton);
            m_buttonMap.put(NormalizerMode.Z_SCORE, zScoreButton);

            minmaxButton.addItemListener(new ItemListener() {

                @Override
                public void itemStateChanged(final ItemEvent e) {
                    if (minmaxButton.isSelected()) {
                        m_minTextField.setEnabled(true);
                        m_maxTextField.setEnabled(true);
                    }
                }
            });
            zScoreButton.addItemListener(new ItemListener() {
                @Override
                public void itemStateChanged(final ItemEvent e) {
                    if (zScoreButton.isSelected()) {
                        m_minTextField.setEnabled(false);
                        m_maxTextField.setEnabled(false);
                    }
                }
            });
            decButton.addItemListener(new ItemListener() {
                @Override
                public void itemStateChanged(final ItemEvent e) {
                    if (decButton.isSelected()) {
                        m_minTextField.setEnabled(false);
                        m_maxTextField.setEnabled(false);
                    }
                }
            });

            BoxLayout bly = new BoxLayout(panel, BoxLayout.Y_AXIS);
            panel.setLayout(bly);
            panel.add(panel1);
            panel.add(panel2);
            panel.add(panel3);
            panel.setBorder(BorderFactory.createTitledBorder("Settings"));
            return panel;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs)
            throws NotConfigurableException {
            SparkNormalizerPMMLConfig config = new SparkNormalizerPMMLConfig();
            DataTableSpec spec = ((SparkDataPortObjectSpec)specs[0]).getTableSpec();
            config.loadConfigurationInDialog(settings, spec);
            m_maxTextField.setText(Double.toString(config.getMax()));
            m_minTextField.setText(Double.toString(config.getMin()));
            m_filterPanel.loadConfiguration(config.getDataColumnFilterConfig(), spec);
            m_buttonMap.get(config.getMode()).setSelected(true);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
            SparkNormalizerPMMLConfig config = new SparkNormalizerPMMLConfig();
            for (Map.Entry<NormalizerMode, JRadioButton> entry : m_buttonMap.entrySet()) {
                if (entry.getValue().isSelected()) {
                    config.setMode(entry.getKey());
                    break;
                }
            }

            if (NormalizerMode.MINMAX.equals(config.getMode())) {
                config.setMin(getDouble("Min", m_minTextField));
                config.setMax(getDouble("Max", m_maxTextField));
                CheckUtils.checkSetting(config.getMin() <= config.getMax(),
                        "Min (%f) cannot be greater than Max (%f)", config.getMin(), config.getMax());
            }
            m_filterPanel.saveConfiguration(config.getDataColumnFilterConfig());
            config.saveSettings(settings);
        }

        private static double getDouble(final String name, final JTextField inputField) throws InvalidSettingsException {
            try {
                return Double.valueOf(inputField.getText());
            } catch (Exception e) {
                CheckUtils.checkSetting(false,
                    "%s must be a valid double value (not a number: \"%s\")", name, inputField.getText());
                return 0;
            }
        }

}
