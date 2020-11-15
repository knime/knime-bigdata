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
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.csv;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JPanel;
import javax.swing.JTextField;

import org.knime.base.filehandling.NodeUtils;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.Spark2GenericDataSourceNodeDialog3;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.StringHistory;

/**
 * Dialog for the Spark to CSV node.
 *
 * @author Sascha Wolke, KNIME.com
 */
class Spark2CSVNodeDialog3 extends Spark2GenericDataSourceNodeDialog3<Spark2CSVSettings3> {

    private JCheckBox m_header;
    private JComboBox<String> m_delimiter;
    private JTextField m_quote;
    private JTextField m_escape;
    private JTextField m_nullValue;
    private JComboBox<String> m_dateFormat;
    private JComboBox<String> m_compressionCodec;
    private JComboBox<String> m_quoteMode;

    public Spark2CSVNodeDialog3(final Spark2CSVSettings3 initialSettings) {
        super(initialSettings);
    }

    @Override
    protected void addSettingsPanels(final JPanel settingsPanel, final GridBagConstraints settingsPanelGbc) {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(createTitledBorder("Options"));
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);

        m_header = new JCheckBox("add Header");
        addToPanel(panel, gbc, "Header", m_header);
        m_delimiter = new JComboBox<>();
        m_delimiter.setEditable(true);
        addToPanel(panel, gbc, "Delimiter", m_delimiter);
        m_quote = new JTextField(1);
        addToPanel(panel, gbc, "Quote character", m_quote);
        m_escape = new JTextField();
        addToPanel(panel, gbc, "Escape character", m_escape);
        m_nullValue = new JTextField();
        addToPanel(panel, gbc, "Null value", m_nullValue);
        m_dateFormat = new JComboBox<>();
        m_dateFormat.setEditable(true);
        addToPanel(panel, gbc, "Date format", m_dateFormat);
        m_compressionCodec = new JComboBox<>(Spark2CSVSettings3.COMPRESSION_CODECS);
        addToPanel(panel, gbc, "Compression", m_compressionCodec);
        m_quoteMode = new JComboBox<>();
        addToPanel(panel, gbc, "Quote mode", m_quoteMode);

        settingsPanel.add(panel, settingsPanelGbc);
        settingsPanelGbc.gridy++;
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        super.loadSettingsFrom(settings, specs);
        m_header.setSelected(m_settings.withHeader());
        updateHistory("delimiter", m_delimiter, Spark2CSVSettings3.DELIMITER_EXAMPLES);
        m_delimiter.setSelectedItem(m_settings.getDelimiter());
        m_quote.setText(m_settings.getQuote());
        m_escape.setText(m_settings.getEscape());
        m_nullValue.setText(m_settings.getNullValue());
        updateHistory("dateFormat", m_dateFormat, Spark2CSVSettings3.DATE_FORMAT_EXAMPLES);
        m_dateFormat.setSelectedItem(m_settings.getDateFormat());
        m_compressionCodec.setSelectedItem(m_settings.getCompressionCodec());
        setAllElements(m_quoteMode, Spark2CSVSettings3.getQuoteModes(getSparkVersion(specs))); // updates elements by spark version
        m_quoteMode.setSelectedItem(m_settings.getQuoteMode());
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setHeader(m_header.isSelected());
        m_settings.setDelimiter(getSelection(m_delimiter));
        StringHistory.getInstance("delimiter", 15).add(getSelection(m_delimiter));
        m_settings.setQuote(m_quote.getText());
        m_settings.setEscape(m_escape.getText());
        m_settings.setNullValue(m_nullValue.getText());
        m_settings.setDateFormat(getSelection(m_dateFormat));
        StringHistory.getInstance("dateFormat", 15).add(getSelection(m_dateFormat));
        m_settings.setCompressionCodec((String)m_compressionCodec.getSelectedItem());
        m_settings.setQuoteMode((String)m_quoteMode.getSelectedItem());
        super.saveSettingsTo(settings);
    }


    /**
     * @param comboBox - Combo box with strings
     * @return Current selection
     */
    @Override
    protected String getSelection(final JComboBox<String> comboBox) {
        return comboBox.getEditor().getItem().toString();
    }

    /**
     * Update a combo box with string history.
     * @param id - History ID
     * @param comboBox - Combo box with strings
     * @param defaults - Default values to always add
     */
    @Override
    protected void updateHistory(final String id, final JComboBox<String> comboBox, final String[] defaults) {
        final StringHistory history = StringHistory.getInstance(id, 15);
        final Set<String> set = new LinkedHashSet<>();
        Collections.addAll(set, history.getHistory());
        Collections.addAll(set, defaults);
        final DefaultComboBoxModel<String> model = (DefaultComboBoxModel<String>) comboBox.getModel();
        model.removeAllElements();
        for (final String string : set) {
            model.addElement(string);
        }
    }
}
