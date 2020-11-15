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
 *   Created on Sep 05, 2016 by Sascha
 */
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.csv;

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
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.reader.GenericDataSource2SparkNodeDialog3;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.StringHistory;

/**
 * Dialog for the JDBC to Spark node.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class CSV2SparkNodeDialog3 extends GenericDataSource2SparkNodeDialog3<CSV2SparkSettings3> {

    private JCheckBox m_header;
    private JComboBox<String> m_delimiter;
    private JTextField m_quote;
    private JTextField m_escape;
    private JComboBox<String> m_mode;
    private JComboBox<String> m_charset;
    private JCheckBox m_inferSchema;
    private JTextField m_comment;
    private JTextField m_nullValue;
    private JComboBox<String> m_dateFormat;

    CSV2SparkNodeDialog3(final CSV2SparkSettings3 initialSettings) {
        super(initialSettings);
    }

    @Override
    protected void addSettingsPanels(final JPanel settingsPanel, final GridBagConstraints settingsPanelGbc) {
        final JPanel panel = new JPanel(new GridBagLayout());
        panel.setBorder(createTitledBorder("Options"));
        final GridBagConstraints gbc = new GridBagConstraints();
        NodeUtils.resetGBC(gbc);

        m_header = new JCheckBox("with Header");
        addToPanel(panel, gbc, "Header", m_header);
        m_delimiter = new JComboBox<>();
        m_delimiter.setEditable(true);
        addToPanel(panel, gbc, "Delimiter", m_delimiter);
        m_quote = new JTextField(1);
        addToPanel(panel, gbc, "Quote character", m_quote);
        m_escape = new JTextField();
        addToPanel(panel, gbc, "Escape character", m_escape);
        m_mode = new JComboBox<>(CSV2SparkSettings3.MODES);
        addToPanel(panel, gbc, "Mode", m_mode);
        m_charset = new JComboBox<>();
        m_charset.setEditable(true);
        addToPanel(panel, gbc, "Charset", m_charset);
        m_inferSchema = new JCheckBox("Infer (requires one extra pass over the data)");
        addToPanel(panel, gbc, "Schema", m_inferSchema);
        m_comment = new JTextField();
        addToPanel(panel, gbc, "Comments", m_comment);
        m_nullValue = new JTextField();
        addToPanel(panel, gbc, "Null value", m_nullValue);
        m_dateFormat = new JComboBox<>();
        m_dateFormat.setEditable(true);
        addToPanel(panel, gbc, "Date format", m_dateFormat);

        settingsPanel.add(panel, settingsPanelGbc);
        settingsPanelGbc.gridy++;
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        super.loadSettingsFrom(settings, specs);
        m_header.setSelected(m_settings.withHeader());
        updateHistory("delimiter", m_delimiter, CSV2SparkSettings3.DELIMITER_EXAMPLES);
        m_delimiter.setSelectedItem(m_settings.getDelimiter());
        m_quote.setText(m_settings.getQuote());
        m_escape.setText(m_settings.getEscape());
        m_mode.setSelectedItem(m_settings.getMode());
        updateHistory("charset", m_charset, CSV2SparkSettings3.CHARSET_EXAMPLES);
        m_charset.setSelectedItem(m_settings.getCharset());
        m_inferSchema.setSelected(m_settings.inferSchema());
        m_comment.setText(m_settings.getComment());
        m_nullValue.setText(m_settings.getNullValue());
        updateHistory("dateFormat", m_dateFormat, CSV2SparkSettings3.DATE_FORMAT_EXAMPLES);
        m_dateFormat.setSelectedItem(m_settings.getDateFormat());
    }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) throws InvalidSettingsException {
        m_settings.setHeader(m_header.isSelected());
        m_settings.setDelimiter(getSelection(m_delimiter));
        StringHistory.getInstance("delimiter", 15).add(getSelection(m_delimiter));
        m_settings.setQuote(m_quote.getText());
        m_settings.setEscape(m_escape.getText());
        m_settings.setMode((String) m_mode.getSelectedItem());
        m_settings.setCharset(getSelection(m_charset));
        StringHistory.getInstance("charset").add(getSelection(m_charset));
        m_settings.setInferSchema(m_inferSchema.isSelected());
        m_settings.setComment(m_comment.getText());
        m_settings.setNullValue(m_nullValue.getText());
        m_settings.setDateFormat(getSelection(m_dateFormat));
        StringHistory.getInstance("dateFormat").add(getSelection(m_dateFormat));
        super.saveSettingsTo(settings);
    }

    /**
     * @param comboBox - Combo box with strings
     * @return Current selection
     */
    protected String getSelection(final JComboBox<String> comboBox) {
        return comboBox.getEditor().getItem().toString();
    }

    /**
     * Update a combo box with string history.
     * @param id - History ID
     * @param comboBox - Combo box with strings
     * @param defaults - Default values to always add
     */
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
