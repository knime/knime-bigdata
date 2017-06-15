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
 *   Created on Sep 05, 2016 by Sascha
 */
package com.knime.bigdata.spark.node.io.genericdatasource.reader.csv;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JTextField;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.StringHistory;

import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkNodeDialog;

/**
 * Dialog for the JDBC to Spark node.
 *
 * @author Sascha Wolke, KNIME.com
 */
class CSV2SparkNodeDialog extends GenericDataSource2SparkNodeDialog<CSV2SparkSettings> {

    private final JCheckBox m_header;
    private final JComboBox<String> m_delimiter;
    private final JTextField m_quote;
    private final JTextField m_escape;
    private final JComboBox<String> m_mode;
    private final JComboBox<String> m_charset;
    private final JCheckBox m_inferSchema;
    private final JTextField m_comment;
    private final JTextField m_nullValue;
    private final JComboBox<String> m_dateFormat;

    public CSV2SparkNodeDialog(final CSV2SparkSettings initialSettings) {
        super(initialSettings);
        m_header = new JCheckBox("with Header");
        addToOptionsPanel("Header", m_header);
        m_delimiter = new JComboBox<>();
        m_delimiter.setEditable(true);
        addToOptionsPanel("Delimiter", m_delimiter);
        m_quote = new JTextField(1);
        addToOptionsPanel("Quote character", m_quote);
        m_escape = new JTextField();
        addToOptionsPanel("Escape character", m_escape);
        m_mode = new JComboBox<>(CSV2SparkSettings.MODES);
        addToOptionsPanel("Mode", m_mode);
        m_charset = new JComboBox<>();
        m_charset.setEditable(true);
        addToOptionsPanel("Charset", m_charset);
        m_inferSchema = new JCheckBox("Infer (requires one extra pass over the data)");
        addToOptionsPanel("Schema", m_inferSchema);
        m_comment = new JTextField();
        addToOptionsPanel("Comments", m_comment);
        m_nullValue = new JTextField();
        addToOptionsPanel("Null value", m_nullValue);
        m_dateFormat = new JComboBox<>();
        m_dateFormat.setEditable(true);
        addToOptionsPanel("Date format", m_dateFormat);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        super.loadSettingsFrom(settings, specs);
        m_header.setSelected(m_settings.withHeader());
        updateHistory("delimiter", m_delimiter, CSV2SparkSettings.DELIMITER_EXAMPLES);
        m_delimiter.setSelectedItem(m_settings.getDelimiter());
        m_quote.setText(m_settings.getQuote());
        m_escape.setText(m_settings.getEscape());
        m_mode.setSelectedItem(m_settings.getMode());
        updateHistory("charset", m_charset, CSV2SparkSettings.CHARSET_EXAMPLES);
        m_charset.setSelectedItem(m_settings.getCharset());
        m_inferSchema.setSelected(m_settings.inferSchema());
        m_comment.setText(m_settings.getComment());
        m_nullValue.setText(m_settings.getNullValue());
        updateHistory("dateFormat", m_dateFormat, CSV2SparkSettings.DATE_FORMAT_EXAMPLES);
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
}
