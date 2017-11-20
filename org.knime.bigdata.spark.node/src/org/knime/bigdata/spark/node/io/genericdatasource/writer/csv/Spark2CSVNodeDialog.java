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
package org.knime.bigdata.spark.node.io.genericdatasource.writer.csv;

import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JTextField;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.NotConfigurableException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.util.StringHistory;

import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceNodeDialog;

/**
 * Dialog for the Spark to CSV node.
 *
 * @author Sascha Wolke, KNIME.com
 */
class Spark2CSVNodeDialog extends Spark2GenericDataSourceNodeDialog<Spark2CSVSettings> {

    private final JCheckBox m_header;
    private final JComboBox<String> m_delimiter;
    private final JTextField m_quote;
    private final JTextField m_escape;
    private final JTextField m_nullValue;
    private final JComboBox<String> m_dateFormat;
    private final JComboBox<String> m_compressionCodec;
    private final JComboBox<String> m_quoteMode;

    public Spark2CSVNodeDialog(final Spark2CSVSettings initialSettings) {
        super(initialSettings);
        m_header = new JCheckBox("add Header");
        addToOptionsPanel("Header", m_header);
        m_delimiter = new JComboBox<>();
        m_delimiter.setEditable(true);
        addToOptionsPanel("Delimiter", m_delimiter);
        m_quote = new JTextField(1);
        addToOptionsPanel("Quote character", m_quote);
        m_escape = new JTextField();
        addToOptionsPanel("Escape character", m_escape);
        m_nullValue = new JTextField();
        addToOptionsPanel("Null value", m_nullValue);
        m_dateFormat = new JComboBox<>();
        m_dateFormat.setEditable(true);
        addToOptionsPanel("Date format", m_dateFormat);
        m_compressionCodec = new JComboBox<>(Spark2CSVSettings.COMPRESSION_CODECS);
        addToOptionsPanel("Compression", m_compressionCodec);
        m_quoteMode = new JComboBox<>();
        addToOptionsPanel("Quote mode", m_quoteMode);
    }

    @Override
    protected void loadSettingsFrom(final NodeSettingsRO settings, final PortObjectSpec[] specs) throws NotConfigurableException {
        super.loadSettingsFrom(settings, specs);
        m_header.setSelected(m_settings.withHeader());
        updateHistory("delimiter", m_delimiter, Spark2CSVSettings.DELIMITER_EXAMPLES);
        m_delimiter.setSelectedItem(m_settings.getDelimiter());
        m_quote.setText(m_settings.getQuote());
        m_escape.setText(m_settings.getEscape());
        m_nullValue.setText(m_settings.getNullValue());
        updateHistory("dateFormat", m_dateFormat, Spark2CSVSettings.DATE_FORMAT_EXAMPLES);
        m_dateFormat.setSelectedItem(m_settings.getDateFormat());
        m_compressionCodec.setSelectedItem(m_settings.getCompressionCodec());
        setAllElements(m_quoteMode, Spark2CSVSettings.getQuoteModes(getSparkVersion(specs))); // updates elements by spark version
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
}
