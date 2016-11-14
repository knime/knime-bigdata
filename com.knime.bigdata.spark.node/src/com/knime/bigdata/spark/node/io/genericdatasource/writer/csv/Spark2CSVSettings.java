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
 *   Created on Sep 29, 2016 by sascha
 */
package com.knime.bigdata.spark.node.io.genericdatasource.writer.csv;

import java.text.SimpleDateFormat;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import com.knime.bigdata.spark.core.version.SparkVersion;
import com.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceJobInput;
import com.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceSettings;

/**
 * CSV specific writer settings.
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2CSVSettings extends Spark2GenericDataSourceSettings {

    /** Read header */
    private static final String CFG_HEADER = "header";
    private static final boolean DEFAULT_HEADER = true;
    private boolean m_header = DEFAULT_HEADER;

    /** Delimiter */
    private static final String CFG_DELIMITER = "delimiter";
    private static final String DEFAULT_DELIMITER = ",";
    private String m_delimiter = DEFAULT_DELIMITER;

    /** Quote character */
    private static final String CFG_QUOTE = "quote";
    private static final String DEFAULT_QUOTE = "\"";
    private String m_quote = DEFAULT_QUOTE;

    /** Escape character */
    private static final String CFG_ESCAPE = "escape";
    private static final String DEFAULT_ESCAPE = "\\";
    private String m_escape = DEFAULT_ESCAPE;

    /** Set values to null if string matches */
    private static final String CFG_NULL_VALUE = "nullValue";
    private static final String DEFAULT_NULL_VALUE = "null";
    private String m_nullValue = DEFAULT_NULL_VALUE;

    /** Date/timestamp format */
    private static final String CFG_DATE_FORMAT = "dateFormat";
    private static final String DEFAULT_DATE_FORMAT = "";
    private String m_dateFormat = DEFAULT_DATE_FORMAT;

    /** Compression codec. */
    public static final String COMPRESSION_CODECS[] = new String[] {
        "", "bzip2", "gzip", "lz4", "snappy"
    };
    private static final String CFG_COMPRESSION_CODEC = "codec";
    private static final String DEFAULT_COMPRESSION_CODEC = "";
    private String m_compressionCodec = DEFAULT_COMPRESSION_CODEC;

    /**
     * Quote mode.
     * @see <a href="https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/QuoteMode.html">common-csv</a>
     */
    public static final String QUOTE_MODES[] = new String[] {
        "ALL", "MINIMAL", "NON_NUMERIC", "NONE"
    };
    private static final String CFG_QUOTE_MODE = "quoteMode";
    private static final String DEFAULT_QUOTE_MODE = "MINIMAL";
    private String m_quoteMode = DEFAULT_QUOTE_MODE;

    /** @see Spark2GenericDataSourceSettings#Spark2GenericDataSourceSettings(String, boolean, boolean) */
    public Spark2CSVSettings(final String format, final SparkVersion minSparkVersion, final boolean supportsPartitioning, final boolean hasDriver) {
        super(format, minSparkVersion, supportsPartitioning, hasDriver);
    }

    @Override
    protected Spark2GenericDataSourceSettings newInstance() {
        return new Spark2CSVSettings(getFormat(), getMinSparkVersion(), supportsPartitioning(), hasDriver());
    }

    /** @return true if first line contains column names */
    public boolean withHeader() { return m_header; }
    /** @param header - true if first line contains column names */
    public void setHeader(final boolean header) { m_header = header; }

    /** @return required delimiter character */
    public String getDelimiter() { return m_delimiter; }
    /** @param delimiter - Delimiter character as string (only one character supported). */
    public void setDelimiter(final String delimiter) { m_delimiter = delimiter; }

    /** @return required quote character */
    public String getQuote() { return m_quote; }
    /** @param quote - Quote character as string (only one character supported). */
    public void setQuote(final String quote) { m_quote = quote; }

    /** @return required escape character */
    public String getEscape() { return m_escape; }
    /** @param escape - Escape character(s) or empty string. */
    public void setEscape(final String escape) { m_escape = escape; }

    /** @return Null value representation or empty string */
    public String getNullValue() { return m_nullValue; }
    /** @param nullValue - Null value representation or empty string. */
    public void setNullValue(final String nullValue) { m_nullValue = nullValue; }

    /** @return Date format string (see {@link SimpleDateFormat}) */
    public String getDateFormat() { return m_dateFormat; }
    /** @param dateFormat - Valid simple data format string (see {@link SimpleDateFormat}) */
    public void setDateFormat(final String dateFormat) { m_dateFormat = dateFormat; }

    /** @return Compression codec or empty string */
    public String getCompressionCodec() { return m_compressionCodec; }
    /** @param codec - Compression codec or empty string (see {@link #COMPRESSION_CODECS}) */
    public void setCompressionCodec(final String codec) { m_compressionCodec = codec; }

    /** @return Quote mode (see {@link #QUOTE_MODES}) */
    public String getQuoteMode() { return m_quoteMode; }
    /** @param quoteMode - Quote mode (see {@link #QUOTE_MODES}) */
    public void setQuoteMode(final String quoteMode) { m_quoteMode = quoteMode; }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addBoolean(CFG_HEADER, m_header);
        settings.addString(CFG_DELIMITER, m_delimiter);
        settings.addString(CFG_QUOTE, m_quote);
        settings.addString(CFG_ESCAPE, m_escape);
        settings.addString(CFG_NULL_VALUE, m_nullValue);
        settings.addString(CFG_DATE_FORMAT, m_dateFormat);
        settings.addString(CFG_COMPRESSION_CODEC, m_compressionCodec);
        settings.addString(CFG_QUOTE_MODE, m_quoteMode);

        super.saveSettingsTo(settings);
    }

    @Override
    public void validateSettings() throws InvalidSettingsException {
        if (m_quote.length() != 1) {
            throw new InvalidSettingsException("Quote character has to be exactly one character (default: " + DEFAULT_QUOTE + ")");
        }

        if (m_delimiter.length() != 1) {
            throw new InvalidSettingsException("Delimiter character has to be exactly one character (default: " + DEFAULT_DELIMITER + ")");
        }

        try {
            new SimpleDateFormat(m_dateFormat);
        } catch(IllegalArgumentException e) {
            throw new InvalidSettingsException("Invalid date format (valid SimpleDateFormat required): " + e.getMessage());
        }

        super.validateSettings();
    }

    @Override
    public void loadSettings(final NodeSettingsRO settings) {
        m_header = settings.getBoolean(CFG_HEADER, DEFAULT_HEADER);
        m_delimiter = settings.getString(CFG_DELIMITER, DEFAULT_DELIMITER);
        m_quote = settings.getString(CFG_QUOTE, DEFAULT_QUOTE);
        m_escape = settings.getString(CFG_ESCAPE, DEFAULT_ESCAPE);
        m_nullValue = settings.getString(CFG_NULL_VALUE, DEFAULT_NULL_VALUE);
        m_dateFormat = settings.getString(CFG_DATE_FORMAT, DEFAULT_DATE_FORMAT);
        m_compressionCodec = settings.getString(CFG_COMPRESSION_CODEC, DEFAULT_COMPRESSION_CODEC);
        m_quoteMode = settings.getString(CFG_QUOTE_MODE, DEFAULT_QUOTE_MODE);

        super.loadSettings(settings);
    }

    @Override
    public void addWriterOptions(final Spark2GenericDataSourceJobInput jobInput) {
        jobInput.setOption(CFG_HEADER, Boolean.toString(m_header));
        jobInput.setOption(CFG_DELIMITER, m_delimiter);
        jobInput.setOption(CFG_QUOTE, m_quote);
        if (!StringUtils.isBlank(m_escape)) {
            jobInput.setOption(CFG_ESCAPE, m_escape);
        }
        jobInput.setOption(CFG_NULL_VALUE, m_nullValue);

        if (!StringUtils.isBlank(m_dateFormat)) {
            jobInput.setOption(CFG_DATE_FORMAT, m_dateFormat);
        }

        if (!StringUtils.isBlank(m_compressionCodec)) {
            jobInput.setOption(CFG_COMPRESSION_CODEC, m_compressionCodec);
        }

        jobInput.setOption(CFG_QUOTE_MODE, m_quoteMode);

        super.addWriterOptions(jobInput);
    }
}
