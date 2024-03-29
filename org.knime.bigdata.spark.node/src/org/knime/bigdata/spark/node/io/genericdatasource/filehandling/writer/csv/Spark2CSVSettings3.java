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
 *   Created on Sep 29, 2016 by sascha
 */
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.csv;

import java.text.SimpleDateFormat;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer.Spark2GenericDataSourceSettings3;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceJobInput;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.SettingsModelWriterFileChooser;

/**
 * CSV specific writer settings.
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2CSVSettings3 extends Spark2GenericDataSourceSettings3 {

    /** Read header */
    private static final String CFG_HEADER = "header";
    private static final boolean DEFAULT_HEADER = true;
    private boolean m_header = DEFAULT_HEADER;

    /** Delimiter */
    private static final String CFG_DELIMITER = "delimiter";
    private static final String DEFAULT_DELIMITER = ",";
    private String m_delimiter = DEFAULT_DELIMITER;

    /** Example delimiters in dialog */
    public static final String[] DELIMITER_EXAMPLES = new String[] {
        ",", ";", "|", "<tab>", "<space>"
    };

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

    /** Example date formats in dialog */
    public static final String[] DATE_FORMAT_EXAMPLES = new String[] {
        "yyyy-MM-dd HH:mm:ss.S", "dd.MM.yyyy HH:mm:ss.S",
        "yyyy-MM-dd;HH:mm:ss.S", "dd.MM.yyyy;HH:mm:ss.S",
        "yyyy-MM-dd'T'HH:mm:ssZZZZ",
        "yyyy-MM-dd", "yyyy/MM/dd", "dd.MM.yyyy", "HH:mm:ss"
    };

    /** Compression codec. */
    public static final String COMPRESSION_CODECS[] = new String[] {
        "", "bzip2", "gzip", "lz4", "snappy"
    };
    private static final String CFG_COMPRESSION_CODEC = "codec";
    private static final String DEFAULT_COMPRESSION_CODEC = "";
    private String m_compressionCodec = DEFAULT_COMPRESSION_CODEC;

    /**
     * Supported quote modes of Spark 1.x CSV writer plugin.
     * @see <a href="https://commons.apache.org/proper/commons-csv/apidocs/org/apache/commons/csv/QuoteMode.html">common-csv</a>
     */
    public static final String QUOTE_MODES_SPARK_1_x[] = new String[] {
        "ALL", "MINIMAL", "NON_NUMERIC", "NONE"
    };
    /** Supported quote modes of inlined Spark 2.x CSV writer. */
    public static final String QUOTE_MODES_SPARK_2_x[] = new String[] {
        "ALL", "MINIMAL"
    };
    /**
     * @param sparkVersion
     * @return supported quote modes
     */
    public static final String[] getQuoteModes(final SparkVersion sparkVersion) {
        return sparkVersion.compareTo(SparkVersion.V_2_0) >= 0 ? QUOTE_MODES_SPARK_2_x : QUOTE_MODES_SPARK_1_x;
    }
    private static final String CFG_QUOTE_MODE = "quoteMode";
    private static final String DEFAULT_QUOTE_MODE = "MINIMAL";
    private String m_quoteMode = DEFAULT_QUOTE_MODE;

    /**
     * Default constructor.
     */
    Spark2CSVSettings3(final String format, final SparkVersion minSparkVersion, final boolean supportsPartitioning,
        final boolean hasDriver, final PortsConfiguration portsConfig) {
        super(format, minSparkVersion, supportsPartitioning, hasDriver, portsConfig);
    }

    /**
     * Constructor used in validation.
     */
    Spark2CSVSettings3(final String format, final SparkVersion minSparkVersion, final boolean supportsPartitioning,
        final boolean hasDriver, final SettingsModelWriterFileChooser outputPathChooser) {
        super(format, minSparkVersion, supportsPartitioning, hasDriver, outputPathChooser);
    }

    @Override
    protected Spark2GenericDataSourceSettings3 newValidateInstance() {
        return new Spark2CSVSettings3(getFormat(), getMinSparkVersion(), supportsPartitioning(), hasDriver(), getFileChooserModel());
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

    /** @return Quote mode (see {@link #QUOTE_MODES_SPARK_1_x} and {@link #QUOTE_MODES_SPARK_2_x}) */
    public String getQuoteMode() { return m_quoteMode; }
    /** @param quoteMode - Quote mode (see {@link #QUOTE_MODES_SPARK_1_x} and {@link #QUOTE_MODES_SPARK_2_x}}) */
    public void setQuoteMode(final String quoteMode) { m_quoteMode = quoteMode; }

    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
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

        if (unescapeDelimiter() == null || unescapeDelimiter().length() != 1) {
            throw new InvalidSettingsException("Delimiter character has to be exactly one character (default: " + DEFAULT_DELIMITER + ")");
        }

        if (unescapeDelimiter() != null && unescapeDelimiter().equals("\n")) {
            throw new InvalidSettingsException("Newline as delimiter is not allowed (default: " + DEFAULT_DELIMITER + ")");
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

        final String delimiter = unescapeDelimiter();
        if (unescapeDelimiter() != null && unescapeDelimiter().length() == 1) {
            jobInput.setOption(CFG_DELIMITER, delimiter);
        }

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
        if (m_quoteMode.equals("ALL")) {
            jobInput.setOption("quoteAll", "true");
        }

        super.addWriterOptions(jobInput);
    }

    /**
     * Transform generic delimiter input string into a delimiter character.
     *
     * @return unescaped delimiter string with one character or null
     */
    private String unescapeDelimiter() {
        final String unescaped = StringEscapeUtils.unescapeJava(m_delimiter);

        if (m_delimiter.equalsIgnoreCase("<space>")) {
            return " ";
        } else if (m_delimiter.equalsIgnoreCase("<tab>")) {
            return "\t";
        } else if (unescaped.length() == 1) {
            return unescaped;
        } else {
            return null;
        }
    }
}
