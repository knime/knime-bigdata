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
package com.knime.bigdata.spark.node.io.genericdatasource.reader.csv;

import java.nio.charset.Charset;
import java.text.SimpleDateFormat;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.osgi.framework.Version;

import com.knime.bigdata.spark.core.version.SparkPluginVersion;
import com.knime.bigdata.spark.core.version.SparkVersion;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkJobInput;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkSettings;

/**
 * CSV specific reader settings.
 * @author Sascha Wolke, KNIME.com
 * @see <a href="https://github.com/databricks/spark-csv">spark-csv</a>
 */
public class CSV2SparkSettings extends GenericDataSource2SparkSettings {

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

    // parserLib

    /** Parsing mode */
    public static final String MODES[] = new String[] {
        "PERMISSIVE", "DROPMALFORMED", "FAILFAST"
    };
    private static final String CFG_MODE = "mode";
    private static final String DEFAULT_MODE = "PERMISSIVE";
    private String m_mode = DEFAULT_MODE;

    /** Default charset */
    private static final String CFG_CHARSET = "charset";
    private static final String DEFAULT_CHARSET = "UTF-8";
    private String m_charset = DEFAULT_CHARSET;

    /** Example charsets in dialog */
    public static final String[] CHARSET_EXAMPLES = new String[] {
        "UTF-8", "UTF-16", "UTF-16LE", "UTF-16BE", "ISO-8859-1"
    };

    /** Infer schema or read all values as string */
    private static final String CFG_INFER_SCHEMA = "inferSchema";
    private static final boolean DEFAULT_INFER_SCHEMA = true;
    private boolean m_inferSchema = DEFAULT_INFER_SCHEMA;

    /** Skip lines beginning with this character */
    private static final String CFG_COMMENT = "comment";
    private static final String DEFAULT_COMMENT = "#";
    private String m_comment = DEFAULT_COMMENT;

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

    /** @see GenericDataSource2SparkSettings#GenericDataSource2SparkSettings(String, SparkVersion, boolean) */
    public CSV2SparkSettings(final String format, final SparkVersion minSparkVersion, final boolean hasDriver, final Version knimeSparkExecutorVersion) {
        super(format, minSparkVersion, hasDriver, knimeSparkExecutorVersion);
    }

    @Override
    protected GenericDataSource2SparkSettings newInstance() {
        return new CSV2SparkSettings(getFormat(), getMinSparkVersion(), hasDriver(), SparkPluginVersion.VERSION_CURRENT);
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

    /** @return Parsing mode (see {@link #MODES}) */
    public String getMode() { return m_mode; }
    /** @param mode - Parsing mode (see {@link #MODES}) */
    public void setMode(final String mode) { m_mode = mode; }

    /** @return Character set name (see {@link Charset#availableCharsets()}) */
    public String getCharset() { return m_charset; }
    /** @param charset - Valid character set name (see {@link Charset#availableCharsets()}) */
    public void setCharset(final String charset) { m_charset = charset; }

    /** @return True if infer schema */
    public boolean inferSchema() { return m_inferSchema; }
    /** @param inferSchema - True if schema should be infered from input data (requires one extra pass over the data) */
    public void setInferSchema(final boolean inferSchema) { m_inferSchema = inferSchema; }

    /** @return Skip lines beginning with this character or empty string to disable comment support */
    public String getComment() { return m_comment; }
    /** @param comment - Comment character or empty string to disable comments */
    public void setComment(final String comment) { m_comment = comment; }

    /** @return Null value representation */
    public String getNullValue() { return m_nullValue; }
    /** @param nullValue - Null value representation or empty string. */
    public void setNullValue(final String nullValue) { m_nullValue = nullValue; }

    /** @return Date format string (see {@link SimpleDateFormat}) */
    public String getDateFormat() { return m_dateFormat; }
    /** @param dateFormat - Valid simple data format string (see {@link SimpleDateFormat}) */
    public void setDateFormat(final String dateFormat) { m_dateFormat = dateFormat; }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addBoolean(CFG_HEADER, m_header);
        settings.addString(CFG_DELIMITER, m_delimiter);
        settings.addString(CFG_QUOTE, m_quote);
        settings.addString(CFG_ESCAPE, m_escape);
        settings.addString(CFG_MODE, m_mode);
        settings.addString(CFG_CHARSET, m_charset);
        settings.addBoolean(CFG_INFER_SCHEMA, m_inferSchema);
        settings.addString(CFG_COMMENT, m_comment);
        settings.addString(CFG_NULL_VALUE, m_nullValue);
        settings.addString(CFG_DATE_FORMAT, m_dateFormat);

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

        if (!StringUtils.isBlank(m_charset) && !Charset.isSupported(m_charset)) {
            throw new InvalidSettingsException("Unsupported charset.");
        }

        if (m_comment.length() > 1) {
            throw new InvalidSettingsException("Comment character has to be exactly one character (default: " + DEFAULT_COMMENT + ")");
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
        m_mode = settings.getString(CFG_MODE, DEFAULT_MODE);
        m_charset = settings.getString(CFG_CHARSET, DEFAULT_CHARSET);
        m_inferSchema = settings.getBoolean(CFG_INFER_SCHEMA, DEFAULT_INFER_SCHEMA);
        m_comment = settings.getString(CFG_COMMENT, DEFAULT_COMMENT);
        m_nullValue = settings.getString(CFG_NULL_VALUE, DEFAULT_NULL_VALUE);
        m_dateFormat = settings.getString(CFG_DATE_FORMAT, DEFAULT_DATE_FORMAT);

        super.loadSettings(settings);
    }

    @Override
    public void addReaderOptions(final GenericDataSource2SparkJobInput jobInput) {
        jobInput.setOption(CFG_HEADER, Boolean.toString(m_header));

        final String delimiter = unescapeDelimiter();
        if (unescapeDelimiter() != null && unescapeDelimiter().length() == 1) {
            jobInput.setOption(CFG_DELIMITER, delimiter);
        }

        jobInput.setOption(CFG_QUOTE, m_quote);
        if (!StringUtils.isBlank(m_escape)) {
            jobInput.setOption(CFG_ESCAPE, m_escape);
        }
        jobInput.setOption(CFG_MODE, m_mode);
        jobInput.setOption(CFG_CHARSET, m_charset);
        jobInput.setOption(CFG_INFER_SCHEMA, Boolean.toString(m_inferSchema));

        if (!StringUtils.isBlank(m_comment)) {
            jobInput.setOption(CFG_COMMENT, m_comment);
        }

        jobInput.setOption(CFG_NULL_VALUE, m_nullValue);

        if (!StringUtils.isBlank(m_dateFormat)) {
            jobInput.setOption(CFG_DATE_FORMAT, m_dateFormat);
        }

        super.addReaderOptions(jobInput);
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
