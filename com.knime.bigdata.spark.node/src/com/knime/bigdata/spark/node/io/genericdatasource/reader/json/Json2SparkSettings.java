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
package com.knime.bigdata.spark.node.io.genericdatasource.reader.json;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import com.knime.bigdata.spark.core.version.SparkVersion;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkJobInput;
import com.knime.bigdata.spark.node.io.genericdatasource.reader.GenericDataSource2SparkSettings;

/**
 * JSON specific reader settings.
 * @author Sascha Wolke, KNIME.com
 * Śee JSONOptions.scala in Spark
 */
public class Json2SparkSettings extends GenericDataSource2SparkSettings {

    /** Use all or only some samples to infer schema */
    private static final String CFG_SCHEMA_SAMPLING_RATIO = "samplingRatio";
    private static final double DEFAULT_SCHEMA_SAMPLING_RATIO = 1.0;
    private double m_samplingRatio = DEFAULT_SCHEMA_SAMPLING_RATIO;

    /** Map primitives to string */
    private static final String CFG_PRIMITIVE_AS_STRING = "primitivesAsString";
    private static final boolean DEFAULT_PRIMITIVE_AS_STRING = false;
    private boolean m_primitivesAsString = DEFAULT_PRIMITIVE_AS_STRING;

    /** Allow comments */
    private static final String CFG_ALLOW_COMMENTS = "allowComments";
    private static final boolean DEFAULT_ALLOW_COMMENTS = false;
    private boolean m_allowComments = DEFAULT_ALLOW_COMMENTS;

    /** Allow unquoted fieldnames */
    private static final String CFG_ALLOW_UNQUOTED_FIELD_NAMES = "allowUnquotedFieldNames";
    private static final boolean DEFAULT_ALLOW_UNQUOTED_FIELD_NAMES = false;
    private boolean m_allowUnquotedFieldNames = DEFAULT_ALLOW_UNQUOTED_FIELD_NAMES;

    /** Allow single quotes */
    private static final String CFG_ALLOW_SINGLE_QUOTES = "allowSingleQuotes";
    private static final boolean DEFAULT_ALLOW_SINGLE_QUOTES = true;
    private boolean m_allowSingleQuotes = DEFAULT_ALLOW_SINGLE_QUOTES;

    /** Allow numeric leading zeros */
    private static final String CFG_ALLOW_NUMERIC_LEADING_ZEROS = "allowNumericLeadingZeros";
    private static final boolean DEFAULT_ALLOW_NUMERIC_LEADING_ZEROS = false;
    private boolean m_allowNumericLeadingZeros = DEFAULT_ALLOW_NUMERIC_LEADING_ZEROS;

    /** Allow non numeric numbers (e.g. -inf) */
    private static final String CFG_ALLOW_NON_NUMERIC_NUMBERS = "allowNonNumericNumbers";
    private static final boolean DEFAULT_ALLOW_NON_NUMERIC_NUMBERS = false;
    private boolean m_allowNonNumericNumbers = DEFAULT_ALLOW_NON_NUMERIC_NUMBERS;

    /** @see GenericDataSource2SparkSettings#GenericDataSource2SparkSettings(String, boolean) */
    public Json2SparkSettings(final String format, final SparkVersion minSparkVersion, final boolean hasDriver) {
        super(format, minSparkVersion, hasDriver);
    }

    @Override
    protected GenericDataSource2SparkSettings newInstance() {
        return new Json2SparkSettings(getFormat(), getMinSparkVersion(), hasDriver());
    }

    /** @return Sampling ratio to infer schema from */
    public double getSchemaSamplingRatio() { return m_samplingRatio; }
    /** @param samplingRatio - Sampling ratio to infer schema from */
    public void setSchemaSamplingRatio(final double samplingRatio) { m_samplingRatio = samplingRatio; }

    /** @return True if primitives should be converted to strings */
    public boolean primitivesAsString() { return m_primitivesAsString; }
    /** @param primitivesAsString - True if primitives should be converted to strings */
    public void setPrimitivesAsString(final boolean primitivesAsString) { m_primitivesAsString = primitivesAsString; }

    /** @return True if comments are allowed */
    public boolean allowComments() { return m_allowComments; }
    /** @param allowComments - True if comments are allowed */
    public void setAllowComments(final boolean allowComments) { m_allowComments = allowComments; }

    /** @return True if unquoted field names are allowed */
    public boolean allowUnquotedFieldNames() { return m_allowUnquotedFieldNames; }
    /** @param allowUnquotedFieldNames - True if unquoted field names are allowed */
    public void setAllowUnquotedFieldNames(final boolean allowUnquotedFieldNames) { m_allowUnquotedFieldNames = allowUnquotedFieldNames; }

    /** @return True if single quotes are allowed */
    public boolean allowSingleQuotes() { return m_allowSingleQuotes; }
    /** @param allowSingleQuotes - True if single quotes are allowed */
    public void setAllowSingleQuotes(final boolean allowSingleQuotes) { m_allowSingleQuotes = allowSingleQuotes; }

    /** @return True if leading zeros are allowed */
    public boolean allowNumericLeadingZeros() { return m_allowNumericLeadingZeros; }
    /** @param allowNumericLeadingZeros - True if leading zeros are allowed */
    public void setAllowNumericLeadingZeros(final boolean allowNumericLeadingZeros) { m_allowNumericLeadingZeros = allowNumericLeadingZeros; }

    /** @return True if non numeric numbers (e.g. -inf) are allowed) */
    public boolean allowNonNumericNumbers() { return m_allowNonNumericNumbers; }
    /** @param allowNonNumericNumbers - True if non numeric numbers (e.g. -inf) are allowed) */
    public void setAllowNonNumericNumbers(final boolean allowNonNumericNumbers) { m_allowNonNumericNumbers = allowNonNumericNumbers; }

    @Override
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addDouble(CFG_SCHEMA_SAMPLING_RATIO, m_samplingRatio);
        settings.addBoolean(CFG_PRIMITIVE_AS_STRING, m_primitivesAsString);
        settings.addBoolean(CFG_ALLOW_COMMENTS, m_allowComments);
        settings.addBoolean(CFG_ALLOW_UNQUOTED_FIELD_NAMES, m_allowUnquotedFieldNames);
        settings.addBoolean(CFG_ALLOW_SINGLE_QUOTES, m_allowSingleQuotes);
        settings.addBoolean(CFG_ALLOW_NUMERIC_LEADING_ZEROS, m_allowNumericLeadingZeros);
        settings.addBoolean(CFG_ALLOW_NON_NUMERIC_NUMBERS, m_allowNonNumericNumbers);

        super.saveSettingsTo(settings);
    }

    @Override
    public void validateSettings() throws InvalidSettingsException {
        super.validateSettings();
    }

    @Override
    public void loadSettings(final NodeSettingsRO settings) {
        m_samplingRatio = settings.getDouble(CFG_SCHEMA_SAMPLING_RATIO, DEFAULT_SCHEMA_SAMPLING_RATIO);
        m_primitivesAsString = settings.getBoolean(CFG_PRIMITIVE_AS_STRING, DEFAULT_PRIMITIVE_AS_STRING);
        m_allowComments = settings.getBoolean(CFG_ALLOW_COMMENTS, DEFAULT_ALLOW_COMMENTS);
        m_allowUnquotedFieldNames = settings.getBoolean(CFG_ALLOW_UNQUOTED_FIELD_NAMES, DEFAULT_ALLOW_UNQUOTED_FIELD_NAMES);
        m_allowSingleQuotes = settings.getBoolean(CFG_ALLOW_SINGLE_QUOTES, DEFAULT_ALLOW_SINGLE_QUOTES);
        m_allowNumericLeadingZeros = settings.getBoolean(CFG_ALLOW_NUMERIC_LEADING_ZEROS, DEFAULT_ALLOW_NUMERIC_LEADING_ZEROS);
        m_allowNonNumericNumbers = settings.getBoolean(CFG_ALLOW_NON_NUMERIC_NUMBERS, DEFAULT_ALLOW_NON_NUMERIC_NUMBERS);

        super.loadSettings(settings);
    }

    @Override
    public void addReaderOptions(final GenericDataSource2SparkJobInput jobInput) {
        jobInput.setOption(CFG_SCHEMA_SAMPLING_RATIO, Double.toString(m_samplingRatio));
        jobInput.setOption(CFG_PRIMITIVE_AS_STRING, Boolean.toString(m_primitivesAsString));
        jobInput.setOption(CFG_ALLOW_COMMENTS, Boolean.toString(m_allowComments));
        jobInput.setOption(CFG_ALLOW_UNQUOTED_FIELD_NAMES, Boolean.toString(m_allowUnquotedFieldNames));
        jobInput.setOption(CFG_ALLOW_SINGLE_QUOTES, Boolean.toString(m_allowSingleQuotes));
        jobInput.setOption(CFG_ALLOW_NUMERIC_LEADING_ZEROS, Boolean.toString(m_allowNumericLeadingZeros));
        jobInput.setOption(CFG_ALLOW_NON_NUMERIC_NUMBERS, Boolean.toString(m_allowNonNumericNumbers));

        super.addReaderOptions(jobInput);
    }
}
