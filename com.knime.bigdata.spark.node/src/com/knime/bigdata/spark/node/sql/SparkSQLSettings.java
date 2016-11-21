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
 */
package com.knime.bigdata.spark.node.sql;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for Spark SQL Executor node.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class SparkSQLSettings {

    /** Query to execute */
    private static final String CFG_QUERY = "query";
    private static final String DEFAULT_QUERY = "SELECT * FROM #table#";
    private String m_query = DEFAULT_QUERY;

    /** @return Query to execute */
    public String getQuery() { return m_query; }

    /** @param query - Spark SQL query with table and flow variables placeholder */
    public void setQuery(final String query) { m_query = query; }

    /** @param settings - Settings to save current settings in */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_QUERY, m_query);
    }

    /**
     * @param settings - Settings to validate and not load
     * @throws InvalidSettingsException
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        SparkSQLSettings tmp = new SparkSQLSettings();
        tmp.loadSettingsFrom(settings);
        tmp.validateSettings();
    }

    /**
     * Validate current settings
     * @throws InvalidSettingsException
     */
    public void validateSettings() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_query)) {
            throw new InvalidSettingsException("Query required.");
        }

        if (!m_query.contains(SparkSQLJobInput.TABLE_PLACEHOLDER)) {
            throw new InvalidSettingsException("Query does not contain temporary table placeholder " + SparkSQLJobInput.TABLE_PLACEHOLDER);
        }
    }

    /**
     * @param settings - Already validated settings to load
     * @throws InvalidSettingsException
     */
    public void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadSettingsFrom(settings);
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     * @param settings - Settings to load
     */
    public void loadSettingsFrom(final NodeSettingsRO settings) {
        m_query = settings.getString(CFG_QUERY, DEFAULT_QUERY);
    }
}
