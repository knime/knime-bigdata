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
 *   Created on Aug 10, 2016 by sascha
 */
package com.knime.bigdata.spark.node.io.parquet.reader;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the parquet to spark node.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SuppressWarnings("javadoc")
public class Parquet2SparkSettings {

    /** Required input path. */
    private final String CFG_INPUT_PATH = "inputPath";
    private final String DEFAULT_INPUT_PATH = "";
    private String m_path = DEFAULT_INPUT_PATH;

    public String getInputPath() { return m_path; }
    public void setInputPath(final String path) { m_path = path; }


    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_INPUT_PATH, m_path);
    }

    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        Parquet2SparkSettings tmp = new Parquet2SparkSettings();
        tmp.loadSettings(settings);
        tmp.validateSettings();
    }

    public void validateSettings() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_path)) {
            throw new InvalidSettingsException("No parquet input table path provided.");
        }
    }

    public void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadSettings(settings);
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     */
    public void loadSettings(final NodeSettingsRO settings) {
        m_path = settings.getString(CFG_INPUT_PATH, DEFAULT_INPUT_PATH);
    }

}
