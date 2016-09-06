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

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the parquet to spark node.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class Parquet2SparkSettings {

    private final String CFG_INPUT_PATH = "inputPath";

    private String m_path;

    public String getInputPath() {
        return m_path;
    }

    public void setInputPath(final String path) {
        m_path = path;
    }

    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_INPUT_PATH, m_path);
    }

    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        final String filename = settings.getString(CFG_INPUT_PATH, null);
        if (filename == null || filename.trim().isEmpty()) {
            throw new InvalidSettingsException("No parquet table path provided.");
        }
    }

    public void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_path = settings.getString(CFG_INPUT_PATH);
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        m_path = settings.getString(CFG_INPUT_PATH, "");
    }

}
