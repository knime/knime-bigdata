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
package com.knime.bigdata.spark.node.io.parquet.writer;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * Settings for the spark to parquet node.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2ParquetSettings {

    private final String CFG_DIRECTORY = "directory";
    private final String CFG_TABLE_NAME = "tableName";
    private final String CFG_SAVE_MODE = "saveMode";

    private String m_directory;
    private String m_tableName;
    private String m_saveMode;

    public String getDirectory() {
        return m_directory;
    }

    public void setDirectory(final String directory) {
        m_directory = directory;
    }

    public String getTableName() {
        return m_tableName;
    }

    public void setTableName(final String filename) {
        m_tableName = filename;
    }

    public String getSaveMode() {
        return m_saveMode;
    }

    public void setSaveMode(final String saveMode) {
        m_saveMode = saveMode;
    }

    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_DIRECTORY, m_directory);
        settings.addString(CFG_TABLE_NAME, m_tableName);
        settings.addString(CFG_SAVE_MODE, m_saveMode);
    }

    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        validateNotEmpty(settings, CFG_DIRECTORY, "directory");
        validateNotEmpty(settings, CFG_TABLE_NAME, "filename");
        validateNotEmpty(settings, CFG_SAVE_MODE, "SaveMode");

    }

    private void validateNotEmpty(final NodeSettingsRO settings, final String cfg, final String description) throws InvalidSettingsException {
        final String value = settings.getString(cfg, null);
        if (value == null || value.trim().isEmpty()) {
            throw new InvalidSettingsException("No " + description + " provided or empty.");
        }
    }

    public void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_directory = settings.getString(CFG_DIRECTORY);
        m_tableName = settings.getString(CFG_TABLE_NAME);
        m_saveMode = settings.getString(CFG_SAVE_MODE);
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     */
    public void loadSettingsForDialog(final NodeSettingsRO settings) {
        m_directory = loadSetting(settings, CFG_DIRECTORY, "/");
        m_tableName = loadSetting(settings, CFG_TABLE_NAME, "output.parquet");
        m_saveMode = loadSetting(settings, CFG_SAVE_MODE, "Error");
    }

    /**
     * Loads setting with given key and returns default if key not set or setting is empty.
     * @return Default value if key not set or value is empty
     */
    public String loadSetting(final NodeSettingsRO settings, final String key, final String def) {
        String value = settings.getString(key, "");

        if (value == null || value.trim().isEmpty()) {
            value = def;
        }

        return value;
    }
}
