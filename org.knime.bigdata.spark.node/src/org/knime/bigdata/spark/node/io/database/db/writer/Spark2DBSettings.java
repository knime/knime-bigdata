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
 *   Created on Sep 06, 2016 by Sascha
 */
package org.knime.bigdata.spark.node.io.database.db.writer;

import org.apache.commons.lang3.StringUtils;
import org.knime.bigdata.spark.node.SparkSaveMode;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.database.node.component.dbrowser.SettingsModelDBMetadata;

/**
 * Settings for the Spark to JDBC node.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2DBSettings {

    private final SettingsModelDBMetadata m_table = new SettingsModelDBMetadata("table", false);

    private final SettingsModelBoolean m_uploadDriver = new SettingsModelBoolean("uploadDriver", false);

    private final SettingsModelString m_saveMode =
        new SettingsModelString("saveMode", SparkSaveMode.DEFAULT.toSparkKey());

    /**
     * @return internal settings model for schema and table name
     */
    SettingsModelDBMetadata getSchemaAndTableModel() {
        return m_table;
    }

    /**
     * @return optional schema name (might be empty)
     */
    String getSchema() {
        return m_table.getSchema();
    }

    /**
     * @return required table name
     */
    String getTable() {
        return m_table.getTable();
    }

    /**
     * @return <code>true</code> if driver should be uploaded
     */
    boolean uploadDriver() {
        return m_uploadDriver.getBooleanValue();
    }

    /**
     * @param upload <code>true</code> if driver should be uploaded
     */
    void setUploadDriver(final boolean upload) {
        m_uploadDriver.setBooleanValue(upload);
    }

    /**
     * @return save mode as spark key
     */
    String getSaveMode() {
        return m_saveMode.getStringValue();
    }

    /**
     * @return save mode as {@link SparkSaveMode}
     */
    SparkSaveMode getSparkSaveMode() {
        return SparkSaveMode.fromSparkKey(m_saveMode.getStringValue());
    }

    /**
     * @param mode save mode to use
     */
    void setSparkSaveMode(final SparkSaveMode mode) {
        m_saveMode.setStringValue(mode.toSparkKey());
    }

    /**
     * @param settings - Settings to save current settings in
     */
    void saveSettingsTo(final NodeSettingsWO settings) {
        m_table.saveSettingsTo(settings);
        m_uploadDriver.saveSettingsTo(settings);
        m_saveMode.saveSettingsTo(settings);
    }

    /**
     * @param settings - Settings to validate and not load
     * @throws InvalidSettingsException
     */
    void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_table.validateSettings(settings);
        m_uploadDriver.validateSettings(settings);
        m_saveMode.validateSettings(settings);

        Spark2DBSettings tmp = new Spark2DBSettings();
        tmp.loadSettings(settings);
        tmp.validateSettings();
    }

    /**
     * Validate current settings
     * @throws InvalidSettingsException
     */
    void validateSettings() throws InvalidSettingsException {
        if (StringUtils.isBlank(getTable())) {
            throw new InvalidSettingsException("Table name required.");
        }

        if (StringUtils.isBlank(getSaveMode())) {
            throw new InvalidSettingsException("Save mode required.");
        }
    }

    /**
     * @param settings - Already validated settings to load
     * @throws InvalidSettingsException
     */
    void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadSettings(settings);
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     * @param settings - Settings to load
     * @throws InvalidSettingsException
     */
    void loadSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_table.loadSettingsFrom(settings);
        m_uploadDriver.loadSettingsFrom(settings);
        m_saveMode.loadSettingsFrom(settings);
    }
}
