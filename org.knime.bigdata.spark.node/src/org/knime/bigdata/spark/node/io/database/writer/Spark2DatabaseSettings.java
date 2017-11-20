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
package org.knime.bigdata.spark.node.io.database.writer;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import org.knime.bigdata.spark.node.SparkSaveMode;

/**
 * Settings for the Spark to JDBC node.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2DatabaseSettings {

    /** Required destination table name. */
    private final String CFG_TABLE = "table";
    private final String DEFAULT_TABLE = "";
    private String m_table = DEFAULT_TABLE;

    /** Optional driver class to load. */
    private final String CFG_UPLOAD_DRIVER = "uploadDriver";
    private final boolean DEFAULT_UPLOAD_DRIVER = false;
    private boolean m_uploadDriver = DEFAULT_UPLOAD_DRIVER;

    /** Required save mode. */
    private final String CFG_SAVE_MODE = "saveMode";
    private final SparkSaveMode DEFAULT_SAVE_MODE = SparkSaveMode.DEFAULT;
    private SparkSaveMode m_saveMode = DEFAULT_SAVE_MODE;

    /** @return True if bundled jar should be uploaded */
    public boolean uploadDriver() { return m_uploadDriver; }
    /** @param uploadDriver - True if bundled jars should be uploaded */
    public void setUploadDriver(final boolean uploadDriver) { m_uploadDriver = uploadDriver; }

    /** @return Destination table name */
    public String getTable() { return m_table; }
    /** @param table - Destination table name */
    public void setTable(final String table) { m_table = table; }

    /** @return Spark save mode as string */
    public String getSaveMode() { return m_saveMode.toSparkKey(); }
    /** @return Spark save mode */
    public SparkSaveMode getSparkSaveMode() { return m_saveMode; }
    /** @param saveMode - Spark save mode (see {@link SparkSaveMode}) */
    public void setSaveMode(final SparkSaveMode saveMode) { m_saveMode = saveMode; }

    /** @param settings - Settings to save current settings in */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addBoolean(CFG_UPLOAD_DRIVER, m_uploadDriver);
        settings.addString(CFG_TABLE, m_table);
        settings.addString(CFG_SAVE_MODE, m_saveMode.toSparkKey());
    }

    /**
     * @param settings - Settings to validate and not load
     * @throws InvalidSettingsException
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        Spark2DatabaseSettings tmp = new Spark2DatabaseSettings();
        tmp.loadSettings(settings);
        tmp.validateSettings();
    }

    /**
     * Validate current settings
     * @throws InvalidSettingsException
     */
    public void validateSettings() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_table)) {
            throw new InvalidSettingsException("Table name required.");
        }

        if (m_saveMode == null) {
            throw new InvalidSettingsException("Save mode required.");
        }
    }

    /**
     * @param settings - Already validated settings to load
     * @throws InvalidSettingsException
     */
    public void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadSettings(settings);
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     * @param settings - Settings to load
     */
    public void loadSettings(final NodeSettingsRO settings) {
        m_uploadDriver = settings.getBoolean(CFG_UPLOAD_DRIVER, DEFAULT_UPLOAD_DRIVER);
        m_table = settings.getString(CFG_TABLE, DEFAULT_TABLE);
        m_saveMode = SparkSaveMode.fromSparkKey(settings.getString(CFG_SAVE_MODE, DEFAULT_SAVE_MODE.toSparkKey()));
    }
}
