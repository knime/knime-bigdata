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
 *   Created on Sep 06, 2016 by Sascha
 */
package com.knime.bigdata.spark.node.io.database.writer;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import com.knime.bigdata.spark.node.SparkSaveMode;

/**
 * Settings for the Spark to JDBC node.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SuppressWarnings("javadoc")
public class Spark2DatabaseSettings {

    /** Required destination table name. */
    private final String CFG_TABLE = "table";
    private final String DEFAULT_TABLE = "";
    private String m_table = DEFAULT_TABLE;

    /** Optional driver class to load. */
    private final String CFG_UPLOAD_DRIVER = "uploadDriver";
    private final boolean DEFAULT_UPLOAD_DRIVER = true;
    private boolean m_uploadDriver = DEFAULT_UPLOAD_DRIVER;

    /** Required save mode. */
    private final String CFG_SAVE_MODE = "saveMode";
    private final SparkSaveMode DEFAULT_SAVE_MODE = SparkSaveMode.DEFAULT;
    private SparkSaveMode m_saveMode = DEFAULT_SAVE_MODE;

    public boolean uploadDriver() { return m_uploadDriver; }
    public void setUploadDriver(final boolean uploadDriver) { m_uploadDriver = uploadDriver; }

    public String getTable() { return m_table; }
    public void setTable(final String table) { m_table = table; }

    public String getSaveMode() { return m_saveMode.toSparkKey(); }
    public SparkSaveMode getSparkSaveMode() { return m_saveMode; }
    public void setSaveMode(final SparkSaveMode saveMode) { m_saveMode = saveMode; }


    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addBoolean(CFG_UPLOAD_DRIVER, m_uploadDriver);
        settings.addString(CFG_TABLE, m_table);
        settings.addString(CFG_SAVE_MODE, m_saveMode.toSparkKey());
    }

    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        Spark2DatabaseSettings tmp = new Spark2DatabaseSettings();
        tmp.loadSettings(settings);
        tmp.validateSettings();
    }

    public void validateSettings() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_table)) {
            throw new InvalidSettingsException("Table name required.");
        }

        if (m_saveMode == null) {
            throw new InvalidSettingsException("Save mode required.");
        }
    }

    public void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadSettings(settings);
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     */
    public void loadSettings(final NodeSettingsRO settings) {
        m_uploadDriver = settings.getBoolean(CFG_UPLOAD_DRIVER, DEFAULT_UPLOAD_DRIVER);
        m_table = settings.getString(CFG_TABLE, DEFAULT_TABLE);
        m_saveMode = SparkSaveMode.fromSparkKey(settings.getString(CFG_SAVE_MODE, DEFAULT_SAVE_MODE.toSparkKey()));
    }
}
