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

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

import com.knime.bigdata.spark.node.SparkSaveMode;

/**
 * Settings for the spark to parquet node.
 *
 * @author Sascha Wolke, KNIME.com
 */
@SuppressWarnings("javadoc")
public class Spark2ParquetSettings {

    /** Required output directory. */
    private final String CFG_DIRECTORY = "directory";
    private final String DEFAULT_DIRECTORY = "/";
    private String m_directory = DEFAULT_DIRECTORY;

    /** Required output table name. */
    private final String CFG_TABLE_NAME = "tableName";
    private final String DEFAULT_TABLE_NAME = "";
    private String m_tableName = DEFAULT_TABLE_NAME;

    /** Required output save mode. */
    private final String CFG_SAVE_MODE = "saveMode";
    private final SparkSaveMode DEFAULT_SAVE_MODE = SparkSaveMode.DEFAULT;
    private SparkSaveMode m_saveMode = DEFAULT_SAVE_MODE;

    public String getDirectory() { return m_directory; }
    public void setDirectory(final String directory) { m_directory = directory; }

    public String getTableName() { return m_tableName; }
    public void setTableName(final String filename) { m_tableName = filename; }

    public String getSaveMode() { return m_saveMode.toSparkKey(); }
    public SparkSaveMode getSparkSaveMode() { return m_saveMode; }
    public void setSaveMode(final SparkSaveMode saveMode) { m_saveMode = saveMode; }


    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_DIRECTORY, m_directory);
        settings.addString(CFG_TABLE_NAME, m_tableName);
        settings.addString(CFG_SAVE_MODE, m_saveMode.toSparkKey());
    }

    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        Spark2ParquetSettings tmp = new Spark2ParquetSettings();
        tmp.loadSettings(settings);
        tmp.validateSettings();
    }

    public void validateSettings() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_directory)) {
            throw new InvalidSettingsException("Output directory name required.");
        }

        if (StringUtils.isBlank(m_tableName)) {
            throw new InvalidSettingsException("Output table name required.");
        }
    }

    public void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadSettings(settings);
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     */
    public void loadSettings(final NodeSettingsRO settings) {
        m_directory = settings.getString(CFG_DIRECTORY, DEFAULT_DIRECTORY);
        m_tableName = settings.getString(CFG_TABLE_NAME, DEFAULT_TABLE_NAME);
        m_saveMode = SparkSaveMode.fromSparkKey(settings.getString(CFG_SAVE_MODE, DEFAULT_SAVE_MODE.toSparkKey()));
    }
}
