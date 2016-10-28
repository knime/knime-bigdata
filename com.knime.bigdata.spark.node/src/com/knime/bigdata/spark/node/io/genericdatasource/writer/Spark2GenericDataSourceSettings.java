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
package com.knime.bigdata.spark.node.io.genericdatasource.writer;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;

import com.knime.bigdata.spark.node.SparkSaveMode;

/**
 * Settings for a generic spark writer node.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2GenericDataSourceSettings {

    /** Short or long format name in spark. */
    private final String m_format;

    /** This settings support partitioning over columns. */
    private final boolean m_supportsPartitioning;

    /** This data source has needs an additional driver jar. */
    private final boolean m_hasDriver;

    /** Required output directory. */
    private static final String CFG_DIRECTORY = "outputDirectory";
    private static final String DEFAULT_DIRECTORY = "/";
    private String m_directory = DEFAULT_DIRECTORY;

    /** Required output name. */
    private static final String CFG_NAME = "outputName";
    private static final String DEFAULT_NAME = "";
    private String m_name = DEFAULT_NAME;

    /** Required output save mode. */
    private static final String CFG_SAVE_MODE = "saveMode";
    private static final SparkSaveMode DEFAULT_SAVE_MODE = SparkSaveMode.DEFAULT;
    private SparkSaveMode m_saveMode = DEFAULT_SAVE_MODE;

    /** Upload bundled jar. */
    private static final String CFG_UPLOAD_DRIVER = "uploadDriver";
    private static final boolean DEFAULT_UPLOAD_DRIVER = true;
    private boolean m_uploadDriver = DEFAULT_UPLOAD_DRIVER;

    /** Partition data by columns */
    private static final String CFG_PARTITION_BY = "partitionBy";
    private DataColumnSpecFilterConfiguration m_partitionBy = null;

    /** Overwrite result partition count (@see #setNumPartitions(int)) */
    private static final String CFG_OVERWRITE_NUM_PARTITIONS = "overwriteNumPartitions";
    private static final boolean DEFAULT_OVERWRITE_NUM_PARTITIONS = false;
    private boolean m_overwriteNumPartitions = DEFAULT_OVERWRITE_NUM_PARTITIONS;

    /** Partition count (@see {@link #setOverwriteNumPartitions(boolean)}) */
    private static final String CFG_NUM_PARTITIONS = "numPartitions";
    private static final int DEFAULT_NUM_PARTITIONS = 1;
    private int m_numPartitions = DEFAULT_NUM_PARTITIONS;

    /**
     * Default construct.
     * Custom constructors should overwrite {@link #newInstance()} too.
     * @param format - Short or long format name in spark.
     * @param supportsPartitioning - True if this format has partition by column support.
     * @param hasDriver - True if this data source has a driver jar.
     */
    public Spark2GenericDataSourceSettings(final String format, final boolean supportsPartitioning, final boolean hasDriver) {
        m_format = format;
        m_supportsPartitioning = supportsPartitioning;
        m_hasDriver = hasDriver;
    }

    /** @return New instance of this settings (overwrite this in custom settings) */
    protected Spark2GenericDataSourceSettings newInstance() {
        return new Spark2GenericDataSourceSettings(m_format, m_supportsPartitioning, m_hasDriver);
    }

    /** @return Spark format name */
    public String getFormat() { return m_format; }

    /** @return True if this data source requires additional jar files */
    public boolean hasDriver() { return m_hasDriver; }

    /** @return Absolute output directory */
    public String getDirectory() { return m_directory; }
    /** @param directory - Absolute output directory */
    public void setDirectory(final String directory) { m_directory = directory; }

    /** @return Output name */
    public String getName() { return m_name; }
    /** @param name - Output name */
    public void setName(final String name) { m_name = name; }

    /** @return Spark save mode as string */
    public String getSaveMode() { return m_saveMode.toSparkKey(); }
    /** @return Spark save mode */
    public SparkSaveMode getSparkSaveMode() { return m_saveMode; }
    /** @param saveMode - Spark save mode (see {@link SparkSaveMode}) */
    public void setSaveMode(final SparkSaveMode saveMode) { m_saveMode = saveMode; }

    /** @return True if bundled jar should be uploaded */
    public boolean uploadDriver() { return m_uploadDriver; }
    /** @param uploadDriver - True if bundled jars should be uploaded */
    public void setUploadDriver(final boolean uploadDriver) { m_uploadDriver = uploadDriver; }

    /** @return True if this data source supports partitioning */
    public boolean supportsPartitioning() { return m_supportsPartitioning; }
    /** @return Partition column filter configuration */
    public DataColumnSpecFilterConfiguration getPartitionBy() { return m_partitionBy; }

    /** @return Number of partition (see {@link Spark2GenericDataSourceSettings#overwriteNumPartitions}) */
    public int getNumPartitions() { return m_numPartitions; }
    /** @param numPartitions - Number of output partitions */
    public void setNumPartitions(final int numPartitions) { m_numPartitions = numPartitions; }

    /** @return True if output number of partition should be overwritten (see {@link Spark2GenericDataSourceSettings#setNumPartitions}) */
    public boolean overwriteNumPartitions() { return m_overwriteNumPartitions; }
    /** @param overwriteNumPartitions - True if number of output partition should be overwritten */
    public void setOverwriteNumPartitions(final boolean overwriteNumPartitions) { m_overwriteNumPartitions = overwriteNumPartitions; }

    /** @param settings - Settings to save current settings in */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_DIRECTORY, m_directory);
        settings.addString(CFG_NAME, m_name);
        settings.addString(CFG_SAVE_MODE, m_saveMode.toSparkKey());
        settings.addBoolean(CFG_UPLOAD_DRIVER, m_uploadDriver);

        if (m_supportsPartitioning) {
            m_partitionBy.saveConfiguration(settings);
        }

        settings.addBoolean(CFG_OVERWRITE_NUM_PARTITIONS, m_overwriteNumPartitions);
        settings.addInt(CFG_NUM_PARTITIONS, m_numPartitions);
    }

    /**
     * @param settings - Settings to validate and not load
     * @throws InvalidSettingsException
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        Spark2GenericDataSourceSettings tmp = newInstance();
        tmp.loadSettings(settings);
        tmp.validateSettings();
    }

    /**
     * Validate current settings
     * @throws InvalidSettingsException
     */
    public void validateSettings() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_directory)) {
            throw new InvalidSettingsException("Output directory name required.");
        }

        if (StringUtils.isBlank(m_name)) {
            throw new InvalidSettingsException("Output name required.");
        }

        if (m_overwriteNumPartitions && m_numPartitions <= 0) {
            throw new InvalidSettingsException("Invalid partition count.");
        }
    }

    /**
     * @param settings - Already validated settings to load
     * @throws InvalidSettingsException
     */
    public void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        loadSettings(settings);
        if (m_supportsPartitioning && settings.containsKey(CFG_PARTITION_BY)) {
            m_partitionBy = new DataColumnSpecFilterConfiguration(CFG_PARTITION_BY);
            m_partitionBy.loadConfigurationInModel(settings);
        } else {
            m_partitionBy = null;
        }
    }

    /**
     * Loads the settings from the given settings object using default values for invalid or missing settings.
     * @param settings - Settings to load
     */
    protected void loadSettings(final NodeSettingsRO settings) {
        m_directory = settings.getString(CFG_DIRECTORY, DEFAULT_DIRECTORY);
        m_name = settings.getString(CFG_NAME, DEFAULT_NAME);
        m_saveMode = SparkSaveMode.fromSparkKey(settings.getString(CFG_SAVE_MODE, DEFAULT_SAVE_MODE.toSparkKey()));
        m_uploadDriver = settings.getBoolean(CFG_UPLOAD_DRIVER, DEFAULT_UPLOAD_DRIVER);
        m_overwriteNumPartitions = settings.getBoolean(CFG_OVERWRITE_NUM_PARTITIONS, DEFAULT_OVERWRITE_NUM_PARTITIONS);
        m_numPartitions = settings.getInt(CFG_NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
    }

    /**
     * Guess default settings by provided table spec.
     * @param spec - Spark input data table spec.
     */
    public void loadDefault(final DataTableSpec spec) {
        if (m_partitionBy == null) {
            m_partitionBy = new DataColumnSpecFilterConfiguration(CFG_PARTITION_BY);
            m_partitionBy.loadDefault(spec, null, false);
        }
    }

    /**
     * Loads the configuration in the dialog (no exception thrown) and maps it to the input spec.
     *
     * @param settings The settings to load from.
     * @param spec The non-null spec.
     */
    public void loadConfigurationInDialog(final NodeSettingsRO settings, final DataTableSpec spec) {
        loadSettings(settings);
        if (m_supportsPartitioning && spec != null) {
            loadDefault(spec);
            if (settings.containsKey(CFG_PARTITION_BY)) {
                m_partitionBy.loadConfigurationInDialog(settings, spec);
            }
        }
    }

    /** @param jobInput - Job input to add custom writer options */
    public void addWriterOptions(final Spark2GenericDataSourceJobInput jobInput) {
        // Overwrite this in custom settings
    }
}
