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
 *   Created on Aug 10, 2016 by sascha
 */
package org.knime.bigdata.spark.node.io.genericdatasource.filehandling.writer;

import org.knime.bigdata.spark.core.version.SparkVersion;
import org.knime.bigdata.spark.node.SparkSaveMode;
import org.knime.bigdata.spark.node.io.genericdatasource.writer.Spark2GenericDataSourceJobInput;
import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.context.ports.PortsConfiguration;
import org.knime.core.node.util.filter.column.DataColumnSpecFilterConfiguration;
import org.knime.filehandling.core.defaultnodesettings.EnumConfig;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.FileOverwritePolicy;
import org.knime.filehandling.core.defaultnodesettings.filechooser.writer.SettingsModelWriterFileChooser;
import org.knime.filehandling.core.defaultnodesettings.filtermode.SettingsModelFilterMode.FilterMode;

/**
 * Settings for a generic spark writer node.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class Spark2GenericDataSourceSettings3 {

    /** Short or long format name in spark. */
    private final String m_format;

    /** Required spark version. */
    private final SparkVersion m_minSparkVersion;

    /** This settings support partitioning over columns. */
    private final boolean m_supportsPartitioning;

    /** This data source has needs an additional driver jar. */
    private final boolean m_hasDriver;

    /** Required output path. */
    public static final String CFG_OUTPUT_PATH = "outputPath";

    private final SettingsModelWriterFileChooser m_outputPathChooser;

    /** Upload bundled jar. */
    private static final String CFG_UPLOAD_DRIVER = "uploadDriver";

    private static final boolean DEFAULT_UPLOAD_DRIVER = false;

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
     * Default construct. Custom constructors should overwrite {@link #newValidateInstance()} too.
     *
     * @param format - Short or long format name in spark.
     * @param minSparkVersion - Minimum spark version.
     * @param supportsPartitioning - True if this format has partition by column support.
     * @param hasDriver - True if this data source has a driver jar.
     * @param portsConfig - Current ports configuration.
     */
    public Spark2GenericDataSourceSettings3(final String format, final SparkVersion minSparkVersion,
        final boolean supportsPartitioning, final boolean hasDriver, final PortsConfiguration portsConfig) {

        m_format = format;
        m_minSparkVersion = minSparkVersion;
        m_supportsPartitioning = supportsPartitioning;
        m_hasDriver = hasDriver;
        m_outputPathChooser = new SettingsModelWriterFileChooser(CFG_OUTPUT_PATH, portsConfig,
            Spark2GenericDataSourceNodeFactory3.FS_INPUT_PORT_GRP_NAME,
            EnumConfig.create(FilterMode.FOLDER), EnumConfig.create(FileOverwritePolicy.FAIL,
                FileOverwritePolicy.OVERWRITE, FileOverwritePolicy.APPEND, FileOverwritePolicy.IGNORE));
    }

    /**
     * Constructor used in validation.
     *
     * @param format - Short or long format name in spark.
     * @param minSparkVersion - Minimum spark version.
     * @param supportsPartitioning - True if this format has partition by column support.
     * @param hasDriver - True if this data source has a driver jar.
     * @param outputPathChooser - The output file chooser.
     */
    protected Spark2GenericDataSourceSettings3(final String format, final SparkVersion minSparkVersion,
        final boolean supportsPartitioning, final boolean hasDriver,
        final SettingsModelWriterFileChooser outputPathChooser) {

        m_format = format;
        m_minSparkVersion = minSparkVersion;
        m_supportsPartitioning = supportsPartitioning;
        m_hasDriver = hasDriver;
        m_outputPathChooser = outputPathChooser;
    }

    /** @return New instance of this settings (overwrite this in custom settings) */
    protected Spark2GenericDataSourceSettings3 newValidateInstance() {
        return new Spark2GenericDataSourceSettings3(m_format, m_minSparkVersion, m_supportsPartitioning, m_hasDriver,
            m_outputPathChooser);
    }

    /** @return Spark format name */
    public String getFormat() {
        return m_format;
    }

    /**
     * @param otherVersion - Version to check
     * @return <code>true</code> if version is compatible
     */
    public boolean isCompatibleSparkVersion(final SparkVersion otherVersion) {
        return m_minSparkVersion.compareTo(otherVersion) <= 0;
    }

    /** @return Minimum required spark version */
    public SparkVersion getMinSparkVersion() {
        return m_minSparkVersion;
    }

    /** @return True if this data source requires additional jar files */
    public boolean hasDriver() {
        return m_hasDriver;
    }

    /**
     * @return the output path file chooser model
     */
    public SettingsModelWriterFileChooser getFileChooserModel() {
        return m_outputPathChooser;
    }

    /**
     * @return Spark save mode as string
     */
    public String getSaveMode() {
        return SparkSaveMode.toSparkSaveModeKey(m_outputPathChooser.getFileOverwritePolicy());
    }

    /**
     * @return {@code true} if the save mode is append, {@code false} otherwise
     */
    public boolean isAppendMode() {
        return m_outputPathChooser.getFileOverwritePolicy() == FileOverwritePolicy.APPEND;
    }

    /** @return True if bundled jar should be uploaded */
    public boolean uploadDriver() {
        return m_uploadDriver;
    }

    /** @param uploadDriver - True if bundled jars should be uploaded */
    public void setUploadDriver(final boolean uploadDriver) {
        m_uploadDriver = uploadDriver;
    }

    /** @return True if this data source supports partitioning */
    public boolean supportsPartitioning() {
        return m_supportsPartitioning;
    }

    /** @return Partition column filter configuration */
    public DataColumnSpecFilterConfiguration getPartitionBy() {
        return m_partitionBy;
    }

    /** @return Number of partition (see {@link Spark2GenericDataSourceSettings3#overwriteNumPartitions}) */
    public int getNumPartitions() {
        return m_numPartitions;
    }

    /** @param numPartitions - Number of output partitions */
    public void setNumPartitions(final int numPartitions) {
        m_numPartitions = numPartitions;
    }

    /**
     * @return True if output number of partition should be overwritten (see
     *         {@link Spark2GenericDataSourceSettings3#setNumPartitions})
     */
    public boolean overwriteNumPartitions() {
        return m_overwriteNumPartitions;
    }

    /** @param overwriteNumPartitions - True if number of output partition should be overwritten */
    public void setOverwriteNumPartitions(final boolean overwriteNumPartitions) {
        m_overwriteNumPartitions = overwriteNumPartitions;
    }

    /**
     * Internal method to save settings.
     *
     * @param settings node settings to write to
     */
    protected void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addBoolean(CFG_UPLOAD_DRIVER, m_uploadDriver);

        if (m_supportsPartitioning && m_partitionBy != null) {
            m_partitionBy.saveConfiguration(settings);
        }

        settings.addBoolean(CFG_OVERWRITE_NUM_PARTITIONS, m_overwriteNumPartitions);
        settings.addInt(CFG_NUM_PARTITIONS, m_numPartitions);
    }

    void saveSettingsToModel(final NodeSettingsWO settings) {
        m_outputPathChooser.saveSettingsTo(settings);
        saveSettingsTo(settings);
    }

    void saveSettingsToDialog(final NodeSettingsWO settings) {
        saveSettingsTo(settings);
    }

    /**
     * @param settings - Settings to validate and not load
     * @throws InvalidSettingsException
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_outputPathChooser.validateSettings(settings);

        Spark2GenericDataSourceSettings3 tmp = newValidateInstance();
        tmp.loadSettings(settings);
        tmp.validateSettings();
    }

    /**
     * Validate current settings
     *
     * @throws InvalidSettingsException
     */
    public void validateSettings() throws InvalidSettingsException {
        if (m_overwriteNumPartitions && m_numPartitions <= 0) {
            throw new InvalidSettingsException("Invalid partition count.");
        }
    }

    /**
     * @param settings - Already validated settings to load
     * @throws InvalidSettingsException
     */
    void loadValidatedSettingsFromModel(final NodeSettingsRO settings) throws InvalidSettingsException {
        m_outputPathChooser.loadSettingsFrom(settings);

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
     *
     * @param settings - Settings to load
     */
    protected void loadSettings(final NodeSettingsRO settings) {
        m_uploadDriver = settings.getBoolean(CFG_UPLOAD_DRIVER, DEFAULT_UPLOAD_DRIVER);
        m_overwriteNumPartitions = settings.getBoolean(CFG_OVERWRITE_NUM_PARTITIONS, DEFAULT_OVERWRITE_NUM_PARTITIONS);
        m_numPartitions = settings.getInt(CFG_NUM_PARTITIONS, DEFAULT_NUM_PARTITIONS);
    }

    /**
     * Guess default settings by provided table spec.
     *
     * @param spec - Spark input data table spec.
     */
    public void loadDefault(final DataTableSpec spec) {
        if (m_partitionBy == null) {
            m_partitionBy = new DataColumnSpecFilterConfiguration(CFG_PARTITION_BY);
            m_partitionBy.loadDefaults(spec, false);
        }
    }

    /**
     * Loads the configuration in the dialog (no exception thrown) and maps it to the input spec.
     *
     * @param settings The settings to load from.
     * @param spec The non-null spec.
     */
    public void loadSettingsDialog(final NodeSettingsRO settings, final DataTableSpec spec) {
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
