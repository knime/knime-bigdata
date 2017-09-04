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
package com.knime.bigdata.spark.node.io.genericdatasource.reader;

import org.apache.commons.lang3.StringUtils;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.osgi.framework.Version;

import com.knime.bigdata.spark.core.version.SparkPluginVersion;
import com.knime.bigdata.spark.core.version.SparkVersion;

/**
 * Settings for the generic to spark node.
 *
 * @author Sascha Wolke, KNIME.com
 */
public class GenericDataSource2SparkSettings {

    /** Short or long format name in spark. */
    private final String m_format;

    /** Required spark version. */
    private final SparkVersion m_minSparkVersion;

    /**
     * The version of KNIME Spark Executor that these settings were created with.
     * @since 2.1.0
     */
    private static final String CFG_KNIME_SPARK_EXECUTOR_VERSION = "knimeSparkExecutorVersion";
    private static final String LEGACY_KNIME_SPARK_EXECUTOR_VERSION = SparkPluginVersion.VERSION_2_0_1.toString();
    private Version m_knimeSparkExecutorVersion;

    /** This data source has needs an additional driver jar. */
    private final boolean m_hasDriver;

    /** Required input path. */
    public static final String CFG_INPUT_PATH = "inputPath";
    private static final String DEFAULT_INPUT_PATH = "";
    private String m_path = DEFAULT_INPUT_PATH;

    /** Upload bundled jar. */
    private static final String CFG_UPLOAD_DRIVER = "uploadDriver";
    private static final boolean DEFAULT_UPLOAD_DRIVER = false;


    private boolean m_uploadDriver = DEFAULT_UPLOAD_DRIVER;

    /**
     * Default constructor.
     * Custom constructors should overwrite {@link #newInstance()} too.
     * @param format - Short or long format name in spark.
     * @param minSparkVersion - Minimum spark version.
     * @param hasDriver - True if this data source has a driver jar.
     */
    public GenericDataSource2SparkSettings(final String format, final SparkVersion minSparkVersion, final boolean hasDriver, final Version knimeSparkExecutorVersion) {
        m_format = format;
        m_minSparkVersion = minSparkVersion;
        m_hasDriver = hasDriver;
        m_knimeSparkExecutorVersion = knimeSparkExecutorVersion;
    }

    /** @return New instance of this settings (overwrite this in custom settings) */
    protected GenericDataSource2SparkSettings newInstance() {
        return new GenericDataSource2SparkSettings(m_format, m_minSparkVersion, m_hasDriver, m_knimeSparkExecutorVersion);
    }

    /** @return Spark format name */
    public String getFormat() { return m_format;  }

    /**
     * @param otherVersion - Version to check
     * @return <code>true</code> if version is compatible
     */
    public boolean isCompatibleSparkVersion(final SparkVersion otherVersion) {
        return m_minSparkVersion.compareTo(otherVersion) <= 0;
    }

    /** @return Minimum required spark version */
    public SparkVersion getMinSparkVersion() { return m_minSparkVersion; }

    /**
     * Returns the version of KNIME Spark Executor that this settings object was originally instantiated with.
     *
     * @return the version as an OSGI {@link Version}
     * @since 2.1.0
     */
    public Version getKNIMESparkExecutorVersion() {
        return m_knimeSparkExecutorVersion;
    }

    /** @return True if this data source requires additional jar files */
    public boolean hasDriver() { return m_hasDriver; }

    /** @return Absolute input path */
    public String getInputPath() { return m_path; }
    /** @param path - Absolute input path */
    public void setInputPath(final String path) { m_path = path; }

    /** @return True if bundled jar should be uploaded */
    public boolean uploadDriver() { return m_uploadDriver; }
    /** @param uploadDriver - True if bundled jars should be uploaded */
    public void setUploadDriver(final boolean uploadDriver) { m_uploadDriver = uploadDriver; }

    /** @param settings - Settings to save current settings in */
    public void saveSettingsTo(final NodeSettingsWO settings) {
        settings.addString(CFG_INPUT_PATH, m_path);
        settings.addBoolean(CFG_UPLOAD_DRIVER, m_uploadDriver);
        // added with 2.1.0
        settings.addString(CFG_KNIME_SPARK_EXECUTOR_VERSION, m_knimeSparkExecutorVersion.toString());
    }

    /**
     * @param settings - Settings to validate and not load
     * @throws InvalidSettingsException
     */
    public void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        GenericDataSource2SparkSettings tmp = newInstance();
        tmp.loadSettings(settings);
        tmp.validateSettings();
    }

    /**
     * Validate current settings
     * @throws InvalidSettingsException
     */
    public void validateSettings() throws InvalidSettingsException {
        if (StringUtils.isBlank(m_path)) {
            throw new InvalidSettingsException("Source path required.");
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
        m_path = settings.getString(CFG_INPUT_PATH, DEFAULT_INPUT_PATH);
        m_uploadDriver = settings.getBoolean(CFG_UPLOAD_DRIVER, DEFAULT_UPLOAD_DRIVER);
     // added with 2.1.0
        m_knimeSparkExecutorVersion = Version.valueOf(settings.getString(CFG_KNIME_SPARK_EXECUTOR_VERSION, LEGACY_KNIME_SPARK_EXECUTOR_VERSION));
    }

    /** @param jobInput - Job input to add custom reader options */
    public void addReaderOptions(final GenericDataSource2SparkJobInput jobInput) {
        // Overwrite this in custom settings
    }
}
